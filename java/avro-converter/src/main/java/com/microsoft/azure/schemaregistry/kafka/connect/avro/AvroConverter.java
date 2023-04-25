// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.connect.avro;

import java.util.Map;

import com.azure.core.util.ClientOptions;
import org.apache.avro.Schema.Parser;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import com.azure.core.credential.TokenCredential;
import com.azure.core.experimental.models.MessageWithMetadata;
import com.azure.core.util.BinaryData;
import com.azure.core.util.serializer.TypeReference;
import com.azure.data.schemaregistry.SchemaRegistryAsyncClient;
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroException;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializer;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializerBuilder;
import com.azure.data.schemaregistry.models.SchemaRegistrySchema;
import com.azure.identity.ClientSecretCredentialBuilder;


/**
 * Implementation of Kafka Connect Converter that uses Azure Schema Registry to save schemas of converted data.
 */
public class AvroConverter implements Converter {
    private SchemaRegistryAsyncClient schemaRegistryClient;
    private SchemaRegistryApacheAvroSerializer serializer;
    private SchemaRegistryApacheAvroSerializer deserializer;
    private AvroConverterConfig avroConverterConfig;

    /**
     * Empty constructor for instantiation by Connect framework.
     */
    public AvroConverter() {
    }

    /**
     * Constructor for use with Kafka Connect framework.
     * @param client Schema Registry client to use for schema registration and retrieval.
     */
    public AvroConverter(SchemaRegistryAsyncClient client) {
        schemaRegistryClient = client;
    }

    /**
     * Configures converter instance
     * @param configs Map of configuration settings.
     * @param isKey Indicates if serializing record key or value
     */
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.avroConverterConfig = new AvroConverterConfig(configs);

        TokenCredential tokenCredential = new ClientSecretCredentialBuilder()
            .tenantId((String) this.avroConverterConfig.getProps().get("tenant.id"))
            .clientId((String) this.avroConverterConfig.getProps().get("client.id"))
            .clientSecret((String) this.avroConverterConfig.getProps().get("client.secret")).build();

        if (schemaRegistryClient == null) {
            schemaRegistryClient = new SchemaRegistryClientBuilder()
                .fullyQualifiedNamespace(this.avroConverterConfig.getSchemaRegistryUrl())
                .credential(tokenCredential)
                .clientOptions(new ClientOptions().setApplicationId("KafkaConnectAvro/1.0"))
                .buildAsyncClient();
        }

        serializer = new SchemaRegistryApacheAvroSerializerBuilder()
            .schemaRegistryAsyncClient(schemaRegistryClient)
            .schemaGroup(this.avroConverterConfig.getSchemaGroup()).autoRegisterSchema(true)
            .buildSerializer();

        deserializer = new SchemaRegistryApacheAvroSerializerBuilder()
            .schemaRegistryAsyncClient(schemaRegistryClient)
            .schemaGroup(this.avroConverterConfig.getSchemaGroup()).buildSerializer();
    }

    /**
     * Convert from Connect data to serialized data.
     * @param topics Connect topic name
     * @param schema Connect schema
     * @param value Connect data
     * @return Serialized data
     */
    public byte[] fromConnectData(String topics, Schema schema, Object value) {
        return fromConnectData(topics, null, schema, value);
    }

    /**
     * Converter implementation.
     * @param topics Connect topic name
     * @param headers Connect headers
     * @param schema Connect schema
     * @param value Connect data
     * @return Serialized data
     */
    public byte[] fromConnectData(String topics, Headers headers, Schema schema, Object value) {
        AvroConverterUtils utils = new AvroConverterUtils();
        Object avroValue =
            utils.fromConnectData(schema, utils.fromConnectSchema(schema, false), value, false);

        // Convert Connect schema and object to normal Avro object

        try {
            MessageWithMetadata message = serializer.serializeMessageData(avroValue,
                TypeReference.createInstance(MessageWithMetadata.class));

            byte[] contentTypeBytes = message.getContentType().getBytes();
            headers.add("content-type", contentTypeBytes);

            return message.getBodyAsBinaryData().toBytes();
        } catch (SchemaRegistryApacheAvroException e) {
            throw new DataException("Failed to serialize Avro data: ", e);
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * Convert from serialized data to Connect data.
     * @param topic Connect topic name
     * @param value Serialized data
     * @return Schema and data class for Connect
     */
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return toConnectData(topic, null, value);
    }

    /**
     * Converter implementation.
     * @param topic Connect topic name
     * @param headers Connect headers
     * @param value Serialized data
     * @return Schema and data class for Connect
     */
    public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
        String contentTypeString = "";
        String schemaId = "";

        try {
            MessageWithMetadata message = new MessageWithMetadata();
            message.setBodyAsBinaryData(BinaryData.fromBytes(value));

            Header contentTypeHeader = headers.lastHeader("content-type");
            if (contentTypeHeader != null) {
                contentTypeString = new String(contentTypeHeader.value());
                message.setContentType(contentTypeString);
            }

            Object deserializedMessage = deserializer.deserializeMessageData(message,
                TypeReference.createInstance(this.avroConverterConfig.getAvroSpecificType()));


            String[] splitSchemaId = contentTypeString.split("\\+");
            if (splitSchemaId.length < 2) {
                throw new DataException("Failed to prase schema id " + splitSchemaId[0]);
            }
            schemaId = splitSchemaId[1];

            SchemaRegistrySchema srSchema = schemaRegistryClient.getSchema(schemaId).block();

            // Convert Avro object to Connect SchemaAndValue

            AvroConverterUtils utils = new AvroConverterUtils();
            return utils.toConnectData(new Parser().parse(srSchema.getDefinition()), deserializedMessage);
        } catch (SchemaRegistryApacheAvroException e) {
            throw new DataException("Failed to deserialize Avro data: ", e);
        } catch (Exception e) {
            throw e;
        }
    }
}
