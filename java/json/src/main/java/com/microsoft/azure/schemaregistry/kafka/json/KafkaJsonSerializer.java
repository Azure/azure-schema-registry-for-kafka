// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import com.azure.core.util.ClientOptions;
import com.azure.data.schemaregistry.SchemaRegistryClient;
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder;
import com.azure.data.schemaregistry.models.SchemaFormat;
import com.azure.data.schemaregistry.models.SchemaProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;
import java.util.Map;

/**
 * Serializer implementation for Kafka producer, implementing the Kafka Serializer interface.
 *
 * @see KafkaJsonDeserializer See deserializer class for downstream deserializer implementation
 */
public class KafkaJsonSerializer<T> implements Serializer<T> {
    private SchemaRegistryClient client;
    private String schemaGroup;
    private Boolean autoRegisterSchemas;

  /**
   * Empty constructor for Kafka producer
    */
    public KafkaJsonSerializer() {
        super();
    }

    /**
     * Configures serializer instance.
     *
     * @param props Map of properties used to configure instance.
     * @param isKey Indicates if serializing record key or value.  Required by Kafka serializer interface,
     *              no specific functionality implemented for key use.
     *
     * @see KafkaJsonSerializerConfig Serializer will use configs found in KafkaJsonSerializerConfig.
     */
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        KafkaJsonSerializerConfig config = new KafkaJsonSerializerConfig((Map<String, Object>) props);

        this.autoRegisterSchemas = config.getAutoRegisterSchemas();
        this.schemaGroup = config.getSchemaGroup();

        TokenCredential tokenCredential;
        tokenCredential = config.getCredential();
        if (tokenCredential == null) {
            if (config.createDefaultAzureCredential()) {
                tokenCredential = new DefaultAzureCredentialBuilder().build();
            } else {
                throw new RuntimeException(
                        "TokenCredential not created for serializer. "
                                + "Please provide a TokenCredential in config or set "
                                + "\"use.azure.credential\" to true."
                );
            }
        }

        this.client = new SchemaRegistryClientBuilder()
        .fullyQualifiedNamespace(config.getSchemaRegistryUrl())
        .credential(tokenCredential)
        .clientOptions(new ClientOptions().setApplicationId("java-json-kafka-ser-1.0"))
        .buildClient();
    }

    /**
     * Serializes into a byte array, containing a GUID reference to schema
     * and the encoded payload.
     *
     * Null behavior matches Kafka treatment of null values.
     *
     * @param topic Topic destination for record. Required by Kafka serializer interface, currently not used.
     * @param record Object to be serialized, may be null
     * @return byte[] payload for sending to EH Kafka service, may be null
     * @throws JsonSerializationException Wrapped exception catchable by core Kafka producer code
     */
    @Override
    public byte[] serialize(String topic, T record) {
        return null;
    }

    /**
     * Serializes into a byte array, containing a GUID reference to schema
     * and the encoded payload.
     *
     * Null behavior matches Kafka treatment of null values.
     *
     * @param topic Topic destination for record. Required by Kafka serializer interface, currently not used.
     * @param record Object to be serialized, may be null
     * @param headers Record headers, may be null
     * @return byte[] payload for sending to EH Kafka service, may be null
     * @throws JsonSerializationException Wrapped exception catchable by core Kafka producer code
     */
    @Override
    public byte[] serialize(String topic, Headers headers, T record) {
        if (record == null) {
            return null;
        }

        byte[] recordBytes;
        SchemaProperties schemaProps;
        try {
            ObjectMapper mapper = new ObjectMapper();
            recordBytes = mapper.writeValueAsBytes(record);

            SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON);
            SchemaGeneratorConfig config = configBuilder.build();
            SchemaGenerator generator = new SchemaGenerator(config);
            JsonNode jsonSchema = generator.generateSchema(record.getClass());
            String jsonSchemaString = jsonSchema.toString();

            if (this.autoRegisterSchemas) {
                schemaProps = this.client.registerSchema(
                    this.schemaGroup,
                    record.getClass().getName(),
                    jsonSchemaString,
                    SchemaFormat.JSON);
            } else {
                schemaProps = this.client.getSchemaProperties(
                    this.schemaGroup,
                    record.getClass().getName(),
                    jsonSchemaString,
                    SchemaFormat.JSON);
            }
                        
            headers.add("schemaId", schemaProps.getId().getBytes());
            return recordBytes;
        } catch (IllegalStateException e) {
            throw new JsonSerializationException("Error occured while generating schema", e);
        } catch (JsonProcessingException e) {
            throw new JsonSerializationException("Error occured while serializing record into bytes", e);
        } catch (Exception e) {
            throw new JsonSerializationException("Execption occured during serialization", e);
        }
    }

    @Override
    public void close() { }
}
