// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import com.azure.core.util.ClientOptions;
import com.azure.data.schemaregistry.SchemaRegistryClient;
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder;
import com.azure.data.schemaregistry.models.SchemaRegistrySchema;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

/**
 * Deserializer implementation for Kafka consumer, implementing Kafka Deserializer interface.
 *
 * @see KafkaJsonSerializer See serializer class for upstream serializer implementation
 */
public class KafkaJsonDeserializer<T> implements Deserializer<T> {
    private SchemaRegistryClient client;
    private KafkaJsonDeserializerConfig config;

    /**
     * Empty constructor used by Kafka consumer
     */
    public KafkaJsonDeserializer() {
        super();
    }

    /**
     * Configures deserializer instance.
     *
     * @param props Map of properties used to configure instance
     * @param isKey Indicates if deserializing record key or value.  Required by Kafka deserializer interface,
     *              no specific functionality has been implemented for key use.
     * @see KafkaJsonDeserializerConfig Deserializer will use configs found in here and inherited classes.
     */
    public void configure(Map<String, ?> props, boolean isKey) {
        this.config = new KafkaJsonDeserializerConfig((Map<String, Object>) props);

        this.client = new SchemaRegistryClientBuilder()
                .fullyQualifiedNamespace(this.config.getSchemaRegistryUrl())
                .credential(this.config.getCredential())
                .clientOptions(new ClientOptions().setApplicationId("java-json-kafka-des-1.0"))
                .buildClient();
    }

    /**
     * Deserializes byte array into Java object
     *
     * @param topic topic associated with the record bytes
     * @param data  serialized bytes, may be null
     * @return deserialize object, may be null
     */
    @Override
    public T deserialize(String topic, byte[] data) {
        return deserialize(topic, null, data);
    }

    /**
     * Deserializes byte array into Java object
     *
     * @param topic   topic associated with the record bytes
     * @param headers record headers, may be null
     * @param data    serialized bytes, may be null
     * @return deserialize object, may be null
     * @throws JsonSerializationException Wrapped exception catchable by core Kafka producer code
     */
    @Override
    public T deserialize(String topic, Headers headers, byte[] data) {
        if (data == null) return null;
        T dataObject;
        try {
            byte length = data[0];
            byte[] schemaIdBytes = new byte[length];
            byte[] body = new byte[data.length - 1 - length];
            System.arraycopy(data, 1, schemaIdBytes, 0, schemaIdBytes.length);
            System.arraycopy(data, 1 + length, body, 0, body.length);
            String schemaId = new String(schemaIdBytes);

            ObjectMapper mapper = new ObjectMapper().configure(
                    DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            mapper.setVisibility(mapper.getVisibilityChecker().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
            dataObject = (T) mapper.readValue(body, this.config.getJsonSpecificType());

            SchemaRegistrySchema schema = this.client.getSchema(schemaId);

            JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
            JsonSchema jSchema = factory.getSchema(schema.getDefinition());
            JsonNode node = mapper.readTree(body);

            Set<ValidationMessage> errors = jSchema.validate(node);
            if (errors.size() == 0) {
                return dataObject;
            } else {
                throw new JsonSerializationException(
                        "Failed to validate Json data. Validation errors:\n" + Arrays.toString(errors.toArray()), null);
            }
        } catch (JsonSerializationException e) {
            throw e;
        } catch (Exception e) {
            throw new com.microsoft.azure.schemaregistry.kafka.json.JsonSerializationException("Execption occured during deserialization", e);
        }
    }
}