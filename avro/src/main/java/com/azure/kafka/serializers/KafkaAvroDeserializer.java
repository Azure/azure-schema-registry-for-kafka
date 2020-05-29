// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.kafka.serializers;

import com.azure.core.credential.TokenCredential;
import com.azure.data.schemaregistry.AbstractDataDeserializer;
import com.azure.data.schemaregistry.avro.AvroByteDecoder;
import com.azure.data.schemaregistry.client.CachedSchemaRegistryClientBuilder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * Deserializer implementation for Kafka consumer, implementing Kafka Deserializer interface.
 *
 * Byte arrays are converted into Java objects by using the schema referenced by GUID prefix to deserialize the payload.
 *
 * Receiving Avro GenericRecords and SpecificRecords is supported.  Avro reflection capabilities have been disabled on
 * com.azure.schemaregistry.kafka.KafkaAvroSerializer.
 *
 * @see AbstractDataDeserializer See abstract parent class for implementation details
 * @see KafkaAvroSerializer See serializer class for upstream serializer implementation
 */
public class KafkaAvroDeserializer extends AbstractDataDeserializer
        implements Deserializer<Object> {

    // Constructor used by Kafka consumer.
    public KafkaAvroDeserializer() {
        super();
    }

    /**
     * Configures deserializer instance.
     *
     * @param props Map of properties used to configure instance
     * @param isKey Indicates if deserializing record key or value.  Required by Kafka deserializer interface,
     *              no specific functionality has been implemented for key use.
     *
     * @see KafkaAvroDeserializerConfig Deserializer will use configs found in here and inherited classes.
     */
    public void configure(Map<String, ?> props, boolean isKey) {
        Map<String, Object> kafkaProps = (Map<String, Object>) props;
        String registryUrl = (String) kafkaProps.get(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG);

        Boolean useSpecificAvroReader = (Boolean) kafkaProps.getOrDefault(
                KafkaAvroDeserializerConfig.AVRO_SPECIFIC_READER_CONFIG,
                KafkaAvroDeserializerConfig.AVRO_SPECIFIC_READER_CONFIG_DEFAULT);
        AvroByteDecoder decoder = new AvroByteDecoder(useSpecificAvroReader);

        TokenCredential credential = (TokenCredential) kafkaProps.get(
                KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG);

        Integer maxSchemaMapSize = (Integer) kafkaProps.get(KafkaAvroDeserializerConfig.MAX_SCHEMA_MAP_SIZE_CONFIG);

        this.schemaRegistryClient = new CachedSchemaRegistryClientBuilder()
                .endpoint(registryUrl)
                .credential(credential)
                .maxSchemaMapSize(maxSchemaMapSize)
                .buildClient();

        this.loadByteDecoder(decoder);
    }

    /**
     * Deserializes byte array into Java object
     * @param topic topic associated with the record bytes
     * @param bytes serialized bytes, may be null
     * @return deserialize object, may be null
     * @throws SerializationException catchable by core Kafka fetcher code
     */
    @Override
    public Object deserialize(String topic, byte[] bytes) throws SerializationException {
        try {
            return deserialize(bytes);
        } catch (com.azure.data.schemaregistry.SerializationException e) {
            throw new SerializationException(e.getCause());
        }
    }

    @Override
    public void close() { }
}