// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro;

import com.azure.core.models.MessageContent;
import com.azure.core.util.serializer.TypeReference;
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializer;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializerBuilder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serializer implementation for Kafka producer, implementing the Kafka Serializer interface.
 *
 * Objects are converted to byte arrays containing an Avro-encoded payload and is prefixed with a GUID pointing
 * to the matching Avro schema in Azure Schema Registry.
 *
 * Currently, sending Avro GenericRecords and SpecificRecords is supported.  Avro reflection has been disabled.
 *
 * @see KafkaAvroDeserializer See deserializer class for downstream deserializer implementation
 */
public class KafkaAvroSerializer<T> implements Serializer<T> {
    private SchemaRegistryApacheAvroSerializer serializer;

    /**
     * Empty constructor for Kafka producer
     */
    public KafkaAvroSerializer() {
        super();
    }

    /**
     * Configures serializer instance.
     *
     * @param props Map of properties used to configure instance.
     * @param isKey Indicates if serializing record key or value.  Required by Kafka serializer interface,
     *              no specific functionality implemented for key use.
     *
     * @see KafkaAvroSerializerConfig Serializer will use configs found in KafkaAvroSerializerConfig.
     */
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        KafkaAvroSerializerConfig config = new KafkaAvroSerializerConfig((Map<String, Object>) props);

        this.serializer = new SchemaRegistryApacheAvroSerializerBuilder()
                .schemaRegistryClient(new SchemaRegistryClientBuilder()
                        .fullyQualifiedNamespace(config.getSchemaRegistryUrl())
                        .credential(config.getCredential())
                        .buildAsyncClient())
                .schemaGroup(config.getSchemaGroup())
                .autoRegisterSchemas(config.getAutoRegisterSchemas())
                .buildSerializer();
    }


    /**
     * Serializes GenericRecord or SpecificRecord into a byte array, containing a GUID reference to schema
     * and the encoded payload.
     *
     * Null behavior matches Kafka treatment of null values.
     *
     * @param topic Topic destination for record. Required by Kafka serializer interface, currently not used.
     * @param record Object to be serialized, may be null
     * @return byte[] payload for sending to EH Kafka service, may be null
     * @throws SerializationException Exception catchable by core Kafka producer code
     */
    @Override
    public byte[] serialize(String topic, T record) {
        return null;
    }

    /**
     * Serializes GenericRecord or SpecificRecord into a byte array, containing a GUID reference to schema
     * and the encoded payload.
     *
     * Null behavior matches Kafka treatment of null values.
     *
     * @param topic Topic destination for record. Required by Kafka serializer interface, currently not used.
     * @param record Object to be serialized, may be null
     * @param headers Record headers, may be null
     * @return byte[] payload for sending to EH Kafka service, may be null
     * @throws SerializationException Exception catchable by core Kafka producer code
     */
    @Override
    public byte[] serialize(String topic, Headers headers, T record) {
        // null needs to treated specially since the client most likely just wants to send
        // an individual null value instead of making the subject a null type. Also, null in
        // Kafka has a special meaning for deletion in a topic with the compact retention policy.
        // Therefore, we will bypass schema registration and return a null value in Kafka, instead
        // of an Avro encoded null.
        if (record == null) {
            return null;
        }

        MessageContent message = this.serializer.serialize(record, TypeReference.createInstance(MessageContent.class));
        byte[] contentTypeHeaderBytes = message.getContentType().getBytes();
        headers.add("content-type", contentTypeHeaderBytes);
        return message.getBodyAsBinaryData().toBytes();
    }

    @Override
    public void close() { }
}
