// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro;

import com.azure.core.util.serializer.TypeReference;
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializer;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializerBuilder;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.util.Map;

/**
 * Deserializer implementation for Kafka consumer, implementing Kafka Deserializer interface.
 *
 * Byte arrays are converted into Java objects by using the schema referenced by GUID prefix to deserialize the payload.
 *
 * Receiving Avro GenericRecords and SpecificRecords is supported.  Avro reflection capabilities have been disabled on
 * com.azure.schemaregistry.kafka.KafkaAvroSerializer.
 *
 * @see KafkaAvroSerializer See serializer class for upstream serializer implementation
 */
public class KafkaAvroDeserializer implements Deserializer<Object> {
    private SchemaRegistryApacheAvroSerializer serializer;
    private KafkaAvroDeserializerConfig config;

    /**
     * Empty constructor used by Kafka consumer
     */
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
        this.config = new KafkaAvroDeserializerConfig((Map<String, Object>) props);  
    }

    /**
     * Deserializes byte array into Java object
     * @param topic topic associated with the record bytes
     * @param bytes serialized bytes, may be null
     * @return deserialize object, may be null
     */
    @Override
    public Object deserialize(String topic, byte[] bytes) {
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);

        this.serializer = new SchemaRegistryApacheAvroSerializerBuilder()
                .schemaRegistryAsyncClient(new SchemaRegistryClientBuilder()
                        .fullyQualifiedNamespace(this.config.getSchemaRegistryUrl())
                        .credential(this.config.getCredential())
                        .buildAsyncClient())
                .avroSpecificReader(this.config.getAvroSpecificReader())
                .buildSerializer();

        return serializer.deserialize(in, TypeReference.createInstance(Object.class));
    }

    @Override
    public void close() { }
}
