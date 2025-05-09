// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro.serde;

import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroKStreamDeserializer;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroKStreamSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serde (Serializer / Deserializer) class for Kafka Streams library compatible with Azure Schema Registry.
 */
public class GenericAvroKStreamSerde implements Serde<GenericRecord> {

    private final Serde<GenericRecord> inner;

    /**
     * Empty constructor used with Kafka Streams
     */
    public GenericAvroKStreamSerde() {
        inner = Serdes.serdeFrom(new KafkaAvroKStreamSerializer(), new KafkaAvroKStreamDeserializer());
    }

    @Override
    public Serializer<GenericRecord> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<GenericRecord> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(final Map<String, ?> serdeConfig, final boolean isSerdeForRecordKeys) {
        inner.serializer().configure(serdeConfig, isSerdeForRecordKeys);
        inner.deserializer().configure(serdeConfig, isSerdeForRecordKeys);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }
}
