// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro.serde;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroKStreamDeserializer;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroKStreamSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serde (Serializer / Deserializer) class for Kafka Streams library compatible with Azure Schema Registry.
 */
public class SpecificAvroKStreamSerde<T extends org.apache.avro.specific.SpecificRecord>
        implements Serde<T> {

    private final Serde<T> inner;

    /**
     * Empty constructor used with Kafka Streams
     */
    public SpecificAvroKStreamSerde() {
        inner = Serdes.serdeFrom(new KafkaAvroKStreamSerializer<T>(), new KafkaAvroKStreamDeserializer<T>());
    }

    @Override
    public Serializer<T> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<T> deserializer() {
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
