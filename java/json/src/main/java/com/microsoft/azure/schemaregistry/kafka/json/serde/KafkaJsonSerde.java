// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.
package com.microsoft.azure.schemaregistry.kafka.json.serde;

import com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonDeserializer;
import com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serde (Serializer / Deserializer) class for Kafka Streams library compatible with Azure Schema Registry
 */
public class KafkaJsonSerde<T> implements Serde<T> {

    private final Serde<T> inner;
    /**
     * Empty constructor
     */
    public KafkaJsonSerde() {
        inner = Serdes.serdeFrom(new KafkaJsonSerializer<T>(),
                new KafkaJsonDeserializer<T>());
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
