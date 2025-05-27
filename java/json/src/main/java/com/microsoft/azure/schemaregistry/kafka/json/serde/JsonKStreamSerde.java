// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json.serde;

import com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonKStreamDeserializer;
import com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonKStreamSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serde (Serializer / Deserializer) class for Kafka Streams library compatible with Azure Schema Registry.
 */
public class JsonKStreamSerde<T> implements Serde<T> {

    private Class<T> specificClass;
    private final Serde<T> inner;

    /**
     * Empty constructor used with Kafka Streams
     */
    public JsonKStreamSerde() {
        inner = Serdes.serdeFrom(new KafkaJsonKStreamSerializer<T>(),
                new KafkaJsonKStreamDeserializer<T>());
    }

    /**
     * Constructor with specific class
     * @param specificClass Class for Specific Record
     */
    public JsonKStreamSerde(Class<T> specificClass) {
        this.specificClass = specificClass;
        inner = Serdes.serdeFrom(new KafkaJsonKStreamSerializer<>(),
                new KafkaJsonKStreamDeserializer<T>());
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
