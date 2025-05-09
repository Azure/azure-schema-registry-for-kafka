// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro.serde;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroDeserializer;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Serde (Serializer / Deserializer) class
 */
public class SpecificAvroSerde<T extends org.apache.avro.specific.SpecificRecord>
        implements Serde<T> {

    private final Serde<T> inner;

    /**
     * Empty constructor
     */
    public SpecificAvroSerde() {
        inner = Serdes.serdeFrom(new KafkaAvroSerializer<T>(), new KafkaAvroDeserializer<T>());
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
