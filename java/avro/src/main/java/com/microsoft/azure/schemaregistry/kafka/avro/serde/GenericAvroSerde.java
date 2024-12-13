package com.microsoft.azure.schemaregistry.kafka.avro.serde;

import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroDeserializer;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class GenericAvroSerde implements Serde<GenericRecord> {

    private final Serde<GenericRecord> inner;

    public GenericAvroSerde() {
        inner = Serdes.serdeFrom(new KafkaAvroSerializer(), new KafkaAvroDeserializer());
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