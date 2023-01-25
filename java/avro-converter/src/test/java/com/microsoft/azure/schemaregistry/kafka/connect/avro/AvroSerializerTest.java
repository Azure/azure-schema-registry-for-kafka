package com.microsoft.azure.schemaregistry.kafka.connect.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer;

public class AvroSerializerTest {
    @Test
    public void testNullRecordReturnNull() {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        assertEquals(null, serializer.serialize("test-topic", null));
    }
}
