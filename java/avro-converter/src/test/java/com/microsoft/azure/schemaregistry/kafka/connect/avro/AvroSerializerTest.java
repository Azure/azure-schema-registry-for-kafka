package com.microsoft.azure.schemaregistry.kafka.connect.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.jupiter.api.Test;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.avro.Schema.Parser;
import org.apache.avro.generic.GenericRecord;
import io.confluent.kafka.serializers.NonRecordContainer;

public class AvroSerializerTest {
    @Test
    public void testNullRecordReturnNull() {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        assertEquals(null, serializer.serialize("test-topic", null));
    }

    @Test
    public void connectPrimativeObjectConversionTest() {
        AbstractKafkaSerdeConfig config = new AbstractKafkaSerdeConfig(Collections.emptyMap());
        AvroData avroData = new AvroData(new AvroDataConfig(config.getProps()));
        String testString = "Test String";

        Object avroDataObject = avroData.fromConnectData(Schema.STRING_SCHEMA, testString);
        SchemaAndValue connectObject = avroData.toConnectData(new Parser().parse("{\"type\": \"string\"}"), ((NonRecordContainer) avroDataObject).getValue());
        assertEquals(connectObject.value().toString(), testString);
    }

    @Test
    public void connectRecordObjectConverstionTest() {
        AbstractKafkaSerdeConfig config = new AbstractKafkaSerdeConfig(Collections.emptyMap());
        AvroData avroData = new AvroData(new AvroDataConfig(config.getProps()));
        User user = new User();
        user.setGender("MALE");
        user.setRegionid("206");
        user.setRegistertime(System.currentTimeMillis());
        user.setUserid("98101");

        SchemaAndValue conncetObject = avroData.toConnectData(user.getSchema(), user);
        Object avroDataObject = avroData.fromConnectData(conncetObject.schema(), conncetObject.value());
        assertEquals(((GenericRecord) avroDataObject).toString(), user.toString());
    }
}
