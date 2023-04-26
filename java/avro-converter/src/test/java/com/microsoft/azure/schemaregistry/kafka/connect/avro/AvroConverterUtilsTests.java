package com.microsoft.azure.schemaregistry.kafka.connect.avro;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.TimeZone;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class AvroConverterUtilsTests {
    AvroConverterUtils avroConverterUtils = new AvroConverterUtils();

    // Connect to Avro

    @Test
    public void testFromConnectBoolean() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
        Object converted =
                avroConverterUtils.fromConnectData(Schema.BOOLEAN_SCHEMA, avroSchema, true, false);
        assertEquals(converted, true);
    }

    @Test
    public void testFromConnectByte() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
        avroSchema.addProp("connect.type", "int8");
        Object converted =
                avroConverterUtils.fromConnectData(Schema.INT8_SCHEMA, avroSchema, (byte) 42, false);
        assertEquals(converted, 42);
    }

    @Test
    public void testFromConnectShort() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
        avroSchema.addProp("connect.type", "int16");
        Object converted =
                avroConverterUtils.fromConnectData(Schema.INT16_SCHEMA, avroSchema, (short) 425, false);
        assertEquals(converted, 425);
    }

    @Test
    public void testFromConnectInt() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
        Object converted =
                avroConverterUtils.fromConnectData(Schema.INT32_SCHEMA, avroSchema, 425, false);
        assertEquals(converted, 425);
    }

    @Test
    public void testFromConnectLong() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
        Object converted =
                avroConverterUtils.fromConnectData(Schema.INT64_SCHEMA, avroSchema, 425L, false);
        assertEquals(converted, 425L);
    }

    @Test
    public void testFromConnectFloat() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().floatType();
        Object converted =
                avroConverterUtils.fromConnectData(Schema.FLOAT32_SCHEMA, avroSchema, 4.25f, false);
        assertEquals(converted, 4.25f);
    }

    @Test
    public void testFromConnectDouble() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
        Object converted =
                avroConverterUtils.fromConnectData(Schema.FLOAT64_SCHEMA, avroSchema, 4.25, false);
        assertEquals(converted, 4.25);
    }

    @Test
    public void testFromConnectBytes() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
        Object converted = avroConverterUtils.fromConnectData(Schema.BYTES_SCHEMA, avroSchema,
                ByteBuffer.wrap("foo".getBytes()), false);
        assertEquals(converted, ByteBuffer.wrap("foo".getBytes()));
    }

    @Test
    public void testFromConnectString() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().stringType();
        Object converted = avroConverterUtils.fromConnectData(Schema.STRING_SCHEMA, avroSchema,
                "The quick brown fox jumps over the lazy dog.", false);
        assertEquals(converted, "The quick brown fox jumps over the lazy dog.");
    }

    @Test
    public void testFromConnectMap() {
        Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA);
        org.apache.avro.Schema expected = org.apache.avro.SchemaBuilder.map()
                .values(org.apache.avro.SchemaBuilder.builder().intType());

        assertEquals(avroConverterUtils.fromConnectSchema(schema, false), expected);
    }

    @Test
    public void testFromConnectMapWithOptionalKey() {
        Schema schema = SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.INT32_SCHEMA);
        final org.apache.avro.Schema expected = org.apache.avro.SchemaBuilder.array()
                .items(org.apache.avro.SchemaBuilder.record("com.microsoft.azure.MapEntry").fields()
                        .optionalString("key").requiredInt("value").endRecord());

        assertEquals(expected, avroConverterUtils.fromConnectSchema(schema, false));
    }

    @Test
    public void testFromConnectMapWithNonStringKey() {
        Schema schema = SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.INT32_SCHEMA);
        org.apache.avro.Schema expected = org.apache.avro.SchemaBuilder.array()
                .items(org.apache.avro.SchemaBuilder.record("com.microsoft.azure.MapEntry").fields()
                        .requiredInt("key").requiredInt("value").endRecord());

        assertEquals(expected, avroConverterUtils.fromConnectSchema(schema, false));
    }

    @Test
    public void testFromConnectBytesFixed() {
        org.apache.avro.Schema avroSchema =
                org.apache.avro.SchemaBuilder.builder().fixed("sample").size(4);
        GenericData.Fixed avroObj = new GenericData.Fixed(avroSchema, "foob".getBytes());
        HashMap<String, String> fixedSizedMap = new HashMap<String, String>();
        fixedSizedMap.put("connect.fixed.size", "4");
        avroSchema.addProp("connect.parameters", fixedSizedMap);
        avroSchema.addProp("connect.name", "sample");

        SchemaAndValue schemaAndValue = avroConverterUtils.toConnectData(avroSchema, avroObj);
        assertEquals(ByteBuffer.wrap(avroObj.bytes()), schemaAndValue.value());
    }

    @Test
    public void testFromConnectComplexWithDefaultStructContainingNulls() {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .field("int8", SchemaBuilder.int8().optional().doc("int8 field").build())
                .field("int16", SchemaBuilder.int16().optional().doc("int16 field").build())
                .field("int32", SchemaBuilder.int32().optional().doc("int32 field").build())
                .field("int64", SchemaBuilder.int64().optional().doc("int64 field").build())
                .field("float32", SchemaBuilder.float32().optional().doc("float32 field").build())
                .field("float64", SchemaBuilder.float64().optional().doc("float64 field").build())
                .field("boolean", SchemaBuilder.bool().optional().doc("bool field").build())
                .field("string", SchemaBuilder.string().optional().doc("string field").build())
                .field("bytes", SchemaBuilder.bytes().optional().doc("bytes field").build())
                .field("array", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
                .field("map",
                        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).optional().build())
                .field("date", Date.builder().doc("date field").optional().build())
                .field("time", Time.builder().doc("time field").optional().build())
                .field("ts", Timestamp.builder().doc("ts field").optional().build())
                .field("decimal", Decimal.builder(5).doc("decimal field").optional().build());
        Struct defaultValue = new Struct(schemaBuilder);
        defaultValue.put("int8", (byte) 2);
        Schema schema2 = schemaBuilder.defaultValue(defaultValue).build();

        org.apache.avro.Schema avroSchema = avroConverterUtils.fromConnectSchema(schema2, false);
        assertNotNull(avroSchema);
    }

    // Avro to Connect
    @Test
    public void testToConnectBoolean() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
        SchemaAndValue result = avroConverterUtils.toConnectData(avroSchema, true);
        assertEquals(new SchemaAndValue(Schema.BOOLEAN_SCHEMA, true), result);
    }

    @Test
    public void testToConnectInt64() {
        org.apache.avro.Schema avorSchema = org.apache.avro.SchemaBuilder.builder().longType();
        SchemaAndValue result = avroConverterUtils.toConnectData(avorSchema, 12L);
        assertEquals(new SchemaAndValue(Schema.INT64_SCHEMA, 12L), result);
    }

    @Test
    public void testToConnectFloat32() {
        org.apache.avro.Schema avorSchema = org.apache.avro.SchemaBuilder.builder().floatType();
        SchemaAndValue result = avroConverterUtils.toConnectData(avorSchema, 12.f);
        assertEquals(new SchemaAndValue(Schema.FLOAT32_SCHEMA, 12.f), result);
    }

    @Test
    public void testToConnectFloat64() {
        org.apache.avro.Schema avorSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
        SchemaAndValue result = avroConverterUtils.toConnectData(avorSchema, 12.0);
        assertEquals(new SchemaAndValue(Schema.FLOAT64_SCHEMA, 12.0), result);
    }

    @Test
    public void testToConnectNullableStringNullValue() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.nullable().stringType();
        assertNull(avroConverterUtils.toConnectData(avroSchema, null));
    }

    @Test
    public void testToConnectString() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().stringType();
        SchemaAndValue result = avroConverterUtils.toConnectData(avroSchema, "testString");
        assertEquals(new SchemaAndValue(Schema.STRING_SCHEMA, "testString"), result);
    }

    @Test
    public void toConnectBytes() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
        SchemaAndValue result =
                avroConverterUtils.toConnectData(avroSchema, ByteBuffer.wrap("foo".getBytes()));
        assertEquals(new SchemaAndValue(Schema.BYTES_SCHEMA, ByteBuffer.wrap("foo".getBytes())),
                result);
    }

    @Test
    public void testToConnectArray() {
        org.apache.avro.Schema avroSchema =
                org.apache.avro.SchemaBuilder.builder().array().items().intType();
        avroSchema.getElementType().addProp("connect.type", "int8");

        Schema schema = SchemaBuilder.array(Schema.INT8_SCHEMA).build();
        SchemaAndValue result = avroConverterUtils.toConnectData(avroSchema, Arrays.asList(12, 13));

        assertEquals(new SchemaAndValue(schema, Arrays.asList((byte) 12, (byte) 13)), result);
    }

    @Test
    public void testToConnectMapStringKeys() {
        org.apache.avro.Schema avroSchema =
                org.apache.avro.SchemaBuilder.builder().map().values().intType();
        avroSchema.getValueType().addProp("connect.type", "int8");

        Schema schema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT8_SCHEMA).build();
        SchemaAndValue result =
                avroConverterUtils.toConnectData(avroSchema, Collections.singletonMap("field", 12));

        assertEquals(new SchemaAndValue(schema, Collections.singletonMap("field", (byte) 12)), result);
    }

    @Test
    public void testToConnectRecord() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().record("Record")
                .fields().requiredInt("int8").requiredString("string").endRecord();
        avroSchema.getField("int8").schema().addProp("connect.type", "int8");

        GenericRecord avroRecord =
                new GenericRecordBuilder(avroSchema).set("int8", 12).set("string", "sample string").build();
        Schema schema = SchemaBuilder.struct().name("Record").field("int8", Schema.INT8_SCHEMA)
                .field("string", Schema.STRING_SCHEMA).build();
        Struct struct = new Struct(schema).put("int8", (byte) 12).put("string", "sample string");

        SchemaAndValue result = avroConverterUtils.toConnectData(avroSchema, avroRecord);
        assertEquals(new SchemaAndValue(schema, struct), result);
    }

    @Test
    public void testToConnectUnion() {
        org.apache.avro.Schema avroSchema =
                org.apache.avro.SchemaBuilder.builder().unionOf().intType().and().stringType().endUnion();

        Schema schema = SchemaBuilder.struct().name("com.microsoft.azure.Union")
                .field("int", Schema.INT32_SCHEMA).field("string", Schema.STRING_SCHEMA).build();

        SchemaAndValue result1 = avroConverterUtils.toConnectData(avroSchema, 12);
        SchemaAndValue result2 = avroConverterUtils.toConnectData(avroSchema, "teststring");

        assertEquals(new SchemaAndValue(schema, new Struct(schema).put("int", 12)), result1);
        assertEquals(new SchemaAndValue(schema, new Struct(schema).put("string", "teststring")),
                result2);
    }

    // Connect Logical Types

    @Test
    public void testToConnectDecimal() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
        avroSchema.addProp("logicalType", "decimal");
        avroSchema.addProp("precision", 50);
        avroSchema.addProp("scale", 2);

        final SchemaAndValue expected =
                new SchemaAndValue(Decimal.builder(2).parameter("connect.decimal.precision", "50").build(),
                        new BigDecimal(new BigInteger("156"), 2));

        final SchemaAndValue result =
                avroConverterUtils.toConnectData(avroSchema, new byte[] {0, -100});

        assertEquals(expected.schema().parameters(), result.schema().parameters());
        assertEquals(expected.schema(), result.schema());
        assertEquals(expected.value(), result.value());
    }

    @Test
    public void testToConnectDate() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
        avroSchema.addProp("logicalType", "date");

        SchemaAndValue result = avroConverterUtils.toConnectData(avroSchema, 10000);

        GregorianCalendar cal = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        cal.setTimeZone(TimeZone.getTimeZone("UTC"));
        cal.add(Calendar.DATE, 10000);

        assertEquals(new SchemaAndValue(Date.SCHEMA, cal.getTime()), result);
    }

    @Test
    public void testToConnectTime() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
        avroSchema.addProp("logicalType", "time-millis");

        SchemaAndValue result = avroConverterUtils.toConnectData(avroSchema, 10000);

        GregorianCalendar time = new GregorianCalendar(1970, Calendar.JANUARY, 1, 0, 0, 0);
        time.setTimeZone(TimeZone.getTimeZone("UTC"));
        time.add(Calendar.MILLISECOND, 10000);

        assertEquals(new SchemaAndValue(Time.SCHEMA, time.getTime()), result);
    }

    @Test
    public void testToConnectTimestamp() {
        org.apache.avro.Schema avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
        avroSchema.addProp("logicalType", "timestamp-millis");

        java.util.Date date = new java.util.Date();
        SchemaAndValue result = avroConverterUtils.toConnectData(avroSchema, date.getTime());

        assertEquals(new SchemaAndValue(Timestamp.SCHEMA, date), result);
    }
}
