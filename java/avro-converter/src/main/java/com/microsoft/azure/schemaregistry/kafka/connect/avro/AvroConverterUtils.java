// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.connect.avro;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.AvroTypeException;
import org.apache.avro.JsonProperties;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.util.internal.JacksonUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Utilities for converting between Kafka Connect data types and Avro data types.
 */
public class AvroConverterUtils {
    public static final String NAMESPACE = "com.microsoft.azure";
    public static final String DEFAULT_SCHEMA_NAME = "ConnectSchema";
    public static final String DEFAULT_SCHEMA_FULL_NAME = NAMESPACE + "." + DEFAULT_SCHEMA_NAME;
    public static final String MAP_ENTRY_TYPE_NAME = "MapEntry";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";

    public static final String CONNECT_TYPE_PROP = "connect.type";
    public static final String CONNECT_TYPE_INT8 = "int8";
    public static final String CONNECT_TYPE_INT16 = "int16";

    public static final String AVRO_TYPE_UNION = NAMESPACE + ".Union";
    public static final String AVRO_TYPE_ANYTHING = NAMESPACE + ".Anything";
    public static final String AVRO_RECORD_DOC_PROP = NAMESPACE + ".record.doc";
    public static final String AVRO_FIELD_DOC_PREFIX_PROP = NAMESPACE + ".field.doc.";

    private static final Map<String, Schema.Type> NON_AVRO_TYPES_BY_TYPE_CODE = new HashMap<>();

    static {
        NON_AVRO_TYPES_BY_TYPE_CODE.put(CONNECT_TYPE_INT8, Schema.Type.INT8);
        NON_AVRO_TYPES_BY_TYPE_CODE.put(CONNECT_TYPE_INT16, Schema.Type.INT16);
    }

    private static final Map<Schema.Type, List<Class>> SIMPLE_AVRO_SCHEMA_TYPES = new HashMap<>();

    static {
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.INT32, Arrays.asList((Class) Integer.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.INT64, Arrays.asList((Class) Long.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.FLOAT32, Arrays.asList((Class) Float.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.FLOAT64, Arrays.asList((Class) Double.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.BOOLEAN, Arrays.asList((Class) Boolean.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.STRING, Arrays.asList((Class) CharSequence.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.BYTES,
            Arrays.asList((Class) ByteBuffer.class, (Class) byte[].class, (Class) GenericFixed.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.ARRAY, Arrays.asList((Class) Collection.class));
        SIMPLE_AVRO_SCHEMA_TYPES.put(Schema.Type.MAP, Arrays.asList((Class) Map.class));
    }

    private static final Map<Schema.Type, org.apache.avro.Schema.Type> CONNECT_TYPES_TO_AVRO_TYPES =
        new HashMap<>();

    static {
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.INT32, org.apache.avro.Schema.Type.INT);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.INT64, org.apache.avro.Schema.Type.LONG);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.FLOAT32, org.apache.avro.Schema.Type.FLOAT);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.FLOAT64, org.apache.avro.Schema.Type.DOUBLE);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.BOOLEAN, org.apache.avro.Schema.Type.BOOLEAN);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.STRING, org.apache.avro.Schema.Type.STRING);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.BYTES, org.apache.avro.Schema.Type.BYTES);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.ARRAY, org.apache.avro.Schema.Type.ARRAY);
        CONNECT_TYPES_TO_AVRO_TYPES.put(Schema.Type.MAP, org.apache.avro.Schema.Type.MAP);
    }

    private static final String ANYTHING_SCHEMA_BOOLEAN_FIELD = "boolean";
    private static final String ANYTHING_SCHEMA_BYTES_FIELD = "bytes";
    private static final String ANYTHING_SCHEMA_DOUBLE_FIELD = "double";
    private static final String ANYTHING_SCHEMA_FLOAT_FIELD = "float";
    private static final String ANYTHING_SCHEMA_INT_FIELD = "int";
    private static final String ANYTHING_SCHEMA_LONG_FIELD = "long";
    private static final String ANYTHING_SCHEMA_STRING_FIELD = "string";
    private static final String ANYTHING_SCHEMA_ARRAY_FIELD = "array";
    private static final String ANYTHING_SCHEMA_MAP_FIELD = "map";

    static final String AVRO_PROP = "avro";
    static final String AVRO_LOGICAL_TYPE_PROP = "logicalType";
    static final String AVRO_LOGICAL_TIMESTAMP_MILLIS = "timestamp-millis";
    static final String AVRO_LOGICAL_TIME_MILLIS = "time-millis";
    static final String AVRO_LOGICAL_DATE = "date";
    static final String AVRO_LOGICAL_DECIMAL = "decimal";
    static final String AVRO_LOGICAL_DECIMAL_SCALE_PROP = "scale";
    static final String AVRO_LOGICAL_DECIMAL_PRECISION_PROP = "precision";
    public static final String AVRO_TYPE_ENUM = NAMESPACE + ".Enum";
    public static final String AVRO_ENUM_DOC_PREFIX_PROP = NAMESPACE + ".enum.doc.";

    static final String CONNECT_AVRO_FIXED_SIZE_PROP = "connect.fixed.size";
    static final String CONNECT_AVRO_DECIMAL_PRECISION_PROP = "connect.decimal.precision";
    static final Integer CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT = 64;
    public static final String CONNECT_INTERNAL_TYPE_NAME = "connect.internal.type";
    public static final String CONNECT_NAME_PROP = "connect.name";
    public static final String CONNECT_DOC_PROP = "connect.doc";
    public static final String CONNECT_RECORD_DOC_PROP = "connect.record.doc";
    public static final String CONNECT_ENUM_DOC_PROP = "connect.enum.doc";
    public static final String CONNECT_VERSION_PROP = "connect.version";
    public static final String CONNECT_DEFAULT_VALUE_PROP = "connect.default";
    public static final String CONNECT_PARAMETERS_PROP = "connect.parameters";

    public static final org.apache.avro.Schema ANYTHING_SCHEMA_MAP_ELEMENT;
    public static final org.apache.avro.Schema ANYTHING_SCHEMA;
    private static final org.apache.avro.Schema NULL_AVRO_SCHEMA =
        org.apache.avro.Schema.create(org.apache.avro.Schema.Type.NULL);

    static {
        ANYTHING_SCHEMA = org.apache.avro.SchemaBuilder.record(AVRO_TYPE_ANYTHING).namespace(NAMESPACE)
            .fields().optionalBoolean(ANYTHING_SCHEMA_BOOLEAN_FIELD)
            .optionalBytes(ANYTHING_SCHEMA_BYTES_FIELD).optionalDouble(ANYTHING_SCHEMA_DOUBLE_FIELD)
            .optionalFloat(ANYTHING_SCHEMA_FLOAT_FIELD).optionalInt(ANYTHING_SCHEMA_INT_FIELD)
            .optionalLong(ANYTHING_SCHEMA_LONG_FIELD).optionalString(ANYTHING_SCHEMA_STRING_FIELD)
            .name(ANYTHING_SCHEMA_ARRAY_FIELD).type().optional().array().items()
            .type(AVRO_TYPE_ANYTHING).name(ANYTHING_SCHEMA_MAP_FIELD).type().optional().array().items()
            .record(MAP_ENTRY_TYPE_NAME).namespace(NAMESPACE).fields().name(KEY_FIELD)
            .type(AVRO_TYPE_ANYTHING).noDefault().name(VALUE_FIELD).type(AVRO_TYPE_ANYTHING).noDefault()
            .endRecord().endRecord();

        ANYTHING_SCHEMA_MAP_ELEMENT =
            ANYTHING_SCHEMA.getField("map").schema().getTypes().get(1).getElementType();
    }

    /**
     * Convert from Avro data to Schema and Value data
     * @param avroSchema The Avro schema
     * @param value The value to convert
     * @return The converted schema and value
     */
    public SchemaAndValue toConnectData(org.apache.avro.Schema avroSchema, Object value) {
        if (value == null) {
            return null;
        }

        Schema schema = (avroSchema.equals(ANYTHING_SCHEMA)) ? null : toConnectSchema(avroSchema);

        return new SchemaAndValue(schema, toConnectValue(schema, value));
    }

    private Object toConnectValue(Schema schema, Object value) {
        if (value == null || value == JsonProperties.NULL_VALUE) {
            return null;
        }

        try {
            if (schema == null) {
                if (!(value instanceof IndexedRecord)) {
                    throw new DataException("Invalid Avro data for schemaless Connect data");
                }
                IndexedRecord recordValue = (IndexedRecord) value;

                Object boolVal =
                    recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_BOOLEAN_FIELD).pos());
                if (boolVal != null) {
                    return toConnectValue(Schema.BOOLEAN_SCHEMA, boolVal);
                }

                Object bytesVal =
                    recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_BYTES_FIELD).pos());
                if (bytesVal != null) {
                    return toConnectValue(Schema.BYTES_SCHEMA, bytesVal);
                }

                Object doubleVal =
                    recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_DOUBLE_FIELD).pos());
                if (doubleVal != null) {
                    return toConnectValue(Schema.FLOAT64_SCHEMA, doubleVal);
                }

                Object floatVal =
                    recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_FLOAT_FIELD).pos());
                if (floatVal != null) {
                    return toConnectValue(Schema.FLOAT32_SCHEMA, floatVal);
                }

                Object intVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_INT_FIELD).pos());
                if (intVal != null) {
                    return toConnectValue(Schema.INT32_SCHEMA, intVal);
                }

                Object longVal =
                    recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_LONG_FIELD).pos());
                if (longVal != null) {
                    return toConnectValue(Schema.INT64_SCHEMA, longVal);
                }

                Object stringVal =
                    recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_STRING_FIELD).pos());
                if (stringVal != null) {
                    return toConnectValue(Schema.STRING_SCHEMA, stringVal);
                }

                Object arrayVal =
                    recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_ARRAY_FIELD).pos());
                if (arrayVal != null) {
                    if (!(arrayVal instanceof Collection)) {
                        throw new DataException("Expected Collection for schemaless array field but found "
                            + arrayVal.getClass().getName());
                    }
                    Collection<Object> arrayValCollection = (Collection<Object>) arrayVal;
                    List<Object> result = new ArrayList<>(arrayValCollection.size());
                    for (Object arrayValue : arrayValCollection) {
                        result.add(toConnectValue((Schema) null, arrayValue));
                    }

                    return result;
                }

                Object mapVal = recordValue.get(ANYTHING_SCHEMA.getField(ANYTHING_SCHEMA_MAP_FIELD).pos());
                if (mapVal != null) {
                    if (!(mapVal instanceof Collection)) {
                        throw new DataException(
                            "Expected List for schemaless map field but found " + mapVal.getClass().getName());
                    }
                    Collection<IndexedRecord> mapValueCollection = (Collection<IndexedRecord>) mapVal;
                    Map<Object, Object> result = new HashMap<>(mapValueCollection.size());
                    for (IndexedRecord mapValue : mapValueCollection) {
                        int avroKeyFieldIndex = mapValue.getSchema().getField(KEY_FIELD).pos();
                        int avroValueFieldIndex = mapValue.getSchema().getField(VALUE_FIELD).pos();
                        Object convertedKey = toConnectValue((Schema) null, mapValue.get(avroKeyFieldIndex));
                        Object convertedValue =
                            toConnectValue((Schema) null, mapValue.get(avroValueFieldIndex));
                        result.put(convertedKey, convertedValue);
                    }

                    return result;
                }

                return null;
            }

            Object converted = null;
            switch (schema.type()) {
                case INT32: {
                    Integer intValue = (Integer) value;
                    converted = intValue;
                    break;
                }
                case INT64: {
                    Long longValue = (Long) value;
                    converted = longValue;
                    break;
                }
                case FLOAT32: {
                    Float floatValue = (Float) value;
                    converted = floatValue;
                    break;
                }
                case FLOAT64: {
                    Double doubleValue = (Double) value;
                    converted = doubleValue;
                    break;
                }
                case BOOLEAN: {
                    Boolean boolValue = (Boolean) value;
                    converted = boolValue;
                    break;
                }
                case INT8:
                    converted = ((Integer) value).byteValue();
                    break;
                case INT16:
                    converted = ((Integer) value).shortValue();
                    break;
                case STRING:
                    if (value instanceof String) {
                        converted = value;
                    } else if (value instanceof CharSequence || value instanceof Enum) {
                        converted = value.toString();
                    } else {
                        throw new DataException(
                            "Invalid class for string type, expecting String or CharSequence, got "
                                + value.getClass());
                    }
                    break;
                case BYTES:
                    if (value instanceof byte[]) {
                        converted = ByteBuffer.wrap((byte[]) value);
                    } else if (value instanceof ByteBuffer) {
                        converted = value;
                    } else if (value instanceof GenericFixed) {
                        converted = ByteBuffer.wrap(((GenericFixed) value).bytes());
                    } else {
                        throw new DataException(
                            "Invalid class for bytes type, expecting byte[] or ByteBuffer, got "
                                + value.getClass());
                    }
                    break;
                case ARRAY: {
                    Schema valueSchema = schema.valueSchema();
                    Collection<Object> valueSchemaCollection = (Collection<Object>) value;
                    List<Object> arrayValue = new ArrayList<>(valueSchemaCollection.size());
                    for (Object elem : valueSchemaCollection) {
                        arrayValue.add(toConnectValue(valueSchema, elem));
                    }
                    converted = arrayValue;
                    break;
                }
                case MAP: {
                    Schema keySchema = schema.keySchema();
                    Schema valueSchema = schema.valueSchema();
                    if (keySchema != null && keySchema.type() == Schema.Type.STRING
                        && !keySchema.isOptional()) {
                        Map<CharSequence, Object> valueSchemaMap = (Map<CharSequence, Object>) value;
                        Map<CharSequence, Object> mapValue = new HashMap<>(valueSchemaMap.size());
                        for (Map.Entry<CharSequence, Object> entry : valueSchemaMap.entrySet()) {
                            mapValue.put(entry.getKey().toString(),
                                toConnectValue(valueSchema, entry.getValue()));
                        }
                        converted = mapValue;
                    } else {
                        Collection<IndexedRecord> original = (Collection<IndexedRecord>) value;
                        Map<Object, Object> mapValue = new HashMap<>(original.size());
                        for (IndexedRecord entry : original) {
                            int avroKeyFieldIndex = entry.getSchema().getField(KEY_FIELD).pos();
                            int avroValueFieldIndex = entry.getSchema().getField(VALUE_FIELD).pos();
                            Object convertedKey = toConnectValue(keySchema, entry.get(avroKeyFieldIndex));
                            Object convertedValue = toConnectValue(valueSchema, entry.get(avroValueFieldIndex));
                            mapValue.put(convertedKey, convertedValue);
                        }
                        converted = mapValue;
                    }
                    break;
                }
                case STRUCT: {
                    if (AVRO_TYPE_UNION.equals(schema.name())) {
                        Schema valueRecordSchema = null;
                        if (value instanceof IndexedRecord) {
                            IndexedRecord valueRecord = ((IndexedRecord) value);
                            valueRecordSchema = toConnectSchema(valueRecord.getSchema());
                        }
                        int index = 0;
                        for (Field field : schema.fields()) {
                            Schema fieldSchema = field.schema();
                            if (isInstanceOfAvroSchemaTypeForSimpleSchema(fieldSchema, value, index)
                                || (valueRecordSchema != null && schemaEquals(valueRecordSchema, fieldSchema))) {
                                converted = new Struct(schema).put(unionMemberFieldName(fieldSchema, index),
                                    toConnectValue(fieldSchema, value));
                                break;
                            }
                            index++;
                        }
                        if (converted == null) {
                            throw new DataException("Did not find matching union field for data");
                        }
                    } else if (value instanceof Map) {
                        Map<CharSequence, Object> original = (Map<CharSequence, Object>) value;
                        Struct result = new Struct(schema);
                        for (Field field : schema.fields()) {
                            String fieldName = field.name();
                            Object convertedFieldValue = toConnectValue(field.schema(),
                                original.getOrDefault(fieldName, field.schema().defaultValue()));
                            result.put(field, convertedFieldValue);
                        }
                        return result;
                    } else {
                        IndexedRecord valueRecord = (IndexedRecord) value;
                        Struct structValue = new Struct(schema);
                        for (Field field : schema.fields()) {
                            String fieldName = field.name();
                            int avroFieldIndex = valueRecord.getSchema().getField(fieldName).pos();
                            Object convertedFieldValue =
                                toConnectValue(field.schema(), valueRecord.get(avroFieldIndex));
                            structValue.put(field, convertedFieldValue);
                        }
                        converted = structValue;
                    }
                    break;
                }
                default:
                    throw new DataException("Unknown Connect schema type: " + schema.type());
            }

            if (schema.name() != null) {
                String schemaNameLower = schema.name().toLowerCase();

                switch (schemaNameLower) {
                    case "org.apache.kafka.connect.data.date":
                        if (!(value instanceof Integer)) {
                            throw new DataException(
                                "Invalid type for Time, underlying representation should be int32 but was "
                                    + value.getClass());
                        }
                        converted = Date.toLogical(schema, (int) value);
                        break;
                    case "org.apache.kafka.connect.data.time":
                        if (!(value instanceof Integer)) {
                            throw new DataException(
                                "Invalid type for Time, underlying representation should be int32 but was "
                                    + value.getClass());
                        }
                        converted = Time.toLogical(schema, (int) value);
                        break;
                    case "org.apache.kafka.connect.data.timestamp":
                        if (!(value instanceof Long)) {
                            throw new DataException(
                                "Invalid type for Timestamp, underlying representation should be int64 but was "
                                    + value.getClass());
                        }
                        converted = Timestamp.toLogical(schema, (long) value);
                        break;
                    case "org.apache.kafka.connect.data.decimal":
                        if (value instanceof byte[]) {
                            converted = Decimal.toLogical(schema, (byte[]) value);
                        } else if (value instanceof ByteBuffer) {
                            converted = Decimal.toLogical(schema, ((ByteBuffer) value).array());
                        } else {
                            throw new DataException(
                                "Invalid type for Decimal, underlying representation should be bytes but was "
                                    + value.getClass());
                        }
                        break;
                    default:
                        break;
                }
            }

            return converted;
        } catch (ClassCastException e) {
            String schemaType = schema != null ? schema.type().toString() : "null";
            throw new DataException("Invalid type for " + schemaType + ": " + value.getClass());
        }
    }

    /**
     * Convert the supplied Avro schema to a Kafka Connect schema.
     * @param schema The Avro schema to convert
     * @return The converted Kafka Connect schema
     */
    public Schema toConnectSchema(org.apache.avro.Schema schema) {
        String type = schema.getProp(CONNECT_TYPE_PROP);
        String logicalType = schema.getProp(AVRO_LOGICAL_TYPE_PROP);

        final SchemaBuilder builder;

        switch (schema.getType()) {
            case BOOLEAN:
                builder = SchemaBuilder.bool();
                break;
            case BYTES:
            case FIXED:
                if (AVRO_LOGICAL_DECIMAL.equalsIgnoreCase(logicalType)) {
                    Object scaleNode = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP);
                    int scale = scaleNode instanceof Number ? ((Number) scaleNode).intValue() : 0;
                    builder = Decimal.builder(scale);

                    Object precisionNode = schema.getObjectProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP);
                    if (null != precisionNode) {
                        if (!(precisionNode instanceof Number)) {
                            throw new DataException(AVRO_LOGICAL_DECIMAL_PRECISION_PROP + " is not an integer.");
                        }
                        int percision = ((Number) precisionNode).intValue();
                        if (percision != CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT) {
                            builder.parameter(CONNECT_AVRO_DECIMAL_PRECISION_PROP, String.valueOf(percision));
                        }
                    }
                } else {
                    builder = SchemaBuilder.bytes();
                }
                if (schema.getType() == org.apache.avro.Schema.Type.FIXED) {
                    builder.parameter(CONNECT_AVRO_FIXED_SIZE_PROP, String.valueOf(schema.getFixedSize()));
                }
                break;
            case DOUBLE:
                builder = SchemaBuilder.float64();
                break;
            case FLOAT:
                builder = SchemaBuilder.float32();
                break;
            case INT:
                if (type == null && logicalType == null) {
                    builder = SchemaBuilder.int32();
                } else if (logicalType != null) {
                    if (AVRO_LOGICAL_DATE.equalsIgnoreCase(logicalType)) {
                        builder = Date.builder();
                    } else if (AVRO_LOGICAL_TIME_MILLIS.equalsIgnoreCase(logicalType)) {
                        builder = Time.builder();
                    } else {
                        builder = SchemaBuilder.int32();
                    }
                } else {
                    Schema.Type connectType = NON_AVRO_TYPES_BY_TYPE_CODE.get(type);
                    if (connectType == null) {
                        throw new DataException("Connect type for Avro int field is not found");
                    }
                    builder = SchemaBuilder.type(connectType);
                }
                break;
            case LONG:
                if (AVRO_LOGICAL_TIMESTAMP_MILLIS.equalsIgnoreCase(logicalType)) {
                    builder = Timestamp.builder();
                } else {
                    builder = SchemaBuilder.int64();
                }
                break;
            case STRING:
                builder = SchemaBuilder.string();
                break;
            case ARRAY:
                org.apache.avro.Schema arrayElementSchema = schema.getElementType();
                if (isMapEntry(arrayElementSchema)) {
                    if (arrayElementSchema.getFields().size() != 2
                        || arrayElementSchema.getField(KEY_FIELD) == null
                        || arrayElementSchema.getField(VALUE_FIELD) == null) {
                        throw new DataException("Expected array of key-value pairs");
                    }
                    builder =
                        SchemaBuilder.map(toConnectSchema(arrayElementSchema.getField(KEY_FIELD).schema()),
                            toConnectSchema(arrayElementSchema.getField(VALUE_FIELD).schema()));
                } else {
                    Schema arraySchema = toConnectSchema(schema.getElementType());
                    builder = SchemaBuilder.array(arraySchema);
                }
                break;
            case MAP:
                builder = SchemaBuilder.map(Schema.STRING_SCHEMA, toConnectSchema(schema.getValueType()));
                break;
            case RECORD: {
                builder = SchemaBuilder.struct();
                for (org.apache.avro.Schema.Field field : schema.getFields()) {
                    Schema fieldSchema = toConnectSchema(field.schema());
                    builder.field(field.name(), fieldSchema);
                }
                break;
            }
            case ENUM:
                builder = SchemaBuilder.string();
                String paramName = AVRO_TYPE_ENUM;
                builder.parameter(paramName, schema.getFullName());
                for (String enumSymbol : schema.getEnumSymbols()) {
                    builder.parameter(paramName + "." + enumSymbol, enumSymbol);
                }
                break;
            case UNION: {
                if (schema.getTypes().size() == 2) {
                    if (schema.getTypes().contains(NULL_AVRO_SCHEMA)) {
                        for (org.apache.avro.Schema memberSchema : schema.getTypes()) {
                            if (!memberSchema.equals(NULL_AVRO_SCHEMA)) {
                                return toConnectSchema(memberSchema);
                            }
                        }
                    }
                }
                String unionName = AVRO_TYPE_UNION;
                builder = SchemaBuilder.struct().name(unionName);
                Set<String> fieldNames = new HashSet<>();
                int fieldIndex = 0;
                for (org.apache.avro.Schema memberSchema : schema.getTypes()) {
                    if (memberSchema.getType() == org.apache.avro.Schema.Type.NULL) {
                        builder.optional();
                    } else {
                        String fieldName = unionMemberFieldName(memberSchema, fieldIndex);
                        if (fieldNames.contains(fieldName)) {
                            throw new DataException("Multiple union schemas map to the Connect union field name");
                        }
                        fieldNames.add(fieldName);
                        builder.field(fieldName, toConnectSchema(memberSchema));
                    }
                    fieldIndex++;
                }
                break;
            }
            case NULL:
                throw new DataException("Null schemas are not supported");

            default:
                throw new DataException("Unsupported schema type: " + schema.getType().getName());
        }

        Object parameters = schema.getObjectProp(CONNECT_PARAMETERS_PROP);
        if (parameters != null) {
            if (!(parameters instanceof Map)) {
                throw new DataException("Expected JSON object for schema parameters. Got: " + parameters);
            }
            Map<String, Object> params = (Map<String, Object>) parameters;
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                Object jsonValue = entry.getValue();
                if (!(jsonValue instanceof String)) {
                    throw new DataException("Schema parameter value is not a string. Got: " + jsonValue);
                }
                builder.parameter(entry.getKey(), (String) jsonValue);
            }
        }

        for (Map.Entry<String, Object> entry : schema.getObjectProps().entrySet()) {
            if (entry.getKey().startsWith(AVRO_PROP)) {
                builder.parameter(entry.getKey(), entry.getValue().toString());
            }
        }

        Object connectNameJson = schema.getObjectProp(CONNECT_NAME_PROP);
        String name = null;
        if (connectNameJson != null) {
            if (!(connectNameJson instanceof String)) {
                throw new DataException("Invalid schema name: " + connectNameJson.toString());
            }
            name = (String) connectNameJson;

        } else if (schema.getType() == org.apache.avro.Schema.Type.RECORD
            || schema.getType() == org.apache.avro.Schema.Type.ENUM
            || schema.getType() == org.apache.avro.Schema.Type.FIXED) {
            name = schema.getFullName();
        }
        if (name != null && !name.startsWith(DEFAULT_SCHEMA_FULL_NAME)) {
            if (builder.name() != null) {
                if (!name.equals(builder.name())) {
                    throw new DataException("Schema name that has already been added to SchemaBuilder ("
                        + builder.name() + ") differs from name in source schema (" + name + ")");
                }
            } else {
                builder.name(name);
            }
        }

        return builder.build();
    }

    /**
     * Convert a Connect data to plain Object.
     * @param schema Connect schema
     * @param avroSchema Avro schema
     * @param logicalValue Connect logical value
     * @param buildGenericRecord Boolean for whether to build generic record
     * @return Plain Object
     */
    public Object fromConnectData(Schema schema, org.apache.avro.Schema avroSchema,
                                  Object logicalValue, boolean buildGenericRecord) {
        Schema.Type schemaType =
            schema != null ? schema.type() : getSchemaFromLogicalValue(logicalValue);

        if (schemaType == null) {
            if (buildGenericRecord) {
                return new GenericRecordBuilder(ANYTHING_SCHEMA).build();
            }

            return null;
        }

        if (logicalValue == null && !schema.isOptional()) {
            throw new DataException("Found null value for non-optional schema");
        }

        if (logicalValue == null) {
            if (buildGenericRecord) {
                return new GenericRecordBuilder(ANYTHING_SCHEMA).build();
            }

            return null;
        }

        Object bytesValue = logicalValue;

        // Logical Type Check
        if (schema != null && schema.name() != null) {
            switch (schema.name()) {
                case Decimal.LOGICAL_NAME:
                    if (!(bytesValue instanceof BigDecimal)) {
                        throw new DataException("Can't convert type for Decimal, expected BigDecimal but got "
                            + bytesValue.getClass());
                    }
                    bytesValue = Decimal.fromLogical(schema, (BigDecimal) logicalValue);
                    break;
                case Date.LOGICAL_NAME:
                    if (!(bytesValue instanceof java.util.Date)) {
                        throw new DataException(
                            "Can't convert type for Date, expected Date but got " + bytesValue.getClass());
                    }
                    bytesValue = Date.fromLogical(schema, (java.util.Date) logicalValue);
                    break;
                case Time.LOGICAL_NAME:
                    if (!(bytesValue instanceof java.util.Date)) {
                        throw new DataException(
                            "Can't convert type for Time, expected Date but got " + bytesValue.getClass());
                    }
                    bytesValue = Time.fromLogical(schema, (java.util.Date) logicalValue);
                    break;
                case Timestamp.LOGICAL_NAME:
                    if (!(bytesValue instanceof java.util.Date)) {
                        throw new DataException(
                            "Can't convert type for Timestamp, expected Date but got " + bytesValue.getClass());
                    }
                    bytesValue = Timestamp.fromLogical(schema, (java.util.Date) logicalValue);
                    break;
                default:
                    break;
            }
        }

        try {
            switch (schemaType) {
                case INT8: {
                    Byte byteValue = (Byte) bytesValue;
                    Integer convertedByteValue = byteValue == null ? null : byteValue.intValue();
                    return convertedByteValue;
                }
                case INT16: {
                    Short shortValue = (Short) bytesValue;
                    Integer convertedShortValue = shortValue == null ? null : shortValue.intValue();
                    return convertedShortValue;
                }
                case INT32:
                    Integer intValue = (Integer) bytesValue;
                    return intValue;
                case INT64:
                    Long longValue = (Long) bytesValue;
                    return longValue;
                case FLOAT32:
                    Float floatValue = (Float) bytesValue;
                    return floatValue;
                case FLOAT64:
                    Double doubleValue = (Double) bytesValue;
                    return doubleValue;
                case BOOLEAN:
                    Boolean boolValue = (Boolean) bytesValue;
                    return boolValue;
                case STRING:
                    if (schema != null && schema.parameters() != null
                        && schema.parameters().containsKey(AVRO_TYPE_ENUM)) {
                        String enumSchemaName = schema.parameters().get(AVRO_TYPE_ENUM);
                        return enumSymbol(avroSchema, bytesValue, enumSchemaName);
                    } else {
                        String stringValue = (String) bytesValue;
                        return stringValue;
                    }
                case BYTES: {
                    bytesValue = bytesValue instanceof byte[] ? ByteBuffer.wrap((byte[]) bytesValue)
                        : (ByteBuffer) bytesValue;

                    if (schema != null && isFixedSchema(schema)) {
                        int size = Integer.parseInt(schema.parameters().get(CONNECT_AVRO_FIXED_SIZE_PROP));
                        org.apache.avro.Schema fixedSchema = null;
                        if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
                            int index = 0;
                            for (org.apache.avro.Schema memberSchema : avroSchema.getTypes()) {
                                if (memberSchema.getType() == org.apache.avro.Schema.Type.FIXED
                                    && memberSchema.getFixedSize() == size
                                    && unionMemberFieldName(memberSchema, index)
                                    .equals(unionMemberFieldName(schema, index))) {
                                    fixedSchema = memberSchema;
                                }
                                index++;
                            }
                            if (fixedSchema == null) {
                                throw new DataException("Fixed size " + size + " not found in union " + avroSchema);
                            }
                        } else {
                            fixedSchema = avroSchema;
                        }
                        bytesValue = new GenericData.Fixed(fixedSchema, ((ByteBuffer) bytesValue).array());
                    }

                    return bytesValue;
                }
                case ARRAY: {
                    Collection<Object> list = (Collection<Object>) bytesValue;
                    List<Object> arrayValue = new ArrayList<>(list.size());
                    Schema arrayElementSchema = schema != null ? schema.valueSchema() : null;
                    org.apache.avro.Schema underlyingAvroSchema = getAvroSchema(schema, avroSchema);
                    org.apache.avro.Schema arrayElementAvroSchema =
                        schema != null ? underlyingAvroSchema.getElementType() : ANYTHING_SCHEMA;
                    for (Object val : list) {
                        arrayValue.add(fromConnectData(arrayElementSchema, arrayElementAvroSchema, val, false));
                    }

                    return arrayValue;
                }

                case MAP: {
                    Map<Object, Object> map = (Map<Object, Object>) bytesValue;
                    org.apache.avro.Schema underlyingAvroSchema;
                    if (schema != null && schema.keySchema().type() == Schema.Type.STRING
                        && (!schema.keySchema().isOptional())) {
                        underlyingAvroSchema = getAvroSchema(schema, avroSchema);
                        Map<String, Object> mapValue = new HashMap<>();
                        for (Map.Entry<Object, Object> entry : map.entrySet()) {
                            Object convertedValue = fromConnectData(schema.valueSchema(),
                                underlyingAvroSchema.getValueType(), entry.getValue(), false);
                            mapValue.put((String) entry.getKey(), convertedValue);
                        }
                        return mapValue;
                    } else {
                        List<GenericRecord> mapValue = new ArrayList<>(map.size());
                        underlyingAvroSchema = getAvroSchemaWithMapEntry(schema, avroSchema);
                        org.apache.avro.Schema elementSchema =
                            schema != null ? underlyingAvroSchema.getElementType()
                                : ANYTHING_SCHEMA_MAP_ELEMENT;
                        org.apache.avro.Schema avroKeySchema = elementSchema.getField(KEY_FIELD).schema();
                        org.apache.avro.Schema avroValueSchema = elementSchema.getField(VALUE_FIELD).schema();
                        for (Map.Entry<Object, Object> entry : map.entrySet()) {
                            Object keyConverted = fromConnectData(schema != null ? schema.keySchema() : null,
                                avroKeySchema, entry.getKey(), false);
                            Object valueConverted = fromConnectData(schema != null ? schema.valueSchema() : null,
                                avroValueSchema, entry.getValue(), false);
                            mapValue.add(new GenericRecordBuilder(elementSchema).set(KEY_FIELD, keyConverted)
                                .set(VALUE_FIELD, valueConverted).build());
                        }
                        return mapValue;
                    }
                }
                case STRUCT: {
                    Struct struct = (Struct) bytesValue;
                    if (!struct.schema().equals(schema)) {
                        throw new DataException("Mismatching struct schema");
                    }

                    if (AVRO_TYPE_UNION.equals(schema.name())) {
                        for (Field field : schema.fields()) {
                            Object object = struct.get(field);
                            if (object != null) {
                                return fromConnectData(field.schema(), avroSchema, object, false);
                            }
                        }
                        return fromConnectData(schema, avroSchema, null, true);
                    } else {
                        org.apache.avro.Schema underlyingAvroSchema = getAvroSchema(schema, avroSchema);
                        GenericRecordBuilder convertedBuilder = new GenericRecordBuilder(underlyingAvroSchema);
                        for (Field field : schema.fields()) {
                            String fieldName = field.name();
                            org.apache.avro.Schema.Field schemaField = underlyingAvroSchema.getField(fieldName);
                            org.apache.avro.Schema fieldAvroSchema = schemaField.schema();
                            Object fieldValue = struct.get(field);
                            convertedBuilder.set(fieldName,
                                fromConnectData(field.schema(), fieldAvroSchema, fieldValue, false));
                        }
                        return convertedBuilder.build();
                    }
                }
                default:
                    throw new DataException("Unknown schema type: " + schema.type());
            }
        } catch (ClassCastException e) {
            throw new DataException("Invalid type for " + schema.type() + ": " + bytesValue.getClass());
        }
    }

    /**
     * Convert a Connect schema to an Avro schema.
     * @param schema The Connect schema to convert
     * @param setOptional Boolean for whether to set the schema as optional
     * @return The corresponding Avro schema
     */
    public org.apache.avro.Schema fromConnectSchema(Schema schema, boolean setOptional) {
        if (schema == null) {
            return ANYTHING_SCHEMA;
        }

        final org.apache.avro.Schema avroSchema;

        switch (schema.type()) {
            case INT8:
                avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
                break;
            case INT16:
                avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
                break;
            case INT32:
                avroSchema = org.apache.avro.SchemaBuilder.builder().intType();
                break;
            case INT64:
                avroSchema = org.apache.avro.SchemaBuilder.builder().longType();
                break;
            case FLOAT32:
                avroSchema = org.apache.avro.SchemaBuilder.builder().floatType();
                break;
            case FLOAT64:
                avroSchema = org.apache.avro.SchemaBuilder.builder().doubleType();
                break;
            case BOOLEAN:
                avroSchema = org.apache.avro.SchemaBuilder.builder().booleanType();
                break;
            case STRING:
                avroSchema = org.apache.avro.SchemaBuilder.builder().stringType();
                break;
            case BYTES:
                if (isFixedSchema(schema)) {
                    Pair<String, String> names = getNameOrDefault(schema.name());
                    String namespace = names.getKey();
                    String name = names.getValue();
                    avroSchema = org.apache.avro.SchemaBuilder.builder().fixed(name).namespace(namespace)
                        .size(Integer.parseInt(schema.parameters().get(CONNECT_AVRO_FIXED_SIZE_PROP)));
                } else {
                    avroSchema = org.apache.avro.SchemaBuilder.builder().bytesType();
                }
                if (Decimal.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    int scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
                    avroSchema.addProp(AVRO_LOGICAL_DECIMAL_SCALE_PROP, new IntNode(scale));
                    if (schema.parameters().containsKey(CONNECT_AVRO_DECIMAL_PRECISION_PROP)) {
                        String precisionValue = schema.parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP);
                        int precision = Integer.parseInt(precisionValue);
                        avroSchema.addProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP, new IntNode(precision));
                    } else {
                        avroSchema.addProp(AVRO_LOGICAL_DECIMAL_PRECISION_PROP,
                            new IntNode(CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT));
                    }
                }
                break;
            case ARRAY:
                avroSchema = org.apache.avro.SchemaBuilder.builder().array()
                    .items(fromConnectSchema(schema.valueSchema(), false));
                break;
            case MAP:
                if (schema.keySchema().type() == Schema.Type.STRING && !schema.keySchema().isOptional()) {
                    avroSchema = org.apache.avro.SchemaBuilder.builder().map()
                        .values(fromConnectSchema(schema.valueSchema(), false));
                } else {
                    List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
                    final org.apache.avro.Schema mapSchema;
                    if (schema.name() == null) {
                        mapSchema =
                            org.apache.avro.Schema.createRecord(MAP_ENTRY_TYPE_NAME, null, NAMESPACE, false);
                    } else {
                        Pair<String, String> names = getNameOrDefault(schema.name());
                        String namespace = names.getKey();
                        String name = names.getValue();
                        mapSchema = org.apache.avro.Schema.createRecord(name, null, namespace, false);
                        mapSchema.addProp(CONNECT_INTERNAL_TYPE_NAME, MAP_ENTRY_TYPE_NAME);
                    }
                    addAvroRecordField(fields, KEY_FIELD, schema.keySchema(), null);
                    addAvroRecordField(fields, VALUE_FIELD, schema.valueSchema(), null);
                    mapSchema.setFields(fields);
                    avroSchema = org.apache.avro.Schema.createArray(mapSchema);
                }
                break;
            case STRUCT:
                if (AVRO_TYPE_UNION.equals(schema.name())) {
                    List<org.apache.avro.Schema> unionSchemas = new ArrayList<>();
                    if (schema.isOptional()) {
                        unionSchemas.add(org.apache.avro.SchemaBuilder.builder().nullType());
                    }
                    for (Field field : schema.fields()) {
                        unionSchemas.add(fromConnectSchema(field.schema(), false));
                    }
                    avroSchema = org.apache.avro.Schema.createUnion(unionSchemas);
                } else if (schema.isOptional()) {
                    List<org.apache.avro.Schema> unionSchemas = new ArrayList<>();
                    unionSchemas.add(org.apache.avro.SchemaBuilder.builder().nullType());
                    unionSchemas.add(fromConnectSchema(schema, false));
                    avroSchema = org.apache.avro.Schema.createUnion(unionSchemas);
                } else {
                    Pair<String, String> names = getNameOrDefault(schema.name());
                    String namespace = names.getKey();
                    String name = names.getValue();
                    String doc =
                        schema.parameters() != null ? schema.parameters().get(AVRO_RECORD_DOC_PROP) : null;
                    avroSchema = org.apache.avro.Schema.createRecord(name, doc, namespace, false);
                    List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
                    for (Field field : schema.fields()) {
                        String fieldName = field.name();
                        String fieldDoc = null;
                        if (schema.parameters() != null) {
                            fieldDoc = schema.parameters().get(AVRO_FIELD_DOC_PREFIX_PROP + field.name());
                        }
                        addAvroRecordField(fields, fieldName, field.schema(), fieldDoc);
                    }
                    avroSchema.setFields(fields);
                }
                break;
            default:
                throw new DataException("Unknown schema type: " + schema.type());
        }

        if (!avroSchema.getType().equals(org.apache.avro.Schema.Type.UNION)) {
            if (schema.name() != null) {
                if (Decimal.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    String precisionString = schema.parameters().get(CONNECT_AVRO_DECIMAL_PRECISION_PROP);
                    String scaleString = schema.parameters().get(Decimal.SCALE_FIELD);
                    int precision = precisionString == null ? CONNECT_AVRO_DECIMAL_PRECISION_DEFAULT
                        : Integer.parseInt(precisionString);
                    int scale = scaleString == null ? 0 : Integer.parseInt(scaleString);
                    org.apache.avro.LogicalTypes.decimal(precision, scale).addToSchema(avroSchema);
                } else if (Time.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    org.apache.avro.LogicalTypes.timeMillis().addToSchema(avroSchema);
                } else if (Timestamp.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    org.apache.avro.LogicalTypes.timestampMillis().addToSchema(avroSchema);
                } else if (Date.LOGICAL_NAME.equalsIgnoreCase(schema.name())) {
                    org.apache.avro.LogicalTypes.date().addToSchema(avroSchema);
                }
            }

            if (schema.parameters() != null) {
                for (Map.Entry<String, String> entry : schema.parameters().entrySet()) {
                    if (entry.getKey().startsWith(AVRO_PROP)) {
                        avroSchema.addProp(entry.getKey(), entry.getValue());
                    }
                }
            }
        }

        if (setOptional) {
            return MakeOptional(schema, avroSchema);
        }

        return avroSchema;
    }

    // Static helper functions

    private static boolean schemaEquals(Schema src, Schema that) {
        return schemaEquals(src, that, new HashMap<>());
    }

    private static boolean schemaEquals(Schema src, Schema that,
                                        Map<Pair<Schema, Schema>, Boolean> cache) {
        if (src == that) {
            return true;
        } else if (src == null || that == null) {
            return false;
        }

        Pair<Schema, Schema> sp = Pair.of(src, that);
        Boolean cacheHit = cache.putIfAbsent(sp, true);
        if (cacheHit != null) {
            return cacheHit;
        }

        boolean equals = Objects.equals(src.isOptional(), that.isOptional())
            && Objects.equals(src.version(), that.version()) && Objects.equals(src.name(), that.name())
            && Objects.equals(src.doc(), that.doc()) && Objects.equals(src.type(), that.type())
            && Objects.deepEquals(src.defaultValue(), that.defaultValue())
            && Objects.equals(src.parameters(), that.parameters());

        switch (src.type()) {
            case STRUCT:
                equals = equals && fieldListEquals(src.fields(), that.fields(), cache);
                break;
            case ARRAY:
                equals = equals && schemaEquals(src.valueSchema(), that.valueSchema(), cache);
                break;
            case MAP:
                equals = equals && schemaEquals(src.valueSchema(), that.valueSchema(), cache)
                    && schemaEquals(src.keySchema(), that.keySchema(), cache);
                break;
            default:
                break;
        }
        cache.put(sp, equals);
        return equals;
    }

    private static boolean fieldListEquals(List<Field> one, List<Field> two,
                                           Map<Pair<Schema, Schema>, Boolean> cache) {
        if (one == two) {
            return true;
        } else if (one == null || two == null) {
            return false;
        } else {
            ListIterator<Field> itOne = one.listIterator();
            ListIterator<Field> itTwo = two.listIterator();
            while (itOne.hasNext() && itTwo.hasNext()) {
                if (!fieldEquals(itOne.next(), itTwo.next(), cache)) {
                    return false;
                }
            }
            return itOne.hasNext() == itTwo.hasNext();
        }
    }

    private static boolean fieldEquals(Field one, Field two,
                                       Map<Pair<Schema, Schema>, Boolean> cache) {
        if (one == two) {
            return true;
        } else if (one == null || two == null) {
            return false;
        } else {
            return one.getClass() == two.getClass() && Objects.equals(one.index(), two.index())
                && Objects.equals(one.name(), two.name())
                && schemaEquals(one.schema(), two.schema(), cache);
        }
    }

    private static String unionMemberFieldName(org.apache.avro.Schema schema, int index) {
        if (schema.getType() == org.apache.avro.Schema.Type.RECORD
            || schema.getType() == org.apache.avro.Schema.Type.ENUM
            || schema.getType() == org.apache.avro.Schema.Type.FIXED) {
            return splitName(schema.getName())[1];
        }
        return schema.getType().getName();
    }

    private boolean isMapEntry(final org.apache.avro.Schema mapElementSchema) {
        if (!mapElementSchema.getType().equals(org.apache.avro.Schema.Type.RECORD)) {
            return false;
        }
        if (NAMESPACE.equals(mapElementSchema.getNamespace())
            && MAP_ENTRY_TYPE_NAME.equals(mapElementSchema.getName())) {
            return true;
        }
        if (Objects.equals(mapElementSchema.getProp(CONNECT_INTERNAL_TYPE_NAME), MAP_ENTRY_TYPE_NAME)) {
            return true;
        }
        return false;
    }

    private static org.apache.avro.Schema getAvroSchemaWithMapEntry(Schema schema,
                                                                    org.apache.avro.Schema avroSchema) {

        if (schema != null && schema.isOptional()) {
            if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
                for (org.apache.avro.Schema typeSchema : avroSchema.getTypes()) {
                    if (!typeSchema.getType().equals(org.apache.avro.Schema.Type.NULL)
                        && Schema.Type.ARRAY.getName().equals(typeSchema.getType().getName())) {
                        return typeSchema;
                    }
                }
            } else {
                throw new DataException(
                    "An optional schema should have a Union type, not " + schema.type());
            }
        }

        return avroSchema;
    }

    private static org.apache.avro.Schema getAvroSchema(Schema schema,
                                                        org.apache.avro.Schema avroSchema) {
        if (schema != null && schema.isOptional()) {
            if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
                for (org.apache.avro.Schema typeSchema : avroSchema.getTypes()) {
                    if (!typeSchema.getType().equals(org.apache.avro.Schema.Type.NULL)) {
                        return typeSchema;
                    }
                }
            } else {
                throw new DataException(
                    "An optional schema should have a Union type, not " + schema.type());
            }
        }

        return avroSchema;
    }

    private static Schema.Type getSchemaFromLogicalValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Byte) {
            return Schema.Type.INT8;
        } else if (value instanceof Short) {
            return Schema.Type.INT16;
        } else if (value instanceof Integer) {
            return Schema.Type.INT32;
        } else if (value instanceof Long) {
            return Schema.Type.INT64;
        } else if (value instanceof Float) {
            return Schema.Type.FLOAT32;
        } else if (value instanceof Double) {
            return Schema.Type.FLOAT64;
        } else if (value instanceof Boolean) {
            return Schema.Type.BOOLEAN;
        } else if (value instanceof String) {
            return Schema.Type.STRING;
        } else if (value instanceof Collection) {
            return Schema.Type.ARRAY;
        } else if (value instanceof Map) {
            return Schema.Type.MAP;
        } else {
            throw new DataException("Unknown Java type for schemaless data: " + value.getClass());
        }
    }

    private void addAvroRecordField(List<org.apache.avro.Schema.Field> fields, String fieldName,
                                    Schema fieldSchema, String fieldDoc) {
        Object defaultVal = null;
        if (fieldSchema.defaultValue() != null) {
            defaultVal = JacksonUtils
                .toObject(getDefaultValueFromConnect(fieldSchema, fieldSchema.defaultValue()));
        } else if (fieldSchema.isOptional()) {
            defaultVal = org.apache.avro.JsonProperties.NULL_VALUE;
        }

        org.apache.avro.Schema.Field field;
        org.apache.avro.Schema schema = fromConnectSchema(fieldSchema, true);
        try {
            field = new org.apache.avro.Schema.Field(fieldName, schema,
                fieldDoc != null ? fieldDoc : fieldSchema.doc(), defaultVal);
        } catch (AvroTypeException e) {
            field = new org.apache.avro.Schema.Field(fieldName, schema,
                fieldDoc != null ? fieldDoc : fieldSchema.doc());
        }
        fields.add(field);
    }

    private JsonNode getDefaultValueFromConnect(Schema schema, Object value) {
        try {
            if (value == null) {
                return NullNode.getInstance();
            }
            Object defaultVal = value;

            switch (schema.type()) {
                case INT8:
                    return JsonNodeFactory.instance.numberNode(((Byte) defaultVal).intValue());
                case INT16:
                    return JsonNodeFactory.instance.numberNode(((Short) defaultVal).intValue());
                case INT32:
                    return JsonNodeFactory.instance.numberNode((Integer) defaultVal);
                case INT64:
                    return JsonNodeFactory.instance.numberNode((Long) defaultVal);
                case FLOAT32:
                    return JsonNodeFactory.instance.numberNode((Float) defaultVal);
                case FLOAT64:
                    return JsonNodeFactory.instance.numberNode((Double) defaultVal);
                case BOOLEAN:
                    return JsonNodeFactory.instance.booleanNode((Boolean) defaultVal);
                case STRING:
                    return JsonNodeFactory.instance.textNode((String) defaultVal);
                case BYTES:
                    if (defaultVal instanceof byte[]) {
                        return JsonNodeFactory.instance
                            .textNode(new String((byte[]) defaultVal, StandardCharsets.ISO_8859_1));
                    } else {
                        return JsonNodeFactory.instance.textNode(
                            new String(((ByteBuffer) defaultVal).array(), StandardCharsets.ISO_8859_1));
                    }
                case ARRAY: {
                    ArrayNode array = JsonNodeFactory.instance.arrayNode();
                    for (Object elem : (Collection<Object>) defaultVal) {
                        array.add(getDefaultValueFromConnect(schema.valueSchema(), elem));
                    }
                    return array;
                }
                case MAP:
                    if (schema.keySchema().type() == Schema.Type.STRING) {
                        ObjectNode node = JsonNodeFactory.instance.objectNode();
                        for (Map.Entry<String, Object> entry : ((Map<String, Object>) defaultVal).entrySet()) {
                            JsonNode entryDef =
                                getDefaultValueFromConnect(schema.valueSchema(), entry.getValue());
                            node.set(entry.getKey(), entryDef);
                        }
                        return node;
                    } else {
                        ArrayNode array = JsonNodeFactory.instance.arrayNode();
                        for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) defaultVal).entrySet()) {
                            JsonNode keyDefault = getDefaultValueFromConnect(schema.keySchema(), entry.getKey());
                            JsonNode valDefault =
                                getDefaultValueFromConnect(schema.valueSchema(), entry.getValue());
                            ArrayNode jsonEntry = JsonNodeFactory.instance.arrayNode();
                            jsonEntry.add(keyDefault);
                            jsonEntry.add(valDefault);
                            array.add(jsonEntry);
                        }
                        return array;
                    }
                case STRUCT: {
                    boolean isUnion = AVRO_TYPE_UNION.equals(schema.name());
                    ObjectNode node = JsonNodeFactory.instance.objectNode();
                    Struct struct = ((Struct) defaultVal);
                    for (Field field : (schema.fields())) {
                        String fieldName = field.name();
                        JsonNode fieldDef = getDefaultValueFromConnect(field.schema(), struct.get(field));
                        if (isUnion) {
                            return fieldDef;
                        }
                        node.set(fieldName, fieldDef);
                    }

                    return node;
                }
                default:
                    throw new DataException("Unknown schema type:" + schema.type());
            }
        } catch (ClassCastException e) {
            throw new DataException("Invalid type used for default value of " + schema.type() + " field: "
                + schema.defaultValue().getClass());
        }
    }

    private Pair<String, String> getNameOrDefault(String name) {
        if (name != null) {
            String[] split = splitName(name);
            return Pair.of(split[0], split[1]);
        } else {
            return Pair.of(NAMESPACE, DEFAULT_SCHEMA_NAME);
        }
    }

    private static String[] splitName(String fullName) {
        String[] result = new String[2];
        int indexLastDot = fullName.lastIndexOf('.');
        if (indexLastDot >= 0) {
            result[0] = fullName.substring(0, indexLastDot);
            result[1] = fullName.substring(indexLastDot + 1);
        } else {
            result[0] = null;
            result[1] = fullName;
        }
        return result;
    }

    private boolean isInstanceOfAvroSchemaTypeForSimpleSchema(Schema fieldSchema, Object value,
                                                              int index) {
        if (isEnumSchema(fieldSchema)) {
            String paramName = AVRO_TYPE_ENUM;
            String enumSchemaName = fieldSchema.parameters().get(paramName);
            if (value instanceof GenericData.EnumSymbol) {
                return ((GenericData.EnumSymbol) value).getSchema().getFullName().equals(enumSchemaName);
            } else {
                return value.getClass().getName().equals(enumSchemaName);
            }
        }
        List<Class> classes = SIMPLE_AVRO_SCHEMA_TYPES.get(fieldSchema.type());
        if (classes == null) {
            return false;
        }
        for (Class type : classes) {
            if (type.isInstance(value)) {
                if (isFixedSchema(fieldSchema)) {
                    if (fixedValueSizeMatch(fieldSchema, value,
                        Integer.parseInt(fieldSchema.parameters().get(CONNECT_AVRO_FIXED_SIZE_PROP)),
                        index)) {
                        return true;
                    }
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean fixedValueSizeMatch(Schema fieldSchema, Object value, int size, int index) {
        if (value instanceof byte[]) {
            return ((byte[]) value).length == size;
        } else if (value instanceof ByteBuffer) {
            return ((ByteBuffer) value).remaining() == size;
        } else if (value instanceof GenericFixed) {
            return unionMemberFieldName(((GenericFixed) value).getSchema(), index)
                .equals(unionMemberFieldName(fieldSchema, index));
        } else {
            throw new DataException("Invalid class for fixed, expecting GenericFixed, byte[]"
                + " or ByteBuffer but found " + value.getClass());
        }
    }

    private static String unionMemberFieldName(Schema schema, int index) {
        if (schema.type() == Schema.Type.STRUCT || isEnumSchema(schema) || isFixedSchema(schema)) {
            return splitName(schema.name())[1];
        }
        return CONNECT_TYPES_TO_AVRO_TYPES.get(schema.type()).getName();
    }

    private static boolean isFixedSchema(Schema schema) {
        return schema.type() == Schema.Type.BYTES && schema.name() != null
            && schema.parameters() != null
            && schema.parameters().containsKey(CONNECT_AVRO_FIXED_SIZE_PROP);
    }

    private static boolean isEnumSchema(Schema schema) {
        return schema.type() == Schema.Type.STRING && schema.parameters() != null
            && schema.parameters().containsKey(AVRO_TYPE_ENUM);
    }

    private org.apache.avro.Schema MakeOptional(Schema schema, org.apache.avro.Schema baseSchema) {
        if (!schema.isOptional()) {
            return baseSchema;
        }

        if (schema.defaultValue() != null) {
            return org.apache.avro.SchemaBuilder.builder().unionOf().type(baseSchema).and().nullType()
                .endUnion();
        } else {
            return org.apache.avro.SchemaBuilder.builder().unionOf().nullType().and().type(baseSchema)
                .endUnion();
        }
    }

    private EnumSymbol enumSymbol(org.apache.avro.Schema avroSchema, Object value,
                                  String enumSchemaName) {
        org.apache.avro.Schema enumSchema;
        if (avroSchema.getType() == org.apache.avro.Schema.Type.UNION) {
            int enumIndex = avroSchema.getIndexNamed(enumSchemaName);
            enumSchema = avroSchema.getTypes().get(enumIndex);
        } else {
            enumSchema = avroSchema;
        }
        return new GenericData.EnumSymbol(enumSchema, (String) value);
    }
}
