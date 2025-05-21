// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro;

import com.azure.core.models.MessageContent;
import com.azure.core.util.BinaryData;
import com.azure.core.util.serializer.TypeReference;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializer;
import com.azure.identity.DefaultAzureCredential;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaAvroSerializerTest {
	private SchemaRegistryApacheAvroSerializer mockSerializer;
	private DefaultAzureCredential mockCredential;
	private final String SCHEMA_REGISTRY_URL = "https://test.servicebus.windows.net";
	private final String SCHEMA_GROUP = "test-group";

	@BeforeEach
	public void initMocks() {
		MockitoAnnotations.openMocks(this);
		mockSerializer = Mockito.mock(SchemaRegistryApacheAvroSerializer.class);
		mockCredential = Mockito.mock(DefaultAzureCredential.class);
	}

	@Test
	public void testNullRecordReturnNull() {
		// null record should return null without errors
		KafkaAvroSerializer<GenericRecord> ser = new KafkaAvroSerializer<>();
		assertNull(ser.serialize("topic", (Headers) null, null));
		ser.close();
	}

	@Test
	public void testMissingConfigurationFails() {
		// missing all config should throw RuntimeException (schema registry URL or
		// group required)
		KafkaAvroSerializer<GenericRecord> ser1 = new KafkaAvroSerializer<>();
		assertThrows(RuntimeException.class, () -> ser1.configure(new HashMap<>(), false));
		ser1.close();

		// missing credential but group provided
		Map<String, Object> cfg = new HashMap<>();
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		cfg.put(KafkaAvroSerializerConfig.SCHEMA_GROUP_CONFIG, SCHEMA_GROUP);
		KafkaAvroSerializer<GenericRecord> ser2 = new KafkaAvroSerializer<>();
		assertThrows(RuntimeException.class, () -> ser2.configure(cfg, false));
		ser2.close();
	}

	@Test
	public void testDefaultCredentialConfig() {
		// allow default Azure credential when set
		Map<String, Object> cfg = new HashMap<>();
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		cfg.put(AbstractKafkaSerdeConfig.USE_AZURE_CREDENTIAL, true);
		cfg.put(KafkaAvroSerializerConfig.SCHEMA_GROUP_CONFIG, SCHEMA_GROUP);
		KafkaAvroSerializer<GenericRecord> ser = new KafkaAvroSerializer<>();
		assertDoesNotThrow(() -> ser.configure(cfg, false));
		ser.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSerializeGenericRecord() throws Exception {
		// configure serializer with mock credential
		Map<String, Object> cfg = new HashMap<>();
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, mockCredential);
		cfg.put(KafkaAvroSerializerConfig.SCHEMA_GROUP_CONFIG, SCHEMA_GROUP);
		cfg.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS_CONFIG, true);

		KafkaAvroSerializer<GenericRecord> ser = new KafkaAvroSerializer<>();
		ser.configure(cfg, false);
		// inject mock serializer
		Field f = KafkaAvroSerializer.class.getDeclaredField("serializer");
		f.setAccessible(true);
		f.set(ser, mockSerializer);

		// prepare record and mock response
		GenericRecord rec = createRecord();
		byte[] payload = new byte[] { 0x1, 0x2 };
		// mock MessageContent to return desired headers and payload
		MessageContent mc = Mockito.mock(MessageContent.class);
		when(mc.getContentType()).thenReturn("application/x-avro-binary");
		when(mc.getBodyAsBinaryData()).thenReturn(BinaryData.fromBytes(payload));
		// stub serializer to return our mock MessageContent for any TypeReference
		when(mockSerializer.serialize(eq(rec), ArgumentMatchers.any(TypeReference.class))).thenReturn(mc);

		Headers hdrs = new RecordHeaders();
		byte[] out = ser.serialize("t", hdrs, rec);
		// verify payload and content-type header
		assertArrayEquals(payload, out);
		assertEquals("application/x-avro-binary",
				new String(hdrs.lastHeader("content-type").value(), StandardCharsets.UTF_8));
		// verify underlying serializer called
		verify(mockSerializer).serialize(eq(rec), ArgumentMatchers.any(TypeReference.class));
		ser.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSerializeSpecificRecord() throws Exception {
		// configure serializer with mock credential
		Map<String, Object> cfg = new HashMap<>();
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, mockCredential);
		cfg.put(KafkaAvroSerializerConfig.SCHEMA_GROUP_CONFIG, SCHEMA_GROUP);
		cfg.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS_CONFIG, true);

		KafkaAvroSerializer<org.apache.avro.specific.SpecificRecord> ser = new KafkaAvroSerializer<>();
		ser.configure(cfg, false);
		// inject mock serializer
		Field f = KafkaAvroSerializer.class.getDeclaredField("serializer");
		f.setAccessible(true);
		f.set(ser, mockSerializer);

		// inline SpecificRecord implementation
		class TestSpec extends org.apache.avro.specific.SpecificRecordBase {
			private final Schema schema = new Schema.Parser().parse(
					"{\"type\":\"record\",\"name\":\"TestSpec\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}");
			private String f1 = "value";

			public Schema getSchema() {
				return schema;
			}

			@Override
			public Object get(int i) {
				return f1;
			}

			@Override
			public void put(int i, Object v) {
				f1 = v.toString();
			}
		}
		org.apache.avro.specific.SpecificRecord rec = new TestSpec();
		byte[] payload = new byte[] { 0x5, 0x6 };
		// mock MessageContent
		MessageContent mc = Mockito.mock(MessageContent.class);
		when(mc.getContentType()).thenReturn("application/x-avro-binary");
		when(mc.getBodyAsBinaryData()).thenReturn(BinaryData.fromBytes(payload));
		// stub serializer for specific record
		when(mockSerializer.serialize(eq(rec), ArgumentMatchers.any(TypeReference.class))).thenReturn(mc);

		Headers headers = new RecordHeaders();
		byte[] out = ser.serialize("topic", headers, rec);
		// verify payload and content-type header
		assertArrayEquals(payload, out);
		assertEquals("application/x-avro-binary",
				new String(headers.lastHeader("content-type").value(), StandardCharsets.UTF_8));
		// verify underlying serializer called
		verify(mockSerializer).serialize(eq(rec), ArgumentMatchers.any(TypeReference.class));
		ser.close();
	}

	// helper to create a simple GenericRecord
	private GenericRecord createRecord() {
		String json = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
		Schema schema = new Schema.Parser().parse(json);
		GenericRecord r = new GenericData.Record(schema);
		r.put("f1", "v");
		return r;
	}
}
