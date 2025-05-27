// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro;

import com.azure.core.models.MessageContent;
import com.azure.core.util.serializer.TypeReference;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializer;
import com.azure.identity.DefaultAzureCredential;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaAvroDeserializerTest {
	private SchemaRegistryApacheAvroSerializer mockSerializer;
	private DefaultAzureCredential mockCredential;
	private final String SCHEMA_REGISTRY_URL = "https://test.servicebus.windows.net";

	@BeforeEach
	public void initMocks() {
		MockitoAnnotations.openMocks(this);
		mockSerializer = Mockito.mock(SchemaRegistryApacheAvroSerializer.class);
		mockCredential = Mockito.mock(DefaultAzureCredential.class);
	}

	@Test
	public void testNullBytesReturnNull() {
		KafkaAvroDeserializer<GenericRecord> deser = new KafkaAvroDeserializer<>();
		assertNull(deser.deserialize("topic", (byte[]) null));
		deser.close();
	}

	@Test
	public void testMissingConfigurationFails() {
		KafkaAvroDeserializer<GenericRecord> deser = new KafkaAvroDeserializer<>();
		assertThrows(RuntimeException.class, () -> deser.configure(new HashMap<>(), false));
		deser.close();
	}

	@Test
	public void testDefaultCredentialConfiguration() {
		KafkaAvroDeserializer<GenericRecord> deser = new KafkaAvroDeserializer<>();
		Map<String, Object> cfg = new HashMap<>();
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		cfg.put(AbstractKafkaSerdeConfig.USE_AZURE_CREDENTIAL, true);
		assertDoesNotThrow(() -> deser.configure(cfg, false));
		deser.close();
	}

	@Test
	public void testDeserializeGenericRecord() throws Exception {
		Map<String, Object> cfg = new HashMap<>();
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, mockCredential);
		KafkaAvroDeserializer<GenericRecord> deser = new KafkaAvroDeserializer<>();
		deser.configure(cfg, false);
		// inject mock serializer
		Field f = KafkaAvroDeserializer.class.getDeclaredField("serializer");
		f.setAccessible(true);
		f.set(deser, mockSerializer);

		// prepare payload and headers
		byte[] payload = new byte[] { 0x1, 0x2 };
		Headers headers = new RecordHeaders();
		headers.add("content-type", "application/x-avro-binary".getBytes(StandardCharsets.UTF_8));
		// inline GenericRecord response
		GenericRecord rec = createGenericRecord();
		// stub serializer
		when(mockSerializer.deserialize(any(MessageContent.class), any(TypeReference.class))).thenReturn(rec);

		GenericRecord out = deser.deserialize("topic", headers, payload);
		assertSame(rec, out);
		verify(mockSerializer).deserialize(any(MessageContent.class), any(TypeReference.class));
		deser.close();
	}

	@Test
	public void testDeserializeSpecificRecord() throws Exception {
		// inline SpecificRecord implementation
		class TestSpec extends SpecificRecordBase implements SpecificRecord {
			private final Schema schema = new Schema.Parser().parse(
					"{\"type\":\"record\",\"name\":\"TestSpec\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}");
			private String f1 = "val";

			@Override
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
		TestSpec rec = new TestSpec();

		Map<String, Object> cfg = new HashMap<>();
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, mockCredential);
		cfg.put(KafkaAvroDeserializerConfig.AVRO_SPECIFIC_READER_CONFIG, true);
		cfg.put(KafkaAvroDeserializerConfig.AVRO_SPECIFIC_VALUE_TYPE_CONFIG, TestSpec.class);

		KafkaAvroDeserializer<SpecificRecord> deser = new KafkaAvroDeserializer<>();
		deser.configure(cfg, false);
		// inject mock serializer
		Field f = KafkaAvroDeserializer.class.getDeclaredField("serializer");
		f.setAccessible(true);
		f.set(deser, mockSerializer);

		// prepare payload and headers
		byte[] payload = new byte[] { 0x3, 0x4 };
		Headers headers = new RecordHeaders();
		headers.add("content-type", "application/x-avro-binary".getBytes(StandardCharsets.UTF_8));
		// stub serializer
		when(mockSerializer.deserialize(any(MessageContent.class), any(TypeReference.class))).thenReturn(rec);

		SpecificRecord out = deser.deserialize("topic", headers, payload);
		assertSame(rec, out);
		verify(mockSerializer).deserialize(any(MessageContent.class), any(TypeReference.class));
		deser.close();
	}

	// helper to create a simple GenericRecord
	private static GenericRecord createGenericRecord() {
		String json = "{\"type\":\"record\",\"name\":\"TestRecord\",\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
		Schema schema = new Schema.Parser().parse(json);
		GenericRecord record = new GenericData.Record(schema);
		record.put("f1", "v");
		return record;
	}
}
