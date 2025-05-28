// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import com.azure.data.schemaregistry.SchemaRegistryClient;
import com.azure.data.schemaregistry.models.SchemaRegistrySchema;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.azure.identity.DefaultAzureCredential;
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
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class KafkaJsonDeserializerTest {
	private SchemaRegistryClient mockSchemaRegistryClient;
	private DefaultAzureCredential mockCredential;
	private final String SCHEMA_REGISTRY_URL = "https://test.servicebus.windows.net";
	private final String TEST_SCHEMA_ID = "test-schema-id";
	private final String TEST_SCHEMA_DEFINITION = "{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},\"value\":{\"type\":\"integer\"}}}";

	static class TestPojo {
		public String name;
		public int value;

		// Default constructor for Jackson deserialization
		public TestPojo() {
		}

		public TestPojo(String name, int value) {
			this.name = name;
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;
			TestPojo testPojo = (TestPojo) o;
			return value == testPojo.value && Objects.equals(name, testPojo.name);
		}

		@Override
		public int hashCode() {
			return Objects.hash(name, value);
		}

		@Override
		public String toString() {
			return "TestPojo{" +
					"name='" + name + '\'' +
					", value=" + value +
					'}';
		}
	}

	@BeforeEach
	public void initMocks() {
		MockitoAnnotations.openMocks(this);
		mockSchemaRegistryClient = Mockito.mock(SchemaRegistryClient.class); // Changed
		mockCredential = Mockito.mock(DefaultAzureCredential.class);
	}

	@Test
	public void testNullBytesReturnNull() {
		KafkaJsonDeserializer<Object> deser = new KafkaJsonDeserializer<>();
		assertNull(deser.deserialize("topic", (byte[]) null));
		deser.close();
	}

	@Test
	public void testMissingConfigurationFails() {
		KafkaJsonDeserializer<Object> deser = new KafkaJsonDeserializer<>();
		assertThrows(RuntimeException.class, () -> deser.configure(new HashMap<>(), false));
		deser.close();
	}

	@Test
	public void testDefaultCredentialConfiguration() {
		KafkaJsonDeserializer<Object> deser = new KafkaJsonDeserializer<>();
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

		KafkaJsonDeserializer<Object> deser = new KafkaJsonDeserializer<>();
		deser.configure(cfg, false);

		Field clientField = KafkaJsonDeserializer.class.getDeclaredField("client");
		clientField.setAccessible(true);
		clientField.set(deser, mockSchemaRegistryClient);

		TestPojo expectedRecordData = new TestPojo("genericName", 789);
		ObjectMapper objectMapper = new ObjectMapper();
		byte[] payload = objectMapper.writeValueAsBytes(expectedRecordData);

		Headers headers = new RecordHeaders();
		headers.add("schemaId", TEST_SCHEMA_ID.getBytes(StandardCharsets.UTF_8));

		// Mock SchemaRegistryClient behavior
		SchemaRegistrySchema mockSchema = Mockito.mock(SchemaRegistrySchema.class);
		when(mockSchema.getDefinition()).thenReturn(TEST_SCHEMA_DEFINITION);
		when(mockSchemaRegistryClient.getSchema(eq(TEST_SCHEMA_ID))).thenReturn(mockSchema);

		Object deserializedObject = deser.deserialize("topic", headers, payload);

		assertNotNull(deserializedObject);
		assertTrue(deserializedObject instanceof Map, "Deserialized object should be a Map for generic records");
		@SuppressWarnings("unchecked") // Safe cast after instanceof check
		Map<String, Object> deserializedMap = (Map<String, Object>) deserializedObject;

		assertEquals(expectedRecordData.name, deserializedMap.get("name"));
		assertEquals(expectedRecordData.value, ((Number) deserializedMap.get("value")).intValue());

		verify(mockSchemaRegistryClient).getSchema(eq(TEST_SCHEMA_ID));
		deser.close();
	}

	@Test
	public void testDeserializeSpecificRecord() throws Exception {
		Map<String, Object> cfg = new HashMap<>();
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, mockCredential);
		cfg.put(KafkaJsonDeserializerConfig.SPECIFIC_VALUE_TYPE_CONFIG, TestPojo.class);

		KafkaJsonDeserializer<TestPojo> deser = new KafkaJsonDeserializer<>();
		deser.configure(cfg, false);

		// Inject mock SchemaRegistryClient
		Field clientField = KafkaJsonDeserializer.class.getDeclaredField("client");
		clientField.setAccessible(true);
		clientField.set(deser, mockSchemaRegistryClient);

		TestPojo expectedRecord = new TestPojo("specificName", 456);
		ObjectMapper objectMapper = new ObjectMapper();
		byte[] payload = objectMapper.writeValueAsBytes(expectedRecord);

		Headers headers = new RecordHeaders();
		headers.add("schemaId", TEST_SCHEMA_ID.getBytes(StandardCharsets.UTF_8));

		// Mock SchemaRegistryClient behavior
		SchemaRegistrySchema mockSchema = Mockito.mock(SchemaRegistrySchema.class);
		when(mockSchema.getDefinition()).thenReturn(TEST_SCHEMA_DEFINITION);
		when(mockSchemaRegistryClient.getSchema(eq(TEST_SCHEMA_ID))).thenReturn(mockSchema);

		TestPojo deserializedRecord = deser.deserialize("topic", headers, payload);

		assertNotNull(deserializedRecord);
		assertEquals(expectedRecord, deserializedRecord);
		verify(mockSchemaRegistryClient).getSchema(eq(TEST_SCHEMA_ID));
		deser.close();
	}
}
