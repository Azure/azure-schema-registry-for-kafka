// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import com.azure.data.schemaregistry.SchemaRegistryClient;
import com.azure.data.schemaregistry.models.SchemaFormat;
import com.azure.data.schemaregistry.models.SchemaProperties;
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

public class KafkaJsonSerializerTest {
	private SchemaRegistryClient mockSchemaRegistryClient;
	private DefaultAzureCredential mockCredential;
	private final String SCHEMA_REGISTRY_URL = "https://test.servicebus.windows.net";
	private final String SCHEMA_GROUP = "test-group";
	private final String TEST_SCHEMA_ID = "test-schema-id";

	// Inner class for testing POJO serialization
	static class TestPojo {
		public String name;
		public int value;

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
	}

	@BeforeEach
	public void initMocks() {
		MockitoAnnotations.openMocks(this);
		mockSchemaRegistryClient = Mockito.mock(SchemaRegistryClient.class);
		mockCredential = Mockito.mock(DefaultAzureCredential.class);
	}

	@Test
	public void testNullRecordReturnNull() {
		// null record should return null without errors
		KafkaJsonSerializer<Object> ser = new KafkaJsonSerializer<>();
		assertNull(ser.serialize("topic", (Headers) null, null));
		ser.close();
	}

	@Test
	public void testMissingConfigurationFails() {
		KafkaJsonSerializer<Object> ser1 = new KafkaJsonSerializer<>();
		assertThrows(RuntimeException.class, () -> ser1.configure(new HashMap<>(), false));
		ser1.close();

		Map<String, Object> cfg = new HashMap<>();
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		cfg.put(KafkaJsonSerializerConfig.SCHEMA_GROUP_CONFIG, SCHEMA_GROUP);
		KafkaJsonSerializer<Object> ser2 = new KafkaJsonSerializer<>();
		assertThrows(RuntimeException.class, () -> ser2.configure(cfg, false));
		ser2.close();
	}

	@Test
	public void testDefaultCredentialConfig() {
		// allow default Azure credential when set
		Map<String, Object> cfg = new HashMap<>();
		cfg.put(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
		cfg.put(AbstractKafkaSerdeConfig.USE_AZURE_CREDENTIAL, true);
		cfg.put(KafkaJsonSerializerConfig.SCHEMA_GROUP_CONFIG, SCHEMA_GROUP);
		KafkaJsonSerializer<Object> ser = new KafkaJsonSerializer<>();
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
		cfg.put(KafkaJsonSerializerConfig.SCHEMA_GROUP_CONFIG, SCHEMA_GROUP);
		cfg.put(KafkaJsonSerializerConfig.AUTO_REGISTER_SCHEMAS_CONFIG, true);

		KafkaJsonSerializer<TestPojo> ser = new KafkaJsonSerializer<>();
		ser.configure(cfg, false);

		// Inject mock SchemaRegistryClient
		Field clientField = KafkaJsonSerializer.class.getDeclaredField("client");
		clientField.setAccessible(true);
		clientField.set(ser, mockSchemaRegistryClient);

		TestPojo record = new TestPojo("testName", 123);

		// Mock SchemaProperties
		SchemaProperties mockSchemaProperties = Mockito.mock(SchemaProperties.class);
		when(mockSchemaProperties.getId()).thenReturn(TEST_SCHEMA_ID);

		// Mock SchemaRegistryClient behavior for auto-registering schema
		when(mockSchemaRegistryClient.registerSchema(
				eq(SCHEMA_GROUP),
				eq(record.getClass().getName()), // Schema name used by KafkaJsonSerializer
				Mockito.anyString(), // The generated schema string
				eq(SchemaFormat.JSON)))
				.thenReturn(mockSchemaProperties);

		Headers hdrs = new RecordHeaders();
		byte[] serializedPayload = ser.serialize("topic", hdrs, record);

		ObjectMapper objectMapper = new ObjectMapper();
		byte[] expectedPayload = objectMapper.writeValueAsBytes(record);
		assertArrayEquals(expectedPayload, serializedPayload);

		assertNotNull(hdrs.lastHeader("schemaId"), "schemaId header should not be null");
		assertEquals(TEST_SCHEMA_ID, new String(hdrs.lastHeader("schemaId").value(), StandardCharsets.UTF_8));

		verify(mockSchemaRegistryClient).registerSchema(
				eq(SCHEMA_GROUP),
				eq(record.getClass().getName()),
				Mockito.anyString(),
				eq(SchemaFormat.JSON));

		ser.close();
	}
}