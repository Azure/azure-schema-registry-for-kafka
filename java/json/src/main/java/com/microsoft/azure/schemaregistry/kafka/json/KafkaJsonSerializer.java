package com.microsoft.azure.schemaregistry.kafka.json;

import java.util.Map;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import com.azure.data.schemaregistry.SchemaRegistryClient;
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder;
import com.azure.data.schemaregistry.models.SchemaFormat;
import com.azure.data.schemaregistry.models.SchemaProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaVersion;

public class KafkaJsonSerializer<T> implements Serializer<T> {
  private SchemaRegistryClient client;
  private String schemaGroup;

  public KafkaJsonSerializer() {
    super();
  }

  @Override
  public void configure(Map<String, ?> props, boolean isKey) {
    KafkaJsonSerializerConfig config = new KafkaJsonSerializerConfig((Map<String, Object>) props);

    this.schemaGroup = config.getSchemaGroup();
    
    this.client = new SchemaRegistryClientBuilder()
    .fullyQualifiedNamespace(config.getSchemaRegistryUrl())
    .credential(config.getCredential())
    .buildClient();
  }

  @Override
  public byte[] serialize(String topic, T data) {
    return null;
  }

  @Override
  public byte[] serialize(String topic, Headers headers, T record) {
    if (record == null) {
      return null;
    }

    byte[] recordBytes;

    ObjectMapper mapper = new ObjectMapper();
    try {
      recordBytes = mapper.writeValueAsBytes(record);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
      throw new Error(e);
    }

    SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON);
    SchemaGeneratorConfig config = configBuilder.build();
    SchemaGenerator generator = new SchemaGenerator(config);
    JsonNode jsonSchema = generator.generateSchema(record.getClass());
    String jsonSchemaString = jsonSchema.toString();

    SchemaProperties schemaProps = this.client.registerSchema(
      this.schemaGroup,
      record.getClass().getName(),
      jsonSchemaString,
      SchemaFormat.JSON
    );

    headers.add("schemaId", schemaProps.getId().getBytes());

    return recordBytes;
  }

  @Override
  public void close() {}
}
