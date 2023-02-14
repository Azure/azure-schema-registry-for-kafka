package com.azure.schemaregistry.samples.consumer;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import com.azure.data.schemaregistry.SchemaRegistryClient;
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder;
import com.azure.data.schemaregistry.models.SchemaRegistrySchema;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

public class KafkaJsonDeserializer<T> implements Deserializer<T> {
  private SchemaRegistryClient client;
  private KafkaJsonDeserializerConfig config;

  public KafkaJsonDeserializer() {
    super();
  }

  public void configure(Map<String, ?> props, boolean isKey) {
    this.config = new KafkaJsonDeserializerConfig((Map<String, Object>) props);

    this.client = new SchemaRegistryClientBuilder()
    .fullyQualifiedNamespace(this.config.getSchemaRegistryUrl())
    .credential(this.config.getCredential())
    .buildClient();
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    return null;
  }

  @Override
  public T deserialize(String topic, Headers headers, byte[] data) {
    T dataObject;
    String schemaId;

    ObjectMapper mapper = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setVisibility(mapper.getVisibilityChecker().withFieldVisibility(JsonAutoDetect.Visibility.ANY));
    try {
      dataObject = (T) mapper.readValue(data, this.config.getJsonSpecificType());
    } catch (Exception e) {
      e.printStackTrace();
      throw new Error(e);
    }

    if (headers.lastHeader("schemaId") != null) {
      schemaId = new String(headers.lastHeader("schemaId").value());
    } else {
      throw new RuntimeException("Schema Id was not found in record headers.");
    }
    SchemaRegistrySchema schema = this.client.getSchema(schemaId);

    JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
    JsonSchema jSchema = factory.getSchema(schema.getDefinition());
    JsonNode node;
    try {
      node = mapper.readTree(data);
    } catch (Exception e) {
      e.printStackTrace();
      throw new Error(e);
    }
    Set<ValidationMessage> errors = jSchema.validate(node);

    if (errors.size() == 0) {
      return dataObject;
    } else {
      throw new RuntimeException("Failed to validate Json data. Validation errors:\n" + Arrays.toString(errors.toArray()));
    }
  }

  @Override
  public void close() {}
}
