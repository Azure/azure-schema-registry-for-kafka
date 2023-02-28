package com.azure.schemaregistry.samples.consumer;

import java.util.Map;
import com.azure.core.credential.TokenCredential;

public class KafkaJsonDeserializerConfig {
  private Map<String, Object> props;

  KafkaJsonDeserializerConfig(Map<String, Object> props) {
    this.props = (Map<String, Object>) props;
  }

  public String getSchemaRegistryUrl() {
    return (String) this.props.get("schema.registry.url");
  }

  public TokenCredential getCredential() {
    return (TokenCredential) this.props.get("schema.registry.credential");
  }

  public Class<?> getJsonSpecificType() {
    return (Class<?>) this.props.getOrDefault("specific.value.type", Object.class);
  }
}
