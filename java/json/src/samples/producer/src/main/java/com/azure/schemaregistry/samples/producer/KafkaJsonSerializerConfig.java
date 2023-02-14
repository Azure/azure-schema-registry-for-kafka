package com.azure.schemaregistry.samples.producer;

import java.util.Map;
import com.azure.core.credential.TokenCredential;

public class KafkaJsonSerializerConfig {
  private Map<String, Object> props;

  KafkaJsonSerializerConfig(Map<String, Object> props) {
    this.props = (Map<String, Object>) props;
  }

  public String getSchemaRegistryUrl() {
    return (String) this.props.get("schema.registry.url");
  }

  public TokenCredential getCredential() {
    return (TokenCredential) this.props.get("schema.registry.credential");
  }

  public String getSchemaGroup() {
    return (String) this.props.get("schema.group");
  }
}
