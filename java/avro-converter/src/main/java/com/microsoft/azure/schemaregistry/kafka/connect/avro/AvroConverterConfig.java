package com.microsoft.azure.schemaregistry.kafka.connect.avro;

import java.util.HashMap;
import java.util.Map;
import com.azure.core.credential.TokenCredential;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroException;

public class AvroConverterConfig extends AbstractKafkaSerdeConfig {
  public AvroConverterConfig(Map<String, ?> props) {
    super((Map<String, Object>) props);
  }

  public static class Builder {
    private Map<String, Object> props = new HashMap<String, Object>();

    public Builder with(String key, Object value) {
      props.put(key, value);
      return this;
    }

    public AvroConverterConfig build() {
      return new AvroConverterConfig(props);
    }
  }
}

class AbstractKafkaSerdeConfig {
  private Map<String, Object> props;

  /**
   * Required.
   *
   * Sets the service endpoint for the Azure Schema Registry instance
   */
  public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";

  /**
   * Required.
   *
   * Sets the {@link TokenCredential} to use when authenticating HTTP requests for this
   *      * {@link SchemaRegistryClient}.
   */
  public static final String SCHEMA_REGISTRY_CREDENTIAL_CONFIG = "schema.registry.credential";

  /**
   * Schema cache size limit on underlying {@link SchemaRegistryClient}. If limit is exceeded on any cache,
   * all caches are recycled.
   */
  public static final String MAX_SCHEMA_MAP_SIZE_CONFIG = "max.schema.map.size";

  /**
   * Specifies schema group for interacting with Azure Schema Registry service.
   *
   * If auto-registering schemas, schema will be stored under this group.
   * If not auto-registering, serializer will request schema ID for matching data schema under specified group.
   */
  public static final String SCHEMA_GROUP_CONFIG = "schema.group";

  public static final String AVRO_SPECIFIC_VALUE_TYPE_CONFIG = "specific.avro.value.type";

  public static final Integer MAX_SCHEMA_MAP_SIZE_CONFIG_DEFAULT = 1000;


  AbstractKafkaSerdeConfig(Map<String, ?> props) {
      this.props = (Map<String, Object>) props;
  }

  public Map<String, Object> getProps() {
      return props;
  }

  public String getSchemaRegistryUrl() {
      return (String) this.props.get(SCHEMA_REGISTRY_URL_CONFIG);
  }

  /**
   * @return schema group
   */
  public String getSchemaGroup() {
      if (!this.getProps().containsKey(SCHEMA_GROUP_CONFIG)) {
          throw new SchemaRegistryApacheAvroException("Schema group configuration property is required.");
      }
      return (String) this.getProps().get(SCHEMA_GROUP_CONFIG);
}

  public TokenCredential getCredential() {
      if (!this.getProps().containsKey(SCHEMA_REGISTRY_CREDENTIAL_CONFIG)) {
        throw new SchemaRegistryApacheAvroException("Token credential for schema registry not found.");
      }
      return (TokenCredential) this.props.get(SCHEMA_REGISTRY_CREDENTIAL_CONFIG);
  }

  public Integer getMaxSchemaMapSize() {
      return (Integer) this.props.getOrDefault(MAX_SCHEMA_MAP_SIZE_CONFIG, MAX_SCHEMA_MAP_SIZE_CONFIG_DEFAULT);
  }

  public Class<?> getAvroSpecificType() {
      return (Class<?>) this.getProps().getOrDefault(AVRO_SPECIFIC_VALUE_TYPE_CONFIG, Object.class);
  }
}
