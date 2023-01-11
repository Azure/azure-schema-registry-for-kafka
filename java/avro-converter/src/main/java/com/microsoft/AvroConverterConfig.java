package com.microsoft;

import java.util.HashMap;
import java.util.Map;
import com.azure.core.credential.TokenCredential;

public class AvroConverterConfig extends KafkaAvroSerializerConfig {
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

class KafkaAvroSerializerConfig extends AbstractKafkaSerdeConfig {
  /**
   * If specified true, serializer will register schemas against Azure Schema Registry service under the specified
   * group.  See Azure Schema Registry documentation for a description of schema registration behavior.
   *
   * If specified false, serializer will simply query the service for an existing ID given schema content.
   * Serialization will fail if the schema has not been pre-created.
   *
   * Auto-registration is **NOT RECOMMENDED** for production scenarios.
   *
   * Requires type String.
   */
  public static final String AUTO_REGISTER_SCHEMAS_CONFIG = "auto.register.schemas";

  public static final String AUTO_REGISTER_SCHEMAS_CONFIG_DEFAULT = "false";

  public static final String AVRO_SPECIFIC_READER_CONFIG = "specific.avro.reader";

  public static final Boolean AVRO_SPECIFIC_READER_CONFIG_DEFAULT = false;

  public static final String AVRO_SPECIFIC_VALUE_TYPE_CONFIG = "specific.avro.value.type";

  /**
   * Specifies schema group for interacting with Azure Schema Registry service.
   *
   * If auto-registering schemas, schema will be stored under this group.
   * If not auto-registering, serializer will request schema ID for matching data schema under specified group.
   */
  public static final String SCHEMA_GROUP_CONFIG = "schema.group";

  KafkaAvroSerializerConfig(Map<String, Object> props) {
      super(props);
  }

  /**
   * @return auto-registration flag, with default set to false
   */
  public Boolean getAutoRegisterSchemas() {
      return Boolean.parseBoolean((String) this.getProps().getOrDefault(
              AUTO_REGISTER_SCHEMAS_CONFIG, AUTO_REGISTER_SCHEMAS_CONFIG_DEFAULT));
  }

  /**
   * @return schema group
   */
  public String getSchemaGroup() {
      if (!this.getProps().containsKey(SCHEMA_GROUP_CONFIG)) {
          throw new NullPointerException("Schema group configuration property is required.");
      }
      return (String) this.getProps().get(SCHEMA_GROUP_CONFIG);
  }

  public Class<?> getAvroSpecificType() {
    return (Class<?>) this.getProps().getOrDefault(AVRO_SPECIFIC_VALUE_TYPE_CONFIG, Object.class);
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

  public TokenCredential getCredential() {
      return (TokenCredential) this.props.get(SCHEMA_REGISTRY_CREDENTIAL_CONFIG);
  }

  public Integer getMaxSchemaMapSize() {
      return (Integer) this.props.getOrDefault(MAX_SCHEMA_MAP_SIZE_CONFIG, MAX_SCHEMA_MAP_SIZE_CONFIG_DEFAULT);
  }
}
