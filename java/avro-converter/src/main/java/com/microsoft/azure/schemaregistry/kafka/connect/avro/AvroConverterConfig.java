// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.connect.avro;

import java.util.HashMap;
import java.util.Map;

import com.azure.core.credential.TokenCredential;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroException;

/**
 * Configuration settings for {@link AvroConverter}.
 */
public class AvroConverterConfig extends AbstractKafkaSerdeConfig {
    /**
     * Converter configuration constructor.
     * @param props properties
     */
    public AvroConverterConfig(Map<String, ?> props) {
        super((Map<String, Object>) props);
    }

    /**
     * Builder class for {@link AvroConverterConfig}.
     */
    public static class Builder {
        private Map<String, Object> props = new HashMap<String, Object>();

        /**
         * Builder
         * @param key Azure Schema Registry configuration key
         * @param value Azure Schema Registry configuration value
         * @return Builder instance
         */
        public Builder with(String key, Object value) {
            props.put(key, value);
            return this;
        }

        /**
         * @return {@link AvroConverterConfig} instance
         */
        public AvroConverterConfig build() {
            return new AvroConverterConfig(props);
        }
    }
}

class AbstractKafkaSerdeConfig {
    private Map<String, Object> props;

    /**
     * Required.
     * <p>
     * Sets the service endpoint for the Azure Schema Registry instance
     */
    public static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";

    /**
     * Required.
     * <p>
     * Sets the {@link TokenCredential} to use when authenticating HTTP requests for this
     */
    public static final String SCHEMA_REGISTRY_CREDENTIAL_CONFIG = "schema.registry.credential";

    /**
     * Schema cache size limit on underlying SchemaRegistryClient. If limit is exceeded on any cache,
     * all caches are recycled.
     */
    public static final String MAX_SCHEMA_MAP_SIZE_CONFIG = "max.schema.map.size";

    /**
     * Specifies schema group for interacting with Azure Schema Registry service.
     * <p>
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
