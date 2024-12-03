// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro;

import com.azure.core.credential.TokenCredential;
import com.azure.data.schemaregistry.SchemaRegistryClient;

import java.util.Map;

/**
 *
 */
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

    public static final String CREATE_DEFAULT_AZURE_CREDENTIAL = "create.default.azure.credential";


    AbstractKafkaSerdeConfig(Map<String, Object> props) {
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

    public Boolean createDefaultAzureCredential() {
        return (Boolean) this.props.getOrDefault(CREATE_DEFAULT_AZURE_CREDENTIAL, false);
    }

    public Integer getMaxSchemaMapSize() {
        return (Integer) this.props.getOrDefault(MAX_SCHEMA_MAP_SIZE_CONFIG, MAX_SCHEMA_MAP_SIZE_CONFIG_DEFAULT);
    }
}
