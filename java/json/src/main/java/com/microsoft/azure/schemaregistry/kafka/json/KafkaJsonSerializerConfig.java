// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import java.util.Map;

/**
 * Class containing configuration properties for KafkaJsonSerializer class.
 */
public final class KafkaJsonSerializerConfig extends AbstractKafkaSerdeConfig {
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

    public static final Boolean AUTO_REGISTER_SCHEMAS_CONFIG_DEFAULT = false;

    /**
     * Specifies schema group for interacting with Azure Schema Registry service.
     *
     * If auto-registering schemas, schema will be stored under this group.
     * If not auto-registering, serializer will request schema ID for matching data schema under specified group.
     */
    public static final String SCHEMA_GROUP_CONFIG = "schema.group";

    KafkaJsonSerializerConfig(Map<String, Object> props) {
        super(props);
    }

    /**
     * @return auto-registration flag, with default set to false
     */
    public Boolean getAutoRegisterSchemas() {
        return (Boolean) this.getProps().getOrDefault(
                AUTO_REGISTER_SCHEMAS_CONFIG, AUTO_REGISTER_SCHEMAS_CONFIG_DEFAULT);
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
}
