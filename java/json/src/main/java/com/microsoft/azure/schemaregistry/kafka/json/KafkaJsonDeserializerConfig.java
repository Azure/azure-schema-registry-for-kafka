// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import java.util.Map;

public final class KafkaJsonDeserializerConfig extends AbstractKafkaSerdeConfig {
    public static final String SPECIFIC_VALUE_TYPE_CONFIG = "specific.avro.value.type";

    KafkaJsonDeserializerConfig(Map<String, Object> props) {
        super(props);
    }

    /**
     * @return  Specific class flag, with default set to Object class
     */
    public Class<?> getJsonSpecificType() {
        return (Class<?>) this.getProps().getOrDefault(SPECIFIC_VALUE_TYPE_CONFIG, Object.class);
    }
}
