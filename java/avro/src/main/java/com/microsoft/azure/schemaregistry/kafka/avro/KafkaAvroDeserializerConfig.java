// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro;

import java.util.Map;

/**
 *
 */
public final class KafkaAvroDeserializerConfig extends AbstractKafkaSerdeConfig {

    /**
     * Configures deserializer to decode into SpecificRecord class instance when reading encoded bytes
     *
     * Defaults to false (using Avro GenericRecords)
     */
    public static final String AVRO_SPECIFIC_READER_CONFIG = "specific.avro.reader";

    public static final Boolean AVRO_SPECIFIC_READER_CONFIG_DEFAULT = false;

    public static final String AVRO_SPECIFIC_VALUE_TYPE_CONFIG = "specific.avro.value.type";

    KafkaAvroDeserializerConfig(Map<String, Object> props) {
        super(props);
    }

    /**
     * @return avro specific reader flag, with default set to false
     */
    public Boolean getAvroSpecificReader() {
        return (Boolean) this.getProps().getOrDefault(
                AVRO_SPECIFIC_READER_CONFIG, AVRO_SPECIFIC_READER_CONFIG_DEFAULT);
    }

    /**
     * @return avro specific class flag, with default set to Object class
     */
    public Class<?> getAvroSpecificType() {
        return (Class<?>) this.getProps().getOrDefault(AVRO_SPECIFIC_VALUE_TYPE_CONFIG, Object.class);
    }
}
