// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaAvroSerializerConfigTest {
    @Test
    public void testGetAutoRegisterSchemasDefault() {
        KafkaAvroSerializerConfig config = new KafkaAvroSerializerConfig(Collections.emptyMap());
        assertEquals(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS_CONFIG_DEFAULT, config.getAutoRegisterSchemas());
    }

    @Test
    public void testGetSchemaGroupDefault() {
        KafkaAvroSerializerConfig config = new KafkaAvroSerializerConfig(Collections.emptyMap());
        assertEquals(KafkaAvroSerializerConfig.SCHEMA_GROUP_CONFIG_DEFAULT, config.getSchemaGroup());
    }
}