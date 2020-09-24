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
    public void testNoSchemaGroupProvidedThenFail() {
        KafkaAvroSerializerConfig config = new KafkaAvroSerializerConfig(Collections.emptyMap());
        try {
            config.getSchemaGroup();
            fail();
        } catch (NullPointerException e) {
            assertTrue(true);
        }
    }
}