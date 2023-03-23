// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaJsonSerializerConfigTest {
    @Test
    public void testGetAutoRegisterSchemasDefault() {
        KafkaJsonSerializerConfig config = new KafkaJsonSerializerConfig(Collections.emptyMap());
        assertEquals(KafkaJsonSerializerConfig.AUTO_REGISTER_SCHEMAS_CONFIG_DEFAULT, config.getAutoRegisterSchemas());
    }

    @Test
    public void testNoSchemaGroupProvidedThenFail() {
        KafkaJsonSerializerConfig config = new KafkaJsonSerializerConfig(Collections.emptyMap());
        try {
            config.getSchemaGroup();
            fail();
        } catch (NullPointerException e) {
            assertTrue(true);
        }
    }
}
