// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaJsonDeserializerConfigTest {
    @Test
    public void TestJsonSpecificTypeDefault() {
        KafkaJsonDeserializerConfig config = new KafkaJsonDeserializerConfig(Collections.emptyMap());
        assertEquals(Object.class, config.getJsonSpecificType());
    }
}