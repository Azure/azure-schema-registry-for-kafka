// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaJsonSerializerTest {
    @Test
    public void testNullRecordReturnNull() {
        KafkaJsonSerializer serializer = new KafkaJsonSerializer();
        assertEquals(null, serializer.serialize("dummy-topic", null));
    }
}