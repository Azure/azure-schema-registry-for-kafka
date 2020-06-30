// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaAvroSerializerTest {
    @Test
    public void testNullRecordReturnNull() {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        assertEquals(null, serializer.serialize("dummy-topic", null));
    }
}
