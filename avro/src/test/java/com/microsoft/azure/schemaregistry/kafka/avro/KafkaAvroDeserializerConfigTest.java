// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaAvroDeserializerConfigTest {
    @Test
    public void testAvroSpecificReaderDefault() {
        KafkaAvroDeserializerConfig config = new KafkaAvroDeserializerConfig(Collections.emptyMap());
        assertEquals(KafkaAvroDeserializerConfig.AVRO_SPECIFIC_READER_CONFIG_DEFAULT, config.getAvroSpecificReader());
    }
}
