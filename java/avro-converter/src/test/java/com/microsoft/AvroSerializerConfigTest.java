package com.microsoft;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Collections;
import org.junit.jupiter.api.Test;

public class AvroSerializerConfigTest {
    @Test
    public void testGetAutoRegisterSchemasDefault() {
        KafkaAvroSerializerConfig config = new KafkaAvroSerializerConfig(Collections.emptyMap());
        assertEquals(Boolean.parseBoolean(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS_CONFIG_DEFAULT), config.getAutoRegisterSchemas());
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
