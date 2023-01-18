package com.microsoft.azure.schemaregistry.kafka.connect.avro;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import java.util.Collections;
import org.junit.jupiter.api.Test;

public class AvroSerializerConfigTest {
    @Test
    public void testNoSchemaGroupProvidedThenFail() {
        AbstractKafkaSerdeConfig config = new AbstractKafkaSerdeConfig(Collections.emptyMap());
        try {
            config.getSchemaGroup();
            fail();
        } catch (NullPointerException e) {
            assertTrue(true);
        }
    }
}
