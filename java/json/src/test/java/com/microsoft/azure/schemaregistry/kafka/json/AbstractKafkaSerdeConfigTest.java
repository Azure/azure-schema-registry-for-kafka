// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import com.azure.core.credential.TokenCredential;

public class AbstractKafkaSerdeConfigTest {
  @Test
  public void testMaxSchemaMapSizeDefault() {
      AbstractKafkaSerdeConfig config = new AbstractKafkaSerdeConfig(Collections.emptyMap());
      assertEquals(AbstractKafkaSerdeConfig.MAX_SCHEMA_MAP_SIZE_CONFIG_DEFAULT, config.getMaxSchemaMapSize());
  }

  @Test
  public void testSchemaRegistryUrl() {
      String dummyString = "dummyString"; // does not get validated at this layer
      AbstractKafkaSerdeConfig config = new KafkaJsonSerializerConfig(
              Collections.singletonMap(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_URL_CONFIG, dummyString));
      assertEquals(dummyString, config.getSchemaRegistryUrl());
  }

  @Test
  public void testSchemaRegistryCredential() {
      TokenCredential dummyCredential = tokenRequestContext -> null;
      AbstractKafkaSerdeConfig config = new KafkaJsonSerializerConfig(
          Collections.singletonMap(AbstractKafkaSerdeConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, dummyCredential));
      assertEquals(dummyCredential, config.getCredential());
  }
}
