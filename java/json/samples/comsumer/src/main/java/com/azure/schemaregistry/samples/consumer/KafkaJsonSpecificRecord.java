package com.azure.schemaregistry.samples.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.azure.core.credential.TokenCredential;
import com.azure.core.util.logging.ClientLogger;
import com.azure.schemaregistry.samples.CustomerInvoice;
import com.azure.schemaregistry.samples.Order;

public class KafkaJsonSpecificRecord {
  private static final ClientLogger logger = new ClientLogger(KafkaJsonSpecificRecord.class);

  public static void consumeSpecificRecords(
    String brokerUrl, String registryUrl, String jaasConfig, String topicName, TokenCredential credential) {
    Properties props = new Properties();

    // EH Kafka Configs
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "PLAIN");
    props.put("sasl.jaas.config", jaasConfig);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "testgroup");
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            com.microsoft.azure.schemaregistry.kafka.json.KafkaJsonDeserializer.class);

    // Schema Registry configs
    props.put("schema.registry.url", registryUrl);
    props.put("schema.registry.credential", credential);
    props.put("auto.register.schemas", true);
    props.put("specific.value.type", CustomerInvoice.class);

    final KafkaConsumer<String, CustomerInvoice> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topicName));

    try {
      System.out.println("Reading records...");
      while (true) {
          ConsumerRecords<String, CustomerInvoice> records = consumer.poll(Duration.ofMillis(5000));
          for (ConsumerRecord<String, CustomerInvoice> record : records) {
              logger.info("Invoice received: " + record.value());
          }
      }
    } finally {
      consumer.close();
    }
  }
}
