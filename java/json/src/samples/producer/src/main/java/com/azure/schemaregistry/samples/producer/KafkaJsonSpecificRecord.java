package com.azure.schemaregistry.samples.producer;

import java.util.Properties;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.azure.core.credential.TokenCredential;
import com.azure.core.util.logging.ClientLogger;
import com.azure.schemaregistry.samples.Order;

public class KafkaJsonSpecificRecord {
  private static final ClientLogger logger = new ClientLogger(KafkaJsonSpecificRecord.class);

  public static void produceSpecificRecords(
    String brokerUrl, String registryUrl, String jaasConfig, String topicName, String schemaGroup, TokenCredential credential) {
    Properties props = new Properties();

    // EH Kafka Configs
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
    props.put("security.protocol", "SASL_SSL");
    props.put("sasl.mechanism", "PLAIN");
    props.put("sasl.jaas.config", jaasConfig);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
            StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
            com.azure.schemaregistry.samples.producer.KafkaJsonSerializer.class);

    // Schema Registry configs
    props.put("schema.registry.url", registryUrl);
    props.put("schema.registry.credential", credential);
    props.put("auto.register.schemas", true);
    props.put("schema.group", schemaGroup);
    KafkaProducer<String, Order> producer = new KafkaProducer<String,Order>(props);

    String key = "sample-key";

    System.out.println("Producing records...");
    try {
    while (true) {
        for (int i = 0; i < 10; i++) {
            Order order = new Order("ID-" + i, 0.99 + i, "Sample order #" + Math.abs(new Random().nextInt()));
            ProducerRecord<String, Order> record = new ProducerRecord<String, Order>(topicName, key, order);
            producer.send(record);
            logger.info("Sent Order {}", order);
            System.out.println("Sent Order " + order.getId());
        }
        producer.flush();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    } finally {
        producer.close();
    }
  }
}
