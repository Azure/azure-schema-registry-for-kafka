package com.azure.schemaregistry.samples.consumer;

import com.azure.core.credential.TokenCredential;
import com.azure.schemaregistry.samples.Order;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.*;
import com.azure.core.util.logging.ClientLogger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroSpecificRecord {
    private static final ClientLogger logger = new ClientLogger(KafkaAvroSpecificRecord.class);

    static void consumeSpecificRecords(String brokerUrl, String registryUrl, String jaasConfig, String topicName, TokenCredential credential) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", jaasConfig);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-registry-sample");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroDeserializer.class);
                
        props.put("schema.registry.url", registryUrl);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, credential);
        props.put(KafkaAvroDeserializerConfig.AVRO_SPECIFIC_READER_CONFIG, true);
        // Specify class to deserialize record into (defaults to Object.class)
        props.put(KafkaAvroDeserializerConfig.AVRO_SPECIFIC_VALUE_TYPE_CONFIG, Order.class);
        
        final KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, Order> record : records) {
                    logger.info("Order received: " + record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
