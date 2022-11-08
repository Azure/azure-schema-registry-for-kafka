package com.azure.schemaregistry.samples.consumer;

import com.azure.core.credential.TokenCredential;
import com.azure.core.util.logging.ClientLogger;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroDeserializerConfig;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroGenericRecord {
    private static final ClientLogger logger = new ClientLogger(KafkaAvroGenericRecord.class);

    static void consumeGenericRecords(String brokerUrl, String registryUrl, String jaasConfig, String topicName, TokenCredential credential) {
        Properties props = new Properties();

        // EH Kafka configs
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", jaasConfig);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "schema-registry-sample");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // Schema Registry configs
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, credential);

        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    logger.info("Received ConsumerRecord containing GenericRecord as record value - {}", record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
