package com.azure.schemaregistry.samples;

import com.azure.core.credential.TokenCredential;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroDeserializerConfig;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.azure.core.util.logging.ClientLogger;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;

public class KafkaAvroSpecificRecord {
    private static final ClientLogger logger = new ClientLogger(KafkaAvroSpecificRecord.class);

    static void produceSpecificRecords(
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
                com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer.class);

        // Schema Registry configs
        props.put("schema.registry.url", registryUrl);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, credential);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS_CONFIG, true);
        props.put(KafkaAvroSerializerConfig.SCHEMA_GROUP_CONFIG, schemaGroup);
        KafkaProducer<String, AvroUser> producer = new KafkaProducer<String, AvroUser>(props);

        String key = "sample-key";

        while (true) {
            for (int i = 0; i < 10; i++) {
                // specific record
                AvroUser user = new AvroUser("user" + i, i);
                ProducerRecord<String, AvroUser> record = new ProducerRecord<String, AvroUser>(topicName, key, user);
                producer.send(record);
                logger.info("Sent AvroUser {}", user);
            }
            producer.flush();
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

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

        final Consumer<String, AvroUser> consumer = new KafkaConsumer<String, AvroUser>(props);
        consumer.subscribe(Collections.singletonList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, AvroUser> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, AvroUser> record : records) {
                    logger.info("Received ConsumerRecord containing AvroUser as record value - {}", record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}

