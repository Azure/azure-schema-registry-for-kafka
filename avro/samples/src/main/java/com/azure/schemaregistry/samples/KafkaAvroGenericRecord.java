package com.azure.schemaregistry.samples;

import com.azure.core.credential.TokenCredential;
import com.azure.core.util.logging.ClientLogger;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroDeserializerConfig;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaAvroGenericRecord {
    private static final ClientLogger logger = new ClientLogger(KafkaAvroGenericRecord.class);

    static void produceGenericRecords(String brokerUrl, String registryUrl, String jaasConfig, String topicName, String schemaGroup, TokenCredential credential) {
        Properties props = new Properties();

        // EH Kafka Configs
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerUrl);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", jaasConfig);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);

        // Schema Registry configs
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer.class);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, registryUrl);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS_CONFIG, true);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, credential);
        props.put(KafkaAvroSerializerConfig.SCHEMA_GROUP_CONFIG, schemaGroup);
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props);

        String key = "key1";
        String userSchema = "{\"type\":\"record\",\"name\":\"AvroUser\",\"namespace\":\"com.azure.schemaregistry.samples\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"favoriteNumber\",\"type\":\"int\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);

        logger.info("Parsed schema: {}", schema);

        while (true) {
            for (int i = 0; i < 10; i++) {
                GenericRecord avroRecord = new GenericData.Record(schema);
                avroRecord.put("name", "user" + i);
                avroRecord.put("favoriteNumber", i);

                ProducerRecord<String, GenericRecord> record = new ProducerRecord<String, GenericRecord>(topicName, key, avroRecord);
                producer.send(record);

                logger.info("Sent GenericRecord: {}", record);
            }
            producer.flush();
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

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

        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
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