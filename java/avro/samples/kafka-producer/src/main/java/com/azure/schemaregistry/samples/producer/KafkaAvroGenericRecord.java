package com.azure.schemaregistry.samples.producer;

import com.azure.core.credential.TokenCredential;
import com.azure.core.util.logging.ClientLogger;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroGenericRecord {
    private static final ClientLogger logger = new ClientLogger(KafkaAvroGenericRecord.class);

    static void produceGenericRecords(String brokerUrl, String schemaRegistryUrl, String jaasConfig, String topicName, String schemaGroup, TokenCredential credential) {
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
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS_CONFIG, true);
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_CREDENTIAL_CONFIG, credential);
        props.put(KafkaAvroSerializerConfig.SCHEMA_GROUP_CONFIG, schemaGroup);
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(props);

        String key = "key1";
        String userSchema = "{\"namespace\": \"com.azure.schemaregistry.samples\", \"type\": \"record\", \"name\": " +
                "\"Order\", \"fields\": [ { \"name\": \"id\", \"type\": \"string\"}, " +
                "{ \"name\": \"amount\", \"type\": \"double\"}, { \"name\": \"description\", \"type\": \"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);

        logger.info("Parsed schema: {}", schema);

        while (true) {
            for (int i = 0; i < 10; i++) {
                GenericRecord avroRecord = new GenericData.Record(schema);
                avroRecord.put("id", "ID-" + i);
                avroRecord.put("amount", 20.00 + i);
                avroRecord.put("description", "Sample order " + i);

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
}
