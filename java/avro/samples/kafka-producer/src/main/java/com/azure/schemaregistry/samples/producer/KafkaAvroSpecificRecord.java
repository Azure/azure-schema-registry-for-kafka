package com.azure.schemaregistry.samples.producer;

import com.azure.core.credential.TokenCredential;
import com.azure.schemaregistry.samples.Order;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import com.azure.core.util.logging.ClientLogger;
import org.apache.kafka.common.header.Header;

import java.util.Arrays;
import java.util.Properties;
import java.util.List;

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
        KafkaProducer<String, Order> producer = new KafkaProducer<String, Order>(props);

        String key = "sample-key";

        while (true) {
            for (int i = 0; i < 10; i++) {
                Order order = new Order("ID-" + i, 10.99 + i, "Sample order -" + i);
                // Implementation of getting schema id from service is under development
                // To deserialize, schema id must be manually added in a Kafka message header as a workaround
                String schemaId = "{Place Schema Id Here}";
                List<Header> headers = Arrays.asList(new RecordHeader("SchemaIdBytes", schemaId.getBytes()));
                ProducerRecord<String, Order> record = new ProducerRecord<String, Order>(topicName, null, key, order, headers);
                producer.send(record);
                logger.info("Sent Order {}", order);
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

