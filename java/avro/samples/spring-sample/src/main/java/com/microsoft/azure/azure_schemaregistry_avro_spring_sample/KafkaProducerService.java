package com.microsoft.azure.azure_schemaregistry_avro_spring_sample;

import com.microsoft.azure.azure_schemaregistry_avro_spring_sample.model.Customer;
import com.microsoft.azure.schemaregistry.kafka.avro.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaProducerService {
	private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);

	private final AppConfig appConfig;
	private KafkaProducer<String, Customer> producer;

	public KafkaProducerService(AppConfig appConfig) {
		this.appConfig = appConfig;
	}

	@PostConstruct
	public void init() {
		Properties props = new Properties();

		// Basic Kafka producer settings
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "customer-producer");

		// Azure Schema Registry specific settings
		props.put("schema.registry.url", appConfig.getSchemaRegistryEndpoint());
		props.put("schema.group", appConfig.getSchemaGroup());
		props.put("auto.register.schemas", true);

		// Authentication for Schema Registry
		// Using this value will use the DefaultAzureCredential generated by the Kafka
		// Avro Library
		props.put("use.azure.credential", appConfig.getUseAzureCredential());

		// Safety settings
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		props.put(ProducerConfig.RETRIES_CONFIG, 10);

		// Connection settings for Azure Event Hubs
		props.put("security.protocol", "SASL_SSL");
		props.put("sasl.mechanism", "PLAIN");
		props.put("sasl.jaas.config", appConfig.buildJaasConfig());

		// Create the Kafka producer
		this.producer = new KafkaProducer<String, Customer>(props);

		logger.info("Kafka producer initialized with bootstrap servers: {}", appConfig.getBootstrapServers());
	}

	/**
	 * Sends a Customer message to the Kafka topic
	 * Now passing Customer object directly to the producer, letting
	 * KafkaAvroSerializer handle serialization
	 */
	public void sendCustomerMessage(Customer customer) {
		try {
			// Use customer ID as the key for partitioning
			String key = customer.getId().toString();

			// Create and send the Kafka record with Customer object directly
			ProducerRecord<String, Customer> record = new ProducerRecord<>(
					appConfig.getTopicName(), key, customer);

			producer.send(record, (metadata, exception) -> {
				if (exception != null) {
					logger.error("Error sending message: {}", exception.getMessage(), exception);
				} else {
					logger.info("Message sent successfully to topic: {}, partition: {}, offset: {}",
							metadata.topic(), metadata.partition(), metadata.offset());
				}
			}).get(); // Using get() to make it synchronous for simplicity

		} catch (InterruptedException | ExecutionException e) {
			logger.error("Failed to send customer message", e);
			Thread.currentThread().interrupt();
		}
	}

	@PreDestroy
	public void close() {
		if (producer != null) {
			producer.flush();
			producer.close();
			logger.info("Kafka producer closed");
		}
	}
}