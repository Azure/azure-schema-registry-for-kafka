package com.microsoft.azure.azure_schemaregistry_avro_spring_sample;

import com.microsoft.azure.azure_schemaregistry_avro_spring_sample.model.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class DemoRunner {

	private static final Logger logger = LoggerFactory.getLogger(DemoRunner.class);

	public static void main(String[] args) {
		SpringApplication.run(DemoRunner.class, args);
	}

	@Bean
	public CommandLineRunner demo(KafkaProducerService producerService, KafkaConsumerService consumerService) {
		return args -> {
			try {
				// Start the consumer in a separate thread
				consumerService.startConsuming();

				// Wait a bit for the consumer to initialize
				TimeUnit.SECONDS.sleep(2);

				logger.info("Producing sample customer messages...");

				// Send a few sample customer messages
				for (int i = 1; i <= 20; i++) {
					// Create a customer object
					Customer customer = Customer.newBuilder()
							.setId(UUID.randomUUID().toString())
							.setName("Customer " + i)
							.setEmail("customer" + i + "@example.com")
							.setRegistrationDate(Instant.now())
							.setActive(true)
							.build();

					// Send the customer message
					producerService.sendCustomerMessage(customer);

					logger.info("Sent customer message: {}", customer.getName());

					// Small delay between messages
					TimeUnit.MILLISECONDS.sleep(500);
				}

				// Keep the application running for a while to allow consumer to receive
				// messages
				logger.info("All messages sent. Waiting for consumer to finish processing...");
				TimeUnit.SECONDS.sleep(10);

				// Stop the consumer
				consumerService.stopConsuming();

				logger.info("Demo completed. Exiting application.");

				// Exit the application
				System.exit(0);

			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				logger.error("Demo interrupted", e);
			}
		};
	}
}