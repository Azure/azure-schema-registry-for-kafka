package com.microsoft.azure.azure_schemaregistry_avro_spring_sample;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("classpath:application.properties")
public class AppConfig {

	@Value("${kafka.bootstrap.servers}")
	private String bootstrapServers;

	@Value("${kafka.topic.name}")
	private String topicName;

	@Value("${kafka.group.id}")
	private String groupId;

	@Value("${azure.eventhub.connection.string}")
	private String eventHubConnectionString;

	@Value("${azure.schemaregistry.endpoint}")
	private String schemaRegistryEndpoint;

	@Value("${azure.schemaregistry.group}")
	private String schemaGroup;

	// The Kafka SASL JAAS config needs to be built from the Event Hub connection
	// string
	public String buildJaasConfig() {
		// Extract key and shared access signature from connection string
		// Format:
		// Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<key-name>;SharedAccessKey=<key>
		String[] parts = eventHubConnectionString.split(";");
		String endpoint = "";
		String sasKeyName = "";
		String sasKey = "";

		for (String part : parts) {
			if (part.startsWith("Endpoint=")) {
				endpoint = part.substring("Endpoint=".length());
			} else if (part.startsWith("SharedAccessKeyName=")) {
				sasKeyName = part.substring("SharedAccessKeyName=".length());
			} else if (part.startsWith("SharedAccessKey=")) {
				sasKey = part.substring("SharedAccessKey=".length());
			}
		}

		// Extract namespace from endpoint
		String namespace = "";
		if (endpoint.startsWith("sb://")) {
			int endIndex = endpoint.indexOf(".servicebus.windows.net");
			if (endIndex > 0) {
				namespace = endpoint.substring(5, endIndex);
			}
		}

		// Build JAAS config for Event Hubs
		return String.format(
				"org.apache.kafka.common.security.plain.PlainLoginModule required " +
						"username=\"$ConnectionString\" " +
						"password=\"%s\";",
				eventHubConnectionString);
	}

	// Getters for all properties
	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public String getTopicName() {
		return topicName;
	}

	public String getGroupId() {
		return groupId;
	}

	public String getEventHubConnectionString() {
		return eventHubConnectionString;
	}

	public String getSchemaRegistryEndpoint() {
		return schemaRegistryEndpoint;
	}

	public String getSchemaGroup() {
		return schemaGroup;
	}
}