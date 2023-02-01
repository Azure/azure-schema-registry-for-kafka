package com.azure.schemaregistry.samples.producer;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredentialBuilder;

import java.io.FileInputStream;
import java.util.Properties;
import java.util.Scanner;

public class App {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.load(new FileInputStream("src/main/resources/app.properties"));

        // EH specific properties
        String brokerUrl = props.getProperty("bootstrap.servers");
        String jaasConfig = props.getProperty("sasl.jaas.config");
        String topicName = props.getProperty("topic");

        // Schema Registry specific properties
        String registryUrl = props.getProperty("schema.registry.url");
        String schemaGroup = props.getProperty("schema.group");

        TokenCredential credential;
        if (props.getProperty("use.managed.identity.credential").equals("true")) {
            if (props.getProperty("managed.identity.clientId") != null) {
                credential = new ManagedIdentityCredentialBuilder()
                        .clientId(props.getProperty("managed.identity.clientId"))
                        .build();
            } else if (props.getProperty("managed.identity.resourceId") != null) {
                credential = new ManagedIdentityCredentialBuilder()
                        .resourceId(props.getProperty("managed.identity.resourceId"))
                        .build();
            } else {
                credential = new ManagedIdentityCredentialBuilder().build();
            }
        } else {
            credential = new ClientSecretCredentialBuilder()
                    .tenantId(props.getProperty("tenant.id"))
                    .clientId(props.getProperty("client.id"))
                    .clientSecret(props.getProperty("client.secret"))
                    .build();
        }

        Scanner in = new Scanner(System.in);

        System.out.println("Enter case number:");
        System.out.println("1 - produce Avro SpecificRecords");
        System.out.println("2 - produce Avro GenericRecords");
        int caseNum = in.nextInt();

        switch (caseNum) {
            case 1:
                KafkaAvroSpecificRecord.produceSpecificRecords(brokerUrl, registryUrl, jaasConfig, topicName, schemaGroup, credential);
                break;
            case 2:
                KafkaAvroGenericRecord.produceGenericRecords(brokerUrl, registryUrl, jaasConfig, topicName, schemaGroup, credential);
                break;
            default:
                System.out.println("no sample matched");
        }
    }
}
