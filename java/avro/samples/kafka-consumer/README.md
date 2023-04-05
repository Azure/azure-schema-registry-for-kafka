## [Public Preview] Kafka Avro Integration for Azure Schema Registry
This document will show how to run Kafka-integrated Apache Avro serializers and deserializers backed by Azure Schema Registry

## Prerequisites
This tutorial requires an Event Hubs namespace with Schema Registry enabled.

Basic Kafka Java producer and consumer scenarios are covered in the [Java Kafka quickstart](https://github.com/Azure/azure-event-hubs-for-kafka/tree/master/quickstart/java).

Before running your sample, a schema group with serialization type 'Avro' should be created.  This is accomplished most easily through the Schema Registry blade in the Azure portal.  Any compatibility mode is acceptable for this sample. 

## Configuration
All required configurations can be found in the [configuration file](kafka-producer/src/main/resources/app.properties).

The following Event Hub configurations are required:
- `bootstrap.servers` - EH namespace 
- `sasl.jaas.config` - JAAS configuration for SASL - EH namespace, SAS key name, and SAS key value should be replaced
- `topic` - EH topic for produce and consume operations

The following Schema Registry configurations are required:
- `schema.registry.url` - EH namespace with Schema Registry enabled (does not have to be the same as `bootstrap.servers`)
- `schema.group` - Schema Registry group with serialization type 'Avro' (*must be created before the application runs*)
- `use.managed.identity.credential` - indicates that MSI credentials should be used, should be used for MSI-enabled VM
- `managed.identity.clientId` - if specified, will build MSI credential with given client Id
- `managed.identity.resourceId` - if specified, will build MSI credential with given resource Id
- `tenant.id` - sets the tenant ID of the application
- `client.id` - sets the client ID of the application
- `client.secret` - sets the client secret for authentication

## Running the sample

This sample contains four scenarios - 
- Producing Avro SpecificRecords
- Consuming Avro SpecificRecords
- Producing Avro GenericRecords
- Consuming Avro GenericRecords

Production environments should use SpecificRecords only.  GenericRecords should only be use for development.

To work with specific records, the `AvroUser` class must be generated with the `avro-maven-plugin` as specified by the included [Avro Schema](kafka-producer/src/main/resources/java/com/azure/schemaregistry/samples/AvroUser.avsc).  Generate the schema by running:
```bash
mvn generate-sources
```

The generated class can be found in the `target/generated-sources` folder.

The code can then be run with the follow command:
```bash
mvn -e clean compile exec:java -Dexec.mainClass="com.azure.schemaregistry.samples.producer.App"
```
