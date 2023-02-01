## Kafka Avro Integration for Azure Schema Registry
This document will show how to run Kafka-integrated Apache Avro serializers and deserializers backed by Azure Schema Registry. 

## Prerequisites
This tutorial requires an Event Hubs namespace with Schema Registry enabled.

Basic Kafka Java producer and consumer scenarios are covered in the [Java Kafka quickstart](https://github.com/Azure/azure-event-hubs-for-kafka/tree/master/quickstart/java).

Before running your sample, a schema group with serialization type 'Avro' and with the default compatibility mode should be created.  This is accomplished most easily through the Schema Registry blade in the Azure portal. 

## Configuration
All required configurations for Kafka producer and consumer can be found in [producer configuration](kafka-producer/src/main/resources/app.properties) and [consumer configuration](kafka-consumer/src/main/resources/app.properties) respectively. 



The following Event Hub configurations are required:
- `bootstrap.servers` - EH namespace 
- `sasl.jaas.config` - JAAS configuration for SASL - EH namespace, SAS key name, and SAS key value should be replaced
- `topic` - EH topic for produce and consume operations

The following Schema Registry configurations are required:
- `schema.registry.url` - EH namespace with Schema Registry enabled (does not have to be the same as `bootstrap.servers`)
- `schema.group` - Schema Registry group with serialization type 'Avro' (*must be created before the application runs*)
- `use.managed.identity.credential` - indicates that MSI credentials should be used, should be used for MSI-enabled VM
- `managed.identity.clientId` - if specified, will build MSI credential with given client Id
  `managed.identity.resourceId` - if specified, will build MSI credential with given resource Id
- `tenant.id` - sets the tenant ID of the application
- `client.id` - sets the client ID of the application
- `client.secret` - sets the client secret for authentication

## Running the sample

This sample contains four scenarios: 
- Producing Avro SpecificRecords  
- Consuming Avro SpecificRecords
- Producing Avro GenericRecords
- Consuming Avro GenericRecords

Production environments should use SpecificRecords only.  GenericRecords should only be use for development.

To work with specific records, the `Order` class must be generated with the `avro-maven-plugin` as specified using the Avro schema for Order type which can be found in either [producer schema](kafka-producer/src/main/resources/java/com/azure/schemaregistry/samples/Order.avsc) or [consumer schema](kafka-producer/src/main/resources/java/com/azure/schemaregistry/samples/Order.avsc).  

This is the schema for `Order` type that we will use in this example. 
```json
{
  "namespace": "com.azure.schemaregistry.samples",
  "type": "record",
  "name": "Order",
  "fields": [
    {
      "name": "id",
      "type": "string"
    },
    {
      "name": "amount",
      "type": "double"
    },
    {
      "name": "description",
      "type": "string"
    }
  ]
}
```

You can generate the classes against either the producer schema or consumer schema by running:
```bash
mvn generate-sources
```
The generated class can be found in the `target/generated-sources` folder.
Then you can update your producer or consumer application as needed by using the generated types. 


### Running the Kafka Producer 
You can run the producer by using the following Maven command. 

```bash
mvn -e clean compile exec:java -Dexec.mainClass="com.azure.schemaregistry.samples.producer.App"
```

### Running the Kafka Consumer 
You can run the consumer by using the following Maven command. 

```bash
mvn -e clean compile exec:java -Dexec.mainClass="com.azure.schemaregistry.samples.consumer.App"
```