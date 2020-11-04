<p align="center">
  <img src="event-hubs.png" alt="Microsoft Azure Event Hubs" width="100"/>
</p>

<h1>Azure Schema Registry for Kafka</h1> 

Azure Schema Registry is a hosted schema repository service provided by Azure Event Hubs, designed to simplify schema management and data governance.

Azure Schema Registry provides:
- Schema versioning and evolution
- Kafka and AMQP client plugins for serialization and deserialization
- Role-based access control for schemas and schema groups

An overview of Azure Schema Registry can be found on the [Event Hubs docs page](https://docs.microsoft.com/en-us/azure/event-hubs/schema-registry-overview). 

Sample code can be found in implementation level folders (e.g. [avro](csharp/avro/samples))

# Implementations

Each Kafka Schema Registry serializer will be backed by common code hosted in the Azure Central SDK repositories.

Base serialization implementations can be found at the following repository links by language:
- Java - [azure-sdk-for-java](https://github.com/Azure/azure-sdk-for-java/tree/master/sdk/schemaregistry/azure-data-schemaregistry-avro)
- C# - [azure-sdk-for-dotnet](https://github.com/Azure/azure-sdk-for-net/tree/master/sdk/schemaregistry/Microsoft.Azure.Data.SchemaRegistry.ApacheAvro)
- Python - [azure-sdk-for-python](https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/schemaregistry/azure-schemaregistry-avroserializer)
- JS/TS - [azure-sdk-for-js](https://github.com/Azure/azure-sdk-for-js/tree/master/sdk/schemaregistry/schema-registry-avro)

Sample code can be found in the `/samples` directories at the following links.

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
