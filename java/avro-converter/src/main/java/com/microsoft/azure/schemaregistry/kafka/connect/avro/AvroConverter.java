package com.microsoft.azure.schemaregistry.kafka.connect.avro;

import java.util.Map;
import org.apache.avro.Schema.Parser;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import com.azure.core.credential.TokenCredential;
import com.azure.core.experimental.models.MessageWithMetadata;
import com.azure.core.util.BinaryData;
import com.azure.core.util.serializer.TypeReference;
import com.azure.data.schemaregistry.SchemaRegistryAsyncClient;
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroException;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializer;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializerBuilder;
import com.azure.data.schemaregistry.models.SchemaRegistrySchema;
import com.azure.identity.ClientSecretCredentialBuilder;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.kafka.serializers.NonRecordContainer;

public class AvroConverter implements Converter {
  private SchemaRegistryAsyncClient schemaRegistryClient;
  private SchemaRegistryApacheAvroSerializer serializer;
  private SchemaRegistryApacheAvroSerializer deserializer;

  private AvroData avroData;
  private AvroConverterConfig avroConverterConfig;

  public AvroConverter() {
  }

  // Public only for testing
  public AvroConverter(SchemaRegistryAsyncClient client) {
    schemaRegistryClient = client;
  }

  public void configure(Map<String, ?> configs, boolean isKey) {
    this.avroConverterConfig = new AvroConverterConfig(configs);
    this.avroData = new AvroData(new AvroDataConfig(configs));

    TokenCredential tokenCredential = new ClientSecretCredentialBuilder()
      .tenantId((String) this.avroConverterConfig.getProps().get("tenant.id"))
      .clientId((String) this.avroConverterConfig.getProps().get("client.id"))
      .clientSecret((String) this.avroConverterConfig.getProps().get("client.secret"))
      .build();

    if (schemaRegistryClient == null) {
      schemaRegistryClient = new SchemaRegistryClientBuilder()
      .fullyQualifiedNamespace(this.avroConverterConfig.getSchemaRegistryUrl())
      .credential(tokenCredential)
      .buildAsyncClient();
    }

    serializer = new SchemaRegistryApacheAvroSerializerBuilder()
    .schemaRegistryAsyncClient(schemaRegistryClient)
    .schemaGroup(this.avroConverterConfig.getSchemaGroup())
    .buildSerializer();

    deserializer = new SchemaRegistryApacheAvroSerializerBuilder()
    .schemaRegistryAsyncClient(schemaRegistryClient)
    .schemaGroup(this.avroConverterConfig.getSchemaGroup())
    .buildSerializer();
  }

  public byte[] fromConnectData(String topics, Schema schema, Object value) {
    return fromConnectData(topics, null, schema, value);
  }

  public byte[] fromConnectData(String topics, Headers headers, Schema schema, Object value) {
    Object avroObject = avroData.fromConnectData(schema, value);
    Object avroValue = avroObject instanceof NonRecordContainer
          ? ((NonRecordContainer) avroObject).getValue()
          : avroObject;
    
    try {
      MessageWithMetadata message = serializer.serializeMessageData(
        avroValue,
        TypeReference.createInstance(MessageWithMetadata.class)
      );

      byte[] contentTypeBytes = message.getContentType().getBytes();
      headers.add("content-type", contentTypeBytes);
      
      return message.getBodyAsBinaryData().toBytes();
    } catch (SchemaRegistryApacheAvroException e) {
      throw new DataException("Failed to serialize Avro data: ", e);
    } catch (Exception e) {
      throw e;
    }
  }

  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return toConnectData(topic, null, value);
  }

  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    String contentTypeString = "";
    String schemaId = "";

    try {
      MessageWithMetadata message = new MessageWithMetadata();
      message.setBodyAsBinaryData(BinaryData.fromBytes(value));

      Header contentTypeHeader = headers.lastHeader("content-type");
      if (contentTypeHeader != null) {
        contentTypeString = new String(contentTypeHeader.value());
        message.setContentType(contentTypeString);
      }
      
      Object deserializedMessage = deserializer.deserializeMessageData(
        message,
        TypeReference.createInstance(this.avroConverterConfig.getAvroSpecificType())
      );

      String[] splitSchemaId = contentTypeString.split("\\+");
      if (splitSchemaId.length < 2) {
        throw new DataException("Failed to prase schema id " + splitSchemaId[0]);
      }    
      schemaId = splitSchemaId[1];
      
      SchemaRegistrySchema srSchema = schemaRegistryClient.getSchema(schemaId).block();
      
      return avroData.toConnectData(new Parser().parse(srSchema.getDefinition()), deserializedMessage);
    } catch (SchemaRegistryApacheAvroException e) {
      throw new DataException("Failed to deserialize Avro data: ", e);
    } catch (Exception e) {
      throw e;
    }
  }
}
