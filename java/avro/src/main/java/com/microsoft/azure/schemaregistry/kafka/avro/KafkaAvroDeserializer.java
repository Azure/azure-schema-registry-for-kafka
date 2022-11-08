// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.avro;

import com.azure.core.experimental.models.MessageWithMetadata;
import com.azure.core.util.BinaryData;
import com.azure.core.util.serializer.TypeReference;
import com.azure.data.schemaregistry.SchemaRegistryClientBuilder;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializer;
import com.azure.data.schemaregistry.apacheavro.SchemaRegistryApacheAvroSerializerBuilder;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Deserializer implementation for Kafka consumer, implementing Kafka Deserializer interface.
 *
 * Byte arrays are converted into Java objects by using the schema referenced by GUID prefix to deserialize the payload.
 *
 * Receiving Avro GenericRecords and SpecificRecords is supported.  Avro reflection capabilities have been disabled on
 * com.azure.schemaregistry.kafka.KafkaAvroSerializer.
 *
 * @see KafkaAvroSerializer See serializer class for upstream serializer implementation
 */
public class KafkaAvroDeserializer<T> implements Deserializer<T> {
    private SchemaRegistryApacheAvroSerializer serializer;
    private KafkaAvroDeserializerConfig config;
    private Class<T> targetClass;

    /**
     * Empty constructor used by Kafka consumer
     */
    public KafkaAvroDeserializer() {
        super();
    }

    /**
     * SpecificRecord Constuctor
     * @param targetClass Class to deserialize into
     * @param props Properties for config and serializer
     */
    public KafkaAvroDeserializer(Class<T> targetClass, Properties props) {
        super();
        this.targetClass = targetClass;

        Map<?, ?> propsMap = props;
        this.config = new KafkaAvroDeserializerConfig(new HashMap<>((Map<String, ?>) propsMap));

        this.serializer = new SchemaRegistryApacheAvroSerializerBuilder()
            .schemaRegistryAsyncClient(
                new SchemaRegistryClientBuilder()
                    .fullyQualifiedNamespace(this.config.getSchemaRegistryUrl())
                    .credential(this.config.getCredential())
                    .buildAsyncClient())
            .avroSpecificReader(this.config.getAvroSpecificReader())
            .buildSerializer();
    }

    /**
     * Configures deserializer instance.
     *
     * @param props Map of properties used to configure instance
     * @param isKey Indicates if deserializing record key or value.  Required by Kafka deserializer interface,
     *              no specific functionality has been implemented for key use.
     *
     * @see KafkaAvroDeserializerConfig Deserializer will use configs found in here and inherited classes.
     */
    public void configure(Map<String, ?> props, boolean isKey) {
        this.config = new KafkaAvroDeserializerConfig((Map<String, Object>) props);

        this.serializer = new SchemaRegistryApacheAvroSerializerBuilder()
            .schemaRegistryAsyncClient(
                new SchemaRegistryClientBuilder()
                    .fullyQualifiedNamespace(this.config.getSchemaRegistryUrl())
                    .credential(this.config.getCredential())
                    .buildAsyncClient())
            .avroSpecificReader(this.config.getAvroSpecificReader())
            .buildSerializer();
    }

    /**
     * Deserializes byte array into Java object
     * @param topic topic associated with the record bytes
     * @param bytes serialized bytes, may be null
     * @return deserialize object, may be null
     */
    @Override
    public T deserialize(String topic, byte[] bytes) {
        return null;
    }

    /**
     * Deserializes byte array into Java object
     * @param topic topic associated with the record bytes
     * @param bytes serialized bytes, may be null
     * @param headers record headers, may be null
     * @return deserialize object, may be null
     */
    @Override
    public T deserialize(String topic, Headers headers, byte[] bytes) {
        MessageWithMetadata message = new MessageWithMetadata();
        message.setBodyAsBinaryData(BinaryData.fromBytes(bytes));

        Header contentTypeHeader = headers.lastHeader("content-type");
        if (contentTypeHeader != null) {
            message.setContentType(new String(contentTypeHeader.value()));
        } else {
            message.setContentType("");
        }

        // this.targetClass is null when deserializing GenericRecords
        Class<?> classInstance = this.targetClass == null ? Object.class : this.targetClass;        
        return (T) this.serializer.deserializeMessageData(message, TypeReference.createInstance(classInstance));
    }

    @Override
    public void close() { }
}
