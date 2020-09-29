//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro
{
    using System;
    using System.IO;
    using System.Threading;
    using Confluent.Kafka;
    using global::Azure.Core;
    using global::Azure.Data.SchemaRegistry;
    using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;

    /// <summary>
    /// Implementation of Confluent .NET Kafka deserializer, wrapping Azure Schema Registry C# implementation.
    /// 
    /// Note that Confluent .NET Kafka removed support for IAsyncDeserializer<T>.  See: https://github.com/confluentinc/confluent-kafka-dotnet/issues/922
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class KafkaAvroDeserializer<T> : IDeserializer<T>
    {
        private readonly SchemaRegistryAvroObjectSerializer serializer;

        /// <summary>
        /// Constructor for KafkaAvroDeserializer.
        /// </summary>
        /// <param name="schemaRegistryUrl"></param> URL endpoint for Azure Schema Registry instance
        /// <param name="credential"></param> TokenCredential implementation for OAuth2 authentication
        public KafkaAvroDeserializer(string schemaRegistryUrl, TokenCredential credential)
        {
            this.serializer = new SchemaRegistryAvroObjectSerializer(new SchemaRegistryClient(schemaRegistryUrl, credential), "$default");
        }
        
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (data.IsEmpty)
            {
                return default(T);
            }

            return (T) this.serializer.Deserialize(new MemoryStream(data.ToArray()), typeof(T), CancellationToken.None);
        }
    }
}
