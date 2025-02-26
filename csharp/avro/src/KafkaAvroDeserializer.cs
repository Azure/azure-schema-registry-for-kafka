//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro
{
    using System;
    using System.Text;
    using Confluent.Kafka;
    using global::Azure;
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
        private readonly SchemaRegistryAvroSerializer serializer;

        /// <summary>
        /// Constructor for KafkaAvroDeserializer.
        /// </summary>
        /// <param name="schemaRegistryUrl"></param> URL endpoint for Azure Schema Registry instance
        /// <param name="credential"></param> TokenCredential implementation for OAuth2 authentication
        public KafkaAvroDeserializer(string schemaRegistryUrl, TokenCredential credential)
        {
            this.serializer = new SchemaRegistryAvroSerializer(
                new SchemaRegistryClient(
                    schemaRegistryUrl,
                    credential, 
                    new SchemaRegistryClientOptions
                        {
                            Diagnostics =
                            {
                                ApplicationId = "net-avro-kafka-des-1.0"
                            }
                        }),
                "$default");
        }
        
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (data.IsEmpty)
            {
                return default(T);
            }



            if (context.Headers == null)
            {
                byte[] bytes = data.ToArray();
                byte length = bytes[0];
                byte[] contentTypeHeaderBytes = new byte[length];
                byte[] body = new byte[bytes.Length - 1 - length];
                Array.Copy(bytes, 1, contentTypeHeaderBytes, 0, contentTypeHeaderBytes.Length);
                Array.Copy(bytes, 1 + length, body, 0, body.Length);
                BinaryContent content = new BinaryContent
                {
                    Data = new BinaryData(body),
                };
                content.ContentType = Encoding.UTF8.GetString(contentTypeHeaderBytes);
                return serializer.Deserialize<T>(content);
            }
            else
            {

                BinaryContent content = new BinaryContent
                {
                    Data = new BinaryData(data.ToArray()),
                };

                if (context.Headers.TryGetLastBytes("content-type", out var headerBytes))
                {
                    content.ContentType = Encoding.UTF8.GetString(headerBytes);
                }
                else
                {
                    content.ContentType = string.Empty;
                }

                return serializer.Deserialize<T>(content);
            }
        }
    }
}
