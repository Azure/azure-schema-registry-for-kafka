//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using global::Azure.Core;
    using global::Azure.Data.SchemaRegistry;
    using Confluent.Kafka;
    using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;

    /// <summary>
    /// Implementation of Confluent .NET Kafka async serializer, wrapping Azure Schema Registry C# implementation.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class KafkaAvroAsyncSerializer<T> : IAsyncSerializer<T>
    {
        private readonly SchemaRegistryAvroObjectSerializer serializer;
        
        public KafkaAvroAsyncSerializer(string schemaRegistryUrl, TokenCredential credential, string schemaGroup, Boolean autoRegisterSchemas = false)
        {
            this.serializer = new SchemaRegistryAvroObjectSerializer(
                new SchemaRegistryClient(
                    schemaRegistryUrl,
                    credential),
                schemaGroup,
                new SchemaRegistryAvroObjectSerializerOptions()
                {
                    AutoRegisterSchemas = autoRegisterSchemas
                });
        }

        public async Task<byte[]> SerializeAsync(T o, SerializationContext context)
        {
            if (o == null)
            {
                return null;
            }

            using (var stream = new MemoryStream())
            {
                await serializer.SerializeAsync(stream, o, typeof(T), CancellationToken.None);
                return stream.ToArray();
            }
        }
    }
}
