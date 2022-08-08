//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Text;
    using global::Azure;
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
        private readonly SchemaRegistryAvroSerializer serializer;
        
        public KafkaAvroAsyncSerializer(string schemaRegistryUrl, TokenCredential credential, string schemaGroup, Boolean autoRegisterSchemas = false)
        {
            this.serializer = new SchemaRegistryAvroSerializer(
                new SchemaRegistryClient(
                    schemaRegistryUrl,
                    credential),
                schemaGroup,
                new SchemaRegistryAvroSerializerOptions()
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

            BinaryContent content = await serializer.SerializeAsync<BinaryContent, T>(o);
            var schemaIdBytes = Encoding.UTF8.GetBytes(content.ContentType.ToString());
            context.Headers.Add("content-type", schemaIdBytes);
            return content.Data.ToArray();
        }
    }
}
