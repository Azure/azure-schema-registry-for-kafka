﻿//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro
{
    using System;
    using System.IO;
    using System.Threading;
    using global::Azure.Core;
    using global::Azure.Data.SchemaRegistry;
    using Confluent.Kafka;
    using Microsoft.Azure.Data.SchemaRegistry.ApacheAvro;

    /// <summary>
    /// Implementation of Confluent .NET Kafka async serializer, wrapping Azure Schema Registry C# implementation.
    /// 
    /// Serializer should be used for GenericRecord type or generated SpecificRecord types.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class KafkaAvroSerializer<T> : ISerializer<T>
    {
        private readonly SchemaRegistryAvroObjectSerializer serializer;

        public KafkaAvroSerializer(string schemaRegistryUrl, TokenCredential credential, string schemaGroup, Boolean autoRegisterSchemas = false)
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

        public byte[] Serialize(T o, SerializationContext context)
        {
            if (o == null)
            {
                return null;
            }

            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, o, typeof(T), CancellationToken.None);
                return stream.ToArray();
            }
        }
    }
}
