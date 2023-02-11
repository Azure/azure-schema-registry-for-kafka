//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Kafka.SchemaRegistry.Json
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Text;
    using global::Azure;
    using global::Azure.Core;
    using global::Azure.Data.SchemaRegistry;
    using Confluent.Kafka;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Schema;

    /// <summary>
    /// Implementation of Confluent .NET Kafka serializer, wrapping Azure Schema Registry C# implementation.
    /// 
    /// Serializer should be used for GenericRecord type or generated SpecificRecord types.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class KafkaJsonSerializer<T> : ISerializer<T>
    {
        private readonly SchemaRegistryClient schemaRegistryClient;
        private readonly string schemaGroup;
        private bool autoRegisterSchemas;

        public KafkaJsonSerializer(string schemaRegistryUrl, TokenCredential credential, string schemaGroup, Boolean autoRegisterSchemas = false)
        {
            this.schemaRegistryClient = new SchemaRegistryClient(schemaRegistryUrl, credential);
            this.schemaGroup = schemaGroup;
            this.autoRegisterSchemas = autoRegisterSchemas;
        }

        public byte[] Serialize(T o, SerializationContext context)
        {
            if (o == null)
            {
                return null;
            }

            //BinaryContent content = serializer.Serialize<BinaryContent, T>(o);
            //var schemaIdBytes = Encoding.UTF8.GetBytes(content.ContentType.ToString());
            //context.Headers.Add("content-type", schemaIdBytes);
            //return content.Data.ToArray();
        }
    }
}