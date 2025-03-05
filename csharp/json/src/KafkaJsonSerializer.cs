//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Kafka.SchemaRegistry.Json
{
    using System;
    using System.IO;
    using System.Threading;
    using System.Text;
    using global::Azure.Core;
    using global::Azure.Data.SchemaRegistry;
    using Confluent.Kafka;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Schema;
    using System.Collections.Generic;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// Sample implementation of Kafka Json serializer, wrapping Azure Schema Registry C# implementation.
    /// 
    /// This is meant for reference and sample purposes only, do not use this for production.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class KafkaJsonSerializer<T> : ISerializer<T>
    {
        readonly JsonSchemaGenerator schemaGenerator;
        readonly SchemaRegistryClient schemaRegistryClient;
        readonly bool autoRegisterSchemas;
        readonly string schemaGroup;

        public KafkaJsonSerializer(string schemaRegistryUrl, TokenCredential credential, string schemaGroup, bool autoRegisterSchemas = false)
        {
            this.schemaRegistryClient = new SchemaRegistryClient(schemaRegistryUrl, credential, new SchemaRegistryClientOptions
            {
                Diagnostics =
                {
                    ApplicationId = "net-json-kafka-ser-1.0"
                }
            });
            this.autoRegisterSchemas = autoRegisterSchemas;
            this.schemaGroup = schemaGroup;
            this.schemaGenerator = new JsonSchemaGenerator();
        }

        public byte[] Serialize(T o, SerializationContext context)
        {
            if (o == null)
            {
                return null;
            }

            // This implementation is actually based on the old Newtonsoft Json implementation which
            // uses a older json-schema draft version.
            // When we updated to use the latest Newtonsoft package/draft, this implementation will
            // need to change using the new classes.
            var schema = schemaGenerator.Generate(typeof(T));
            var jObject = JObject.FromObject(o);
            if (!jObject.IsValid(schema))
            {
                throw new SerializationException(new Error(ErrorCode.Local_ValueSerialization, $"Unexpected parsing error when generating scheam from instance."));
            }

            var schemaJson = schema.ToString();
            SchemaProperties schemaProperties;
            if (this.autoRegisterSchemas)
            {
                schemaProperties = this.schemaRegistryClient.RegisterSchema(
                    this.schemaGroup,
                    typeof(T).FullName,
                    schemaJson,
                    SchemaFormat.Json).Value;
            }
            else
            {
                schemaProperties = this.schemaRegistryClient.GetSchemaProperties(
                    this.schemaGroup,
                    typeof(T).FullName,
                    schemaJson,
                    SchemaFormat.Json).Value;
            }

            if (schemaProperties == null)
            {
                throw new SerializationException(new Error(ErrorCode.Local_ValueSerialization, "Schema registry client returned null response"));
            }
            else if (schemaProperties.Format != SchemaFormat.Json)
            {
                throw new SerializationException(new Error(ErrorCode.Local_ValueSerialization, $"Schema registered was not json type, it was {schemaProperties.Format}"));
            }
            var json = jObject.ToString();
            byte[] recordBytes = UTF8Encoding.UTF8.GetBytes(json);
            if (context.Headers != null)
            {
                context.Headers.Add("schemaId", UTF8Encoding.UTF8.GetBytes(schemaProperties.Id));
                return recordBytes;
            }
            else
            {
                byte[] schemaIdBytes = UTF8Encoding.UTF8.GetBytes(schemaProperties.Id);
                byte[] bytes = new byte[1 + schemaIdBytes.Length + recordBytes.Length];
                bytes[0] = (byte) schemaIdBytes.Length;
                Array.Copy(schemaIdBytes, 0, bytes, 1, schemaIdBytes.Length);
                Array.Copy(recordBytes, 0, bytes, 1 + schemaIdBytes.Length, recordBytes.Length);
                return bytes;
            }

        }
    }
}