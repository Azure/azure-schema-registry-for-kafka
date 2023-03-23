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
    /// Sample implementation of Kafka Json deserializaer, wrapping Azure Schema Registry C# implementation.
    /// 
    /// This is meant for reference and sample purposes only, do not use this for production.
    /// </summary>
    /// <typeparam name="T"></typeparam>

    public class KafkaJsonDeserializer<T> : IDeserializer<T>
    {
        readonly SchemaRegistryClient schemaRegistryClient;
        readonly JsonSerializer serializer;

        public KafkaJsonDeserializer(string schemaRegistryUrl, TokenCredential credential)
        {
            this.schemaRegistryClient = new SchemaRegistryClient(schemaRegistryUrl, credential, new SchemaRegistryClientOptions
            {
                Diagnostics =
                {
                    ApplicationId = "azsdk-net-KafkaJsonDeserializer/1.0"
                }
            });
            this.serializer = new JsonSerializer();
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data == null || data.IsEmpty)
            {
                return default(T);
            }

            if (!context.Headers.TryGetLastBytes("schemaId", out var lastHeader) || lastHeader.Length == 0)
            {
                return default(T);
            }

            var schemaId = UTF8Encoding.UTF8.GetString(lastHeader);
            if (string.IsNullOrEmpty(schemaId))
            {
                return default(T);
            }

            var schemaRegistryData = this.schemaRegistryClient.GetSchema(schemaId).Value;
            if (schemaRegistryData.Properties.Format != SchemaFormat.Json)
            {
                throw new SerializationException(new Error(ErrorCode.Local_ValueDeserialization, $"Schema id {schemaId} is not of json format. the schema is a {schemaRegistryData.Properties.Format} schema."));
            }
            else if (string.IsNullOrEmpty(schemaRegistryData.Definition))
            {
                throw new SerializationException(new Error(ErrorCode.Local_ValueDeserialization, $"Schema id {schemaId} has empty schema."));
            }

            // This implementation is actually based on the old Newtonsoft Json implementation which
            // uses a older json-schema draft version.
            // When we updated to use the latest Newtonsoft package/draft, this implementation will
            // need to change using the new classes.
            using (var stringReader = new StringReader(UTF8Encoding.UTF8.GetString(data)))
            {
                JsonTextReader reader = new JsonTextReader(stringReader);
                try
                {
                    JsonValidatingReader validatingReader = new JsonValidatingReader(reader);
                    validatingReader.Schema = JsonSchema.Parse(schemaRegistryData.Definition);

                    IList<string> messages = new List<string>();
                    validatingReader.ValidationEventHandler += (o, a) => messages.Add(a.Message);

                    T obj = serializer.Deserialize<T>(validatingReader);
                    if (messages.Count > 0)
                    {
                        throw new SerializationException(new Error(ErrorCode.Local_ValueDeserialization, string.Concat(messages)));
                    }

                    return obj;
                }
                finally
                {
                    reader.Close();
                }
            }
        }
    }
}
