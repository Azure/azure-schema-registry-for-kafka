//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Kafka.SchemaRegistry.Json
{
    using Confluent.Kafka;
    using System;

    public class SerializationException : KafkaException
    {
        public SerializationException(Error error) : base(error)
        {
        }

        public SerializationException(Error error, Exception innerException) : base(error, innerException)
        {
        }
    }
}
