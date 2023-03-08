//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace Microsoft.Azure.Kafka.SchemaRegistry.Json
{
    using System;

    public class JsonSerializerException : Exception
    {
        public JsonSerializerException()
        {
        }

        public JsonSerializerException(string message) : base(message)
        {
        }

        public JsonSerializerException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
