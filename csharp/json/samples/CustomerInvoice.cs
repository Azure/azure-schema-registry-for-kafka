//Copyright (c) Microsoft Corporation. All rights reserved.
//Copyright 2016-2017 Confluent Inc., 2015-2016 Andreas Heider
//Licensed under the MIT License.
//Licensed under the Apache License, Version 2.0
//
//Original Confluent sample modified for use with Azure Event Hubs for Apache Kafka Ecosystems

namespace com.azure.schemaregistry.samples
{
    using System;
    using Newtonsoft.Json;

    [JsonObject]
    public class CustomerInvoice
    {
        [JsonProperty]
        public string InvoiceId { get; set; }
        
        [JsonProperty]
        public string MerchantId { get; set; }
        
        [JsonProperty]
        public int TransactionValueUsd { get; set; }

        [JsonProperty]
        public string UserId { get; set; }
    }
}
