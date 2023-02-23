//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace EventHubsForKafkaSample
{
    using System;
    using System.Configuration;

    class Program
    {
        public static void Main(string[] args)
        {
            string brokerList = ConfigurationManager.AppSettings["EH_FQDN"];
            string connectionString = ConfigurationManager.AppSettings["EH_JAAS_CONFIG"];
            string topic = ConfigurationManager.AppSettings["EH_NAME"];
            string consumerGroup = ConfigurationManager.AppSettings["KAFKA_GROUP"];

            Console.WriteLine("Initializing Producer");
            Worker.Producer(brokerList, connectionString, topic).Wait();
            Console.WriteLine();
            Console.WriteLine("Initializing Consumer");
            Worker.Consumer(brokerList, connectionString, consumerGroup, topic);
            Console.ReadKey();
        }
    }
}
