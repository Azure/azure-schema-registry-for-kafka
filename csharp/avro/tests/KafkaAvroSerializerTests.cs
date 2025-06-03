//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro.Tests
{
    [TestClass]
    public class KafkaAvroSerializerTests
    {
        [TestMethod]
        public void BasicTest()
        {
            Assert.AreEqual(true, true, "Basic test to ensure MSTest is set up correctly.");
        }
    }
}
