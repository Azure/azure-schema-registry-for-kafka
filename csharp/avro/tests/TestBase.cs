//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Azure.Core;

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro.Tests
{
	/// <summary>
	/// Base class for all Kafka Avro serializer tests, providing common setup and test infrastructure.
	/// </summary>
	public abstract class TestBase
	{
		protected Mock<TokenCredential> mockCredential;
		protected const string ValidSchemaRegistryUrl = "https://test-schema-registry.servicebus.windows.net";
		protected const string ValidSchemaGroup = "test-group";

		[TestInitialize]
		public virtual void Setup()
		{
			mockCredential = new Mock<TokenCredential>();
		}

		#region Test Helper Classes

		protected class TestClass
		{
			public string Name { get; set; }
			public int Age { get; set; }
		}

		#endregion
	}
}
