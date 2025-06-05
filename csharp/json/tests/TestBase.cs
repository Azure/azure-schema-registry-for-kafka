//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Azure.Core;

namespace Microsoft.Azure.Kafka.SchemaRegistry.Json.Tests
{
	/// <summary>
	/// Base class for all Kafka JSON serializer tests, providing common setup and test infrastructure.
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

		/// <summary>
		/// Simple test class for JSON serialization/deserialization tests.
		/// </summary>
		protected class SimpleTestClass
		{
			public string StringProperty { get; set; }
			public int IntProperty { get; set; }
			public bool BoolProperty { get; set; }
			public double DoubleProperty { get; set; }
		}

		/// <summary>
		/// Test class with nested objects for testing complex JSON serialization.
		/// </summary>
		protected class ComplexTestClass
		{
			public string Name { get; set; }
			public int Id { get; set; }
			public SimpleTestClass NestedObject { get; set; }
		}

		/// <summary>
		/// Test class with collection properties for testing array serialization.
		/// </summary>
		protected class CollectionTestClass
		{
			public string Name { get; set; }
			public List<string> StringList { get; set; }
			public List<SimpleTestClass> ObjectList { get; set; }
			public Dictionary<string, string> StringDictionary { get; set; }
		}

		/// <summary>
		/// Test class with nullable properties for testing nullable handling.
		/// </summary>
		protected class NullableTestClass
		{
			public string StringProperty { get; set; }
			public int? NullableInt { get; set; }
			public DateTime? NullableDateTime { get; set; }
			public SimpleTestClass NestedObject { get; set; }
		}

		#endregion
	}
}
