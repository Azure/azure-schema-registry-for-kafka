//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;
using Azure.Core;

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro.Tests
{
	[TestClass]
	public class KafkaAvroSerializerTests
	{
		private Mock<TokenCredential> mockCredential;
		private const string ValidSchemaRegistryUrl = "https://test-schema-registry.servicebus.windows.net";
		private const string ValidSchemaGroup = "test-group";

		[TestInitialize]
		public void Setup()
		{
			mockCredential = new Mock<TokenCredential>();
		}

		#region KafkaAvroSerializer Constructor Tests

		[TestMethod]
		public void KafkaAvroSerializer_Constructor_ValidParameters_CreatesSerializerSuccessfully()
		{
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);

			// Assert
			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		public void KafkaAvroSerializer_Constructor_ValidParametersWithAutoRegisterTrue_CreatesSerializerSuccessfully()
		{
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: true);

			// Assert
			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		public void KafkaAvroSerializer_Constructor_ValidParametersWithAutoRegisterFalse_CreatesSerializerSuccessfully()
		{
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: false);

			// Assert
			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void KafkaAvroSerializer_Constructor_NullSchemaRegistryUrl_ThrowsArgumentNullException()
		{
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				null,
				mockCredential.Object,
				ValidSchemaGroup);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void KafkaAvroSerializer_Constructor_NullCredential_ThrowsArgumentNullException()
		{
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				null,
				ValidSchemaGroup);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void KafkaAvroSerializer_Constructor_NullSchemaGroup_ThrowsArgumentNullException()
		{
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				null);
		}
		[TestMethod]
		public void KafkaAvroSerializer_Constructor_EmptySchemaRegistryUrl_CreatesSerializerSuccessfully()
		{
			// Note: Empty URL doesn't throw during construction - validation happens later
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				string.Empty,
				mockCredential.Object,
				ValidSchemaGroup);

			// Assert
			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		public void KafkaAvroSerializer_Constructor_EmptySchemaGroup_CreatesSerializerSuccessfully()
		{
			// Note: Empty schema group appears to be allowed by the constructor
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				string.Empty);

			// Assert
			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		public void KafkaAvroSerializer_Constructor_InvalidSchemaRegistryUrl_CreatesSerializerSuccessfully()
		{
			// Note: Invalid URL format doesn't throw during construction - validation happens later
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				"invalid-url",
				mockCredential.Object,
				ValidSchemaGroup);

			// Assert
			Assert.IsNotNull(serializer);
		}

		#endregion        [TestMethod]
		public void BasicTest()
		{
			Assert.AreEqual(true, true, "Basic test to ensure MSTest is set up correctly.");
		}

		#region Test Helper Classes

		private class TestClass
		{
			public string Name { get; set; }
			public int Age { get; set; }
		}

		#endregion
	}
}
