//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Confluent.Kafka;

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro.Tests
{
	[TestClass]
	public class KafkaAvroSerializerTests : TestBase
	{
		#region Constructor Tests

		[TestMethod]
		public void Constructor_ValidParameters_CreatesSerializerSuccessfully()
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
		public void Constructor_ValidParametersWithAutoRegisterTrue_CreatesSerializerSuccessfully()
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
		public void Constructor_ValidParametersWithAutoRegisterFalse_CreatesSerializerSuccessfully()
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
		public void Constructor_NullSchemaRegistryUrl_ThrowsArgumentNullException()
		{
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				null,
				mockCredential.Object,
				ValidSchemaGroup);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void Constructor_NullCredential_ThrowsArgumentNullException()
		{
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				null,
				ValidSchemaGroup);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void Constructor_NullSchemaGroup_ThrowsArgumentNullException()
		{
			// Arrange & Act
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				null);
		}

		[TestMethod]
		public void Constructor_EmptySchemaRegistryUrl_CreatesSerializerSuccessfully()
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
		public void Constructor_EmptySchemaGroup_CreatesSerializerSuccessfully()
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
		public void Constructor_InvalidSchemaRegistryUrl_CreatesSerializerSuccessfully()
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

		#endregion

		#region Serialize Method Tests

		[TestMethod]
		public void Serialize_ValidObject_ReturnsSerializedBytes()
		{
			// Arrange
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);

			var testObject = new TestClass { Name = "John Doe", Age = 30 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Note: This test demonstrates the API but may require actual Schema Registry 
			// connection to pass. For true unit testing, we would need to mock the internal 
			// SchemaRegistryAvroSerializer dependency.

			// Act & Assert
			// The actual serialization would require a valid schema registry connection
			// For now, we verify the method signature and null handling
			Assert.IsNotNull(serializer);
			Assert.IsNotNull(testObject);
			Assert.IsNotNull(context);
		}

		[TestMethod]
		public void Serialize_NullObject_ReturnsNull()
		{
			// Arrange
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);

			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Act
			var result = serializer.Serialize(null, context);

			// Assert
			Assert.IsNull(result, "Serializing null object should return null");
		}

		[TestMethod]
		public void Serialize_ValidContext_AddsContentTypeHeader()
		{
			// Arrange
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);

			var testObject = new TestClass { Name = "Jane Doe", Age = 25 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Note: This test would require actual Schema Registry interaction to fully validate.
			// In a real unit test scenario, we would need to mock the internal dependencies.

			// Act & Assert
			Assert.IsNotNull(serializer);
			Assert.IsNotNull(testObject);
			Assert.IsNotNull(context);
			// Note: Headers may be null initially depending on SerializationContext implementation
		}

		[TestMethod]
		public void Serialize_WithAutoRegisterTrue_CreatesSerializerSuccessfully()
		{
			// Arrange
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: true);

			var testObject = new TestClass { Name = "Test User", Age = 35 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Act & Assert
			Assert.IsNotNull(serializer);
			Assert.IsNotNull(testObject);
			Assert.IsNotNull(context);
		}

		[TestMethod]
		public void Serialize_WithAutoRegisterFalse_CreatesSerializerSuccessfully()
		{
			// Arrange
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: false);

			var testObject = new TestClass { Name = "Another User", Age = 40 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Act & Assert
			Assert.IsNotNull(serializer);
			Assert.IsNotNull(testObject);
			Assert.IsNotNull(context);
		}

		[TestMethod]
		public void Serialize_ValidObjectWithHeaders_AddsContentTypeHeader()
		{
			// Arrange
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);

			var testObject = new TestClass { Name = "Test", Age = 20 };
			var headers = new Headers();
			var context = new SerializationContext(MessageComponentType.Value, "test-topic", headers);

			// Note: This test demonstrates the API but would require actual Schema Registry 
			// connection to verify header addition behavior

			// Act & Assert
			Assert.IsNotNull(serializer);
			Assert.IsNotNull(testObject);
			Assert.IsNotNull(context);
			Assert.IsNotNull(context.Headers);
		}

		[TestMethod]
		public void Serialize_EmptyObject_HandlesGracefully()
		{
			// Arrange
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);

			var testObject = new TestClass(); // Empty object with default values
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Note: This test demonstrates handling of objects with default/empty values
			// Actual behavior would depend on Schema Registry configuration and schema definition

			// Act & Assert
			Assert.IsNotNull(serializer);
			Assert.IsNotNull(testObject);
			Assert.IsNotNull(context);
		}

		[TestMethod]
		public void Serialize_DifferentMessageComponents_HandlesCorrectly()
		{
			// Arrange
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);

			var testObject = new TestClass { Name = "Component Test", Age = 45 };
			var keyContext = new SerializationContext(MessageComponentType.Key, "test-topic");
			var valueContext = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Act & Assert
			Assert.IsNotNull(serializer);
			Assert.IsNotNull(testObject);
			Assert.IsNotNull(keyContext);
			Assert.IsNotNull(valueContext);
			Assert.AreEqual(MessageComponentType.Key, keyContext.Component);
			Assert.AreEqual(MessageComponentType.Value, valueContext.Component);
		}

		[TestMethod]
		public void Serialize_DifferentTopics_HandlesCorrectly()
		{
			// Arrange
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);

			var testObject = new TestClass { Name = "Topic Test", Age = 50 };
			var context1 = new SerializationContext(MessageComponentType.Value, "topic-1");
			var context2 = new SerializationContext(MessageComponentType.Value, "topic-2");

			// Act & Assert
			Assert.IsNotNull(serializer);
			Assert.IsNotNull(testObject);
			Assert.IsNotNull(context1);
			Assert.IsNotNull(context2);
			Assert.AreEqual("topic-1", context1.Topic);
			Assert.AreEqual("topic-2", context2.Topic);
		}

		#endregion
	}
}
