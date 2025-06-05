//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Confluent.Kafka;

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro.Tests
{
	[TestClass]
	public class KafkaAvroAsyncSerializerTests : TestBase
	{
		#region Constructor Tests

		[TestMethod]
		public void Constructor_ValidParameters_CreatesSerializerSuccessfully()
		{
			// Arrange & Act
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
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
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
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
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
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
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				null,
				mockCredential.Object,
				ValidSchemaGroup);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void Constructor_NullCredential_ThrowsArgumentNullException()
		{
			// Arrange & Act
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				null,
				ValidSchemaGroup);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void Constructor_NullSchemaGroup_ThrowsArgumentNullException()
		{
			// Arrange & Act
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				null);
		}

		[TestMethod]
		public void Constructor_EmptySchemaRegistryUrl_CreatesSerializerSuccessfully()
		{
			// Arrange & Act
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				"",
				mockCredential.Object,
				ValidSchemaGroup);

			// Assert
			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		public void Constructor_EmptySchemaGroup_CreatesSerializerSuccessfully()
		{
			// Arrange & Act
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				"");

			// Assert
			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		public void Constructor_InvalidUrl_CreatesSerializerSuccessfully()
		{
			// Arrange & Act
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				"invalid-url",
				mockCredential.Object,
				ValidSchemaGroup);

			// Assert
			Assert.IsNotNull(serializer);
		}

		#endregion

		#region SerializeAsync Method Tests

		[TestMethod]
		public async Task SerializeAsync_NullObject_ReturnsNull()
		{
			// Arrange
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Act
			var result = await serializer.SerializeAsync(null, context);

			// Assert
			Assert.IsNull(result);
		}

		[TestMethod]
		public async Task SerializeAsync_ValidObject_HandlesSchemaRegistryUnavailable()
		{
			// Arrange
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass { Name = "John", Age = 30 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Act & Assert
			try
			{
				var result = await serializer.SerializeAsync(testObject, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is HttpRequestException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is TaskCanceledException);
			}
		}

		[TestMethod]
		public async Task SerializeAsync_WithAutoRegisterTrue_HandlesSchemaRegistryUnavailable()
		{
			// Arrange
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: true);
			var testObject = new TestClass { Name = "Jane", Age = 25 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Act & Assert
			try
			{
				var result = await serializer.SerializeAsync(testObject, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is HttpRequestException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is TaskCanceledException);
			}
		}

		[TestMethod]
		public async Task SerializeAsync_WithAutoRegisterFalse_HandlesSchemaRegistryUnavailable()
		{
			// Arrange
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: false);
			var testObject = new TestClass { Name = "Bob", Age = 35 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Act & Assert
			try
			{
				var result = await serializer.SerializeAsync(testObject, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is HttpRequestException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is TaskCanceledException);
			}
		}

		[TestMethod]
		public async Task SerializeAsync_WithKeyComponent_HandlesSchemaRegistryUnavailable()
		{
			// Arrange
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass { Name = "Alice", Age = 28 };
			var context = new SerializationContext(MessageComponentType.Key, "test-topic");

			// Act & Assert
			try
			{
				var result = await serializer.SerializeAsync(testObject, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is HttpRequestException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is TaskCanceledException);
			}
		}

		[TestMethod]
		public async Task SerializeAsync_WithDifferentTopic_HandlesSchemaRegistryUnavailable()
		{
			// Arrange
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass { Name = "Charlie", Age = 40 };
			var context = new SerializationContext(MessageComponentType.Value, "different-topic");

			// Act & Assert
			try
			{
				var result = await serializer.SerializeAsync(testObject, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is HttpRequestException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is TaskCanceledException);
			}
		}

		[TestMethod]
		public async Task SerializeAsync_EmptyObject_HandlesSchemaRegistryUnavailable()
		{
			// Arrange
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass(); // Empty object
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Act & Assert
			try
			{
				var result = await serializer.SerializeAsync(testObject, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is HttpRequestException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is TaskCanceledException);
			}
		}

		[TestMethod]
		public async Task SerializeAsync_ContextHeadersAvailable_HandlesSchemaRegistryUnavailable()
		{
			// Arrange
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass { Name = "David", Age = 32 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Act & Assert
			try
			{
				var result = await serializer.SerializeAsync(testObject, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is HttpRequestException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is TaskCanceledException);
			}
		}

		#endregion
	}
}
