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
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);

			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		public void Constructor_ValidParametersWithAutoRegisterTrue_CreatesSerializerSuccessfully()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: true);

			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		public void Constructor_ValidParametersWithAutoRegisterFalse_CreatesSerializerSuccessfully()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: false);

			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void Constructor_NullSchemaRegistryUrl_ThrowsArgumentNullException()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				null,
				mockCredential.Object,
				ValidSchemaGroup);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void Constructor_NullCredential_ThrowsArgumentNullException()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				null,
				ValidSchemaGroup);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void Constructor_NullSchemaGroup_ThrowsArgumentNullException()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				null);
		}

		[TestMethod]
		public void Constructor_EmptySchemaRegistryUrl_CreatesSerializerSuccessfully()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				"",
				mockCredential.Object,
				ValidSchemaGroup);

			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		public void Constructor_EmptySchemaGroup_CreatesSerializerSuccessfully()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				"");

			Assert.IsNotNull(serializer);
		}

		[TestMethod]
		public void Constructor_InvalidUrl_CreatesSerializerSuccessfully()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				"invalid-url",
				mockCredential.Object,
				ValidSchemaGroup);

			Assert.IsNotNull(serializer);
		}

		#endregion

		#region SerializeAsync Method Tests

		[TestMethod]
		public async Task SerializeAsync_NullObject_ReturnsNull()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			var result = await serializer.SerializeAsync(null, context);

			Assert.IsNull(result);
		}

		[TestMethod]
		public async Task SerializeAsync_ValidObject_HandlesSchemaRegistryUnavailable()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass { Name = "John", Age = 30 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

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
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: true);
			var testObject = new TestClass { Name = "Jane", Age = 25 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

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
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: false);
			var testObject = new TestClass { Name = "Bob", Age = 35 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

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
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass { Name = "Alice", Age = 28 };
			var context = new SerializationContext(MessageComponentType.Key, "test-topic");

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
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass { Name = "Charlie", Age = 40 };
			var context = new SerializationContext(MessageComponentType.Value, "different-topic");

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
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass(); // Empty object
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

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
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass { Name = "David", Age = 32 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

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
