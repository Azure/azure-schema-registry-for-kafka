//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System;
using System.Net.Http;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Confluent.Kafka;

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro.Tests
{
	[TestClass]
	public class KafkaAvroDeserializerTests : TestBase
	{
		#region Constructor Tests

		[TestMethod]
		public void Constructor_ValidParameters_CreatesDeserializerSuccessfully()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);

			Assert.IsNotNull(deserializer);
		}
		[TestMethod]
		[ExpectedException(typeof(UriFormatException))]
		public void Constructor_NullSchemaRegistryUrl_ThrowsUriFormatException()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				null,
				mockCredential.Object);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void Constructor_NullCredential_ThrowsArgumentNullException()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				null);
		}
		[TestMethod]
		[ExpectedException(typeof(UriFormatException))]
		public void Constructor_EmptySchemaRegistryUrl_ThrowsUriFormatException()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				"",
				mockCredential.Object);
		}

		[TestMethod]
		public void Constructor_InvalidUrl_CreatesDeserializerSuccessfully()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				"invalid-url",
				mockCredential.Object);

			Assert.IsNotNull(deserializer);
		}

		#endregion

		#region Deserialize Method Tests

		[TestMethod]
		public void Deserialize_EmptyData_ReturnsNull()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var emptyData = ReadOnlySpan<byte>.Empty;
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			var result = deserializer.Deserialize(emptyData, false, context);

			Assert.IsNull(result);
		}

		[TestMethod]
		public void Deserialize_IsNullTrue_HandlesExpectedExceptions()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var someData = new ReadOnlySpan<byte>(new byte[] { 1, 2, 3, 4 });
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			try
			{
				var result = deserializer.Deserialize(someData, true, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is NullReferenceException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is FormatException || ex is HttpRequestException);
			}
		}

		[TestMethod]
		public void Deserialize_ValidDataWithoutHeaders_HandlesExpectedExceptions()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var testData = new ReadOnlySpan<byte>(new byte[] { 1, 2, 3, 4, 5 });
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			try
			{
				var result = deserializer.Deserialize(testData, false, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is NullReferenceException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is FormatException || ex is HttpRequestException);
			}
		}

		[TestMethod]
		public void Deserialize_ValidDataWithHeaders_HandlesExpectedExceptions()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var testData = new ReadOnlySpan<byte>(new byte[] { 1, 2, 3, 4, 5 });
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			// Add content-type header if available
			if (context.Headers != null)
			{
				context.Headers.Add("content-type", Encoding.UTF8.GetBytes("application/vnd.schemaregistry.v1+avro"));
			}

			try
			{
				var result = deserializer.Deserialize(testData, false, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is NullReferenceException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is FormatException || ex is HttpRequestException);
			}
		}

		[TestMethod]
		public void Deserialize_WithKeyComponent_HandlesExpectedExceptions()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var testData = new ReadOnlySpan<byte>(new byte[] { 1, 2, 3, 4, 5 });
			var context = new SerializationContext(MessageComponentType.Key, "test-topic");

			try
			{
				var result = deserializer.Deserialize(testData, false, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is NullReferenceException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is FormatException || ex is HttpRequestException);
			}
		}

		[TestMethod]
		public void Deserialize_WithDifferentTopic_HandlesExpectedExceptions()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var testData = new ReadOnlySpan<byte>(new byte[] { 1, 2, 3, 4, 5 });
			var context = new SerializationContext(MessageComponentType.Value, "different-topic");

			try
			{
				var result = deserializer.Deserialize(testData, false, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is NullReferenceException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is FormatException || ex is HttpRequestException);
			}
		}

		[TestMethod]
		public void Deserialize_SingleByteData_HandlesExpectedExceptions()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var testData = new ReadOnlySpan<byte>(new byte[] { 42 });
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			try
			{
				var result = deserializer.Deserialize(testData, false, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is NullReferenceException || ex is ArgumentException ||
					ex is InvalidOperationException || ex is FormatException || ex is HttpRequestException);
			}
		}

		[TestMethod]
		public void Deserialize_LargeData_HandlesExpectedExceptions()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var largeData = new byte[1024];
			for (int i = 0; i < largeData.Length; i++)
			{
				largeData[i] = (byte)(i % 256);
			}
			var testData = new ReadOnlySpan<byte>(largeData);
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			try
			{
				var result = deserializer.Deserialize(testData, false, context);
				Assert.Fail("Expected exception but none was thrown");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
					ex is FormatException || ex is HttpRequestException || ex is NotSupportedException ||
					ex is NullReferenceException);
			}
		}

		#endregion
	}
}
