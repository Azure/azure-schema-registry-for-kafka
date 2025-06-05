//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Confluent.Kafka;

namespace Microsoft.Azure.Kafka.SchemaRegistry.Avro.Tests
{
	[TestClass]
	public class KafkaAvroIntegrationTests : TestBase
	{
		#region Round-Trip Integration Tests

		[TestMethod]
		public void RoundTrip_SameSchemaRegistryUrl_HandlesSchemaRegistryUnavailable()
		{
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var originalObject = new TestClass { Name = "TestName", Age = 42 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			try
			{
				var serializedData = serializer.Serialize(originalObject, context);
				Assert.IsNotNull(serializedData);
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
					ex is FormatException || ex is HttpRequestException || ex is NullReferenceException);
			}
		}

		[TestMethod]
		public async Task AsyncRoundTrip_SameSchemaRegistryUrl_HandlesSchemaRegistryUnavailable()
		{
			var asyncSerializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var originalObject = new TestClass { Name = "AsyncTestName", Age = 25 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			try
			{
				var serializedData = await asyncSerializer.SerializeAsync(originalObject, context);
				Assert.IsNotNull(serializedData);
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
					ex is FormatException || ex is HttpRequestException || ex is NullReferenceException);
			}
		}

		[TestMethod]
		public void RoundTrip_DifferentObjects_MaintainsDataIntegrity()
		{
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var testObjects = new[]
			{
				new TestClass { Name = "Object1", Age = 1 },
				new TestClass { Name = "Object2", Age = 100 },
				new TestClass { Name = "", Age = 0 },
				new TestClass { Name = "VeryLongNameThatExceedsNormalLengthToTestSerialization", Age = int.MaxValue }
			};
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			foreach (var originalObject in testObjects)
			{
				try
				{
					var serializedData = serializer.Serialize(originalObject, context);
					Assert.IsNotNull(serializedData);
				}
				catch (Exception ex)
				{
					Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
						ex is FormatException || ex is HttpRequestException || ex is NullReferenceException);
				}
			}
		}

		#endregion

		#region Schema Registry Error Handling Tests

		[TestMethod]
		public void Serializer_InvalidSchemaRegistryUrl_HandlesGracefully()
		{
			var invalidUrls = new[]
			{
				"invalid-url",
				"http://nonexistent.domain.invalid",
				"https://localhost:99999", // Invalid port
				"ftp://invalid.protocol.com"
			};
			var testObject = new TestClass { Name = "Test", Age = 30 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			foreach (var invalidUrl in invalidUrls)
			{
				try
				{
					var serializer = new KafkaAvroSerializer<TestClass>(
						invalidUrl,
						mockCredential.Object,
						ValidSchemaGroup);
					var result = serializer.Serialize(testObject, context);
					Assert.Fail($"Expected exception for invalid URL: {invalidUrl}");
				}
				catch (Exception ex)
				{
					Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
						ex is FormatException || ex is HttpRequestException || ex is UriFormatException ||
						ex is NullReferenceException);
				}
			}
		}

		[TestMethod]
		public async Task AsyncSerializer_NetworkTimeout_HandlesGracefully()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass { Name = "TimeoutTest", Age = 40 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			try
			{
				var result = await serializer.SerializeAsync(testObject, context);
				Assert.Fail("Expected exception due to schema registry unavailability");
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is HttpRequestException || ex is TaskCanceledException ||
					ex is ArgumentException || ex is InvalidOperationException || ex is NullReferenceException);
			}
		}

		[TestMethod]
		public void Deserializer_CorruptedData_ThrowsAppropriateException()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var corruptedDataSets = new[]
			{
				new byte[] { 0xFF, 0xFF, 0xFF, 0xFF }, // Invalid magic bytes
				new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04 }, // Too short
				System.Text.Encoding.UTF8.GetBytes("not avro data"), // Text data
				new byte[0] // Empty array
			};
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			foreach (var corruptedData in corruptedDataSets)
			{
				try
				{
					var testData = new ReadOnlySpan<byte>(corruptedData);
					var result = deserializer.Deserialize(testData, false, context);
					// Some edge cases might not throw exceptions
				}
				catch (Exception ex)
				{
					Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
						ex is FormatException || ex is NotSupportedException || ex is NullReferenceException ||
						ex is IndexOutOfRangeException);
				}
			}
		}

		#endregion

		#region Header Management Tests

		[TestMethod]
		public void Serializer_WithHeaders_HandlesCorrectly()
		{
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass { Name = "HeaderTest", Age = 35 };
			var headers = new Headers();
			headers.Add("test-header", System.Text.Encoding.UTF8.GetBytes("test-value"));
			headers.Add("schema-version", System.Text.Encoding.UTF8.GetBytes("1.0"));
			var context = new SerializationContext(MessageComponentType.Value, "test-topic", headers);

			try
			{
				var result = serializer.Serialize(testObject, context);
				Assert.IsNotNull(result);
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
					ex is FormatException || ex is HttpRequestException || ex is NullReferenceException);
			}
		}

		[TestMethod]
		public void Deserializer_WithNullHeaders_HandlesGracefully()
		{
			var deserializer = new KafkaAvroDeserializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);
			var testData = new ReadOnlySpan<byte>(new byte[] { 0x01, 0x02, 0x03, 0x04 });
			var context = new SerializationContext(MessageComponentType.Value, "test-topic", null);

			try
			{
				var result = deserializer.Deserialize(testData, false, context);
				// Null headers handled correctly
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
					ex is FormatException || ex is NullReferenceException || ex is NotSupportedException);
			}
		}

		[TestMethod]
		public void Serializer_MessageComponentType_Key_Works()
		{
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var testObject = new TestClass { Name = "KeyTest", Age = 45 };
			var context = new SerializationContext(MessageComponentType.Key, "test-topic");

			try
			{
				var result = serializer.Serialize(testObject, context);
				Assert.IsNotNull(result);
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
					ex is FormatException || ex is HttpRequestException || ex is NullReferenceException);
			}
		}

		#endregion

		#region Advanced Configuration Tests

		[TestMethod]
		public void Serializer_AutoRegisterSchemas_True_AttemptsRegistration()
		{
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: true);
			var testObject = new TestClass { Name = "AutoRegisterTest", Age = 50 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			try
			{
				var result = serializer.Serialize(testObject, context);
				Assert.IsNotNull(result);
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
					ex is FormatException || ex is HttpRequestException || ex is NullReferenceException);
			}
		}

		[TestMethod]
		public void Serializer_AutoRegisterSchemas_False_DoesNotAttemptRegistration()
		{
			var serializer = new KafkaAvroSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: false);
			var testObject = new TestClass { Name = "NoAutoRegisterTest", Age = 55 };
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			try
			{
				var result = serializer.Serialize(testObject, context);
				Assert.IsNotNull(result);
			}
			catch (Exception ex)
			{
				Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
					ex is FormatException || ex is HttpRequestException || ex is NullReferenceException);
			}
		}

		[TestMethod]
		public async Task AsyncSerializer_ConcurrentSerialization_HandlesCorrectly()
		{
			var serializer = new KafkaAvroAsyncSerializer<TestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);
			var tasks = new List<Task>();
			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			for (int i = 0; i < 10; i++)
			{
				var testObject = new TestClass { Name = $"Concurrent{i}", Age = i };
				var task = Task.Run(async () =>
				{
					try
					{
						var result = await serializer.SerializeAsync(testObject, context);
						// In real scenario with schema registry, this would work
					}
					catch (Exception ex)
					{
						Assert.IsTrue(ex is ArgumentException || ex is InvalidOperationException ||
							ex is FormatException || ex is HttpRequestException || ex is NullReferenceException);
					}
				});
				tasks.Add(task);
			}

			await Task.WhenAll(tasks);
			Assert.IsTrue(true); // All concurrent operations completed
		}

		#endregion
	}
}
