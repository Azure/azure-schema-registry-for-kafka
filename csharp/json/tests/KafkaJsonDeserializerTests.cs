//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System;
using System.Text;
using Confluent.Kafka;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace Microsoft.Azure.Kafka.SchemaRegistry.Json.Tests
{
	[TestClass]
	public sealed class KafkaJsonDeserializerTests : TestBase
	{
		#region Constructor Tests

		[TestMethod]
		public void Constructor_ValidParameters_CreatesDeserializerSuccessfully()
		{
			var deserializer = new KafkaJsonDeserializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);

			Assert.IsNotNull(deserializer, "Deserializer should be created successfully with valid parameters");
		}
		[TestMethod]
		[ExpectedException(typeof(UriFormatException))]
		public void Constructor_NullSchemaRegistryUrl_ThrowsUriFormatException()
		{
			var deserializer = new KafkaJsonDeserializer<SimpleTestClass>(
				null!,
				mockCredential.Object);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void Constructor_NullCredential_ThrowsArgumentNullException()
		{
			var deserializer = new KafkaJsonDeserializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				null!);
		}

		[TestMethod]
		[ExpectedException(typeof(UriFormatException))]
		public void Constructor_EmptySchemaRegistryUrl_ThrowsUriFormatException()
		{
			var deserializer = new KafkaJsonDeserializer<SimpleTestClass>(
				string.Empty,
				mockCredential.Object);
		}

		[TestMethod]
		public void Constructor_InvalidSchemaRegistryUrl_CreatesDeserializerSuccessfully()
		{
			var deserializer = new KafkaJsonDeserializer<SimpleTestClass>(
				"invalid-url",
				mockCredential.Object);

			Assert.IsNotNull(deserializer, "Deserializer should be created with invalid schema registry URL");
		}

		#endregion

		#region Deserialize Method Tests

		[TestMethod]
		public void Deserialize_NullData_ReturnsDefaultValue()
		{
			var deserializer = new KafkaJsonDeserializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);

			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			var result = deserializer.Deserialize(ReadOnlySpan<byte>.Empty, true, context);

			Assert.IsNull(result, "Deserializing null data should return default value (null for reference types)");
		}

		[TestMethod]
		public void Deserialize_EmptyData_ReturnsDefaultValue()
		{
			var deserializer = new KafkaJsonDeserializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);

			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			var result = deserializer.Deserialize(ReadOnlySpan<byte>.Empty, false, context);

			Assert.IsNull(result, "Deserializing empty data should return default value (null for reference types)");
		}

		[TestMethod]
		public void Deserialize_MissingSchemaIdHeader_ReturnsDefaultValue()
		{
			var deserializer = new KafkaJsonDeserializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);

			var testData = Encoding.UTF8.GetBytes("{\"StringProperty\":\"Test\",\"IntProperty\":42,\"BoolProperty\":true,\"DoubleProperty\":3.14}");
			var headers = new Headers();
			var context = new SerializationContext(MessageComponentType.Value, "test-topic", headers);

			var result = deserializer.Deserialize(testData, false, context);

			Assert.IsNull(result, "Deserializing without schemaId header should return default value");
		}

		[TestMethod]
		public void Deserialize_EmptySchemaIdHeader_ReturnsDefaultValue()
		{
			var deserializer = new KafkaJsonDeserializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object);

			var testData = Encoding.UTF8.GetBytes("{\"StringProperty\":\"Test\",\"IntProperty\":42,\"BoolProperty\":true,\"DoubleProperty\":3.14}");
			var headers = new Headers();
			headers.Add("schemaId", Array.Empty<byte>());
			var context = new SerializationContext(MessageComponentType.Value, "test-topic", headers);

			var result = deserializer.Deserialize(testData, false, context);

			Assert.IsNull(result, "Deserializing with empty schemaId header should return default value");
		}

		#endregion
	}
}
