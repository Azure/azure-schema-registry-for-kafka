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
	public sealed class KafkaJsonSerializerTests : TestBase
	{
		#region Constructor Tests

		[TestMethod]
		public void Constructor_ValidParameters_CreatesSerializerSuccessfully()
		{
			var serializer = new KafkaJsonSerializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);

			Assert.IsNotNull(serializer, "Serializer should be created successfully with valid parameters");
		}

		[TestMethod]
		public void Constructor_ValidParametersWithAutoRegisterTrue_CreatesSerializerSuccessfully()
		{
			var serializer = new KafkaJsonSerializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: true);

			Assert.IsNotNull(serializer, "Serializer should be created successfully with autoRegisterSchemas=true");
		}

		[TestMethod]
		public void Constructor_ValidParametersWithAutoRegisterFalse_CreatesSerializerSuccessfully()
		{
			var serializer = new KafkaJsonSerializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup,
				autoRegisterSchemas: false);

			Assert.IsNotNull(serializer, "Serializer should be created successfully with autoRegisterSchemas=false");
		}
		[TestMethod]
		[ExpectedException(typeof(UriFormatException))]
		public void Constructor_NullSchemaRegistryUrl_ThrowsUriFormatException()
		{
			var serializer = new KafkaJsonSerializer<SimpleTestClass>(
				null!,
				mockCredential.Object,
				ValidSchemaGroup);
		}

		[TestMethod]
		[ExpectedException(typeof(ArgumentNullException))]
		public void Constructor_NullCredential_ThrowsArgumentNullException()
		{
			var serializer = new KafkaJsonSerializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				null!,
				ValidSchemaGroup);
		}
		[TestMethod]
		public void Constructor_NullSchemaGroup_ThrowsException()
		{
			try
			{
				var serializer = new KafkaJsonSerializer<SimpleTestClass>(
					ValidSchemaRegistryUrl,
					mockCredential.Object,
					null!);

				// If we get here, the constructor accepted null schemaGroup without throwing
				// Let's attempt to use the serializer to see if it fails during operation
				var testObject = new SimpleTestClass
				{
					StringProperty = "Test",
					IntProperty = 42,
					BoolProperty = true,
					DoubleProperty = 3.14
				};

				var context = new SerializationContext(MessageComponentType.Value, "test-topic");

				try
				{
					var result = serializer.Serialize(testObject, context);
					Assert.Fail("Expected exception during serialization with null schema group");
				}
				catch (Exception serializationEx)
				{
					// We expect the serialization to fail with a NullReferenceException or similar
					Assert.IsTrue(serializationEx is NullReferenceException ||
								 serializationEx is ArgumentNullException ||
								 serializationEx is SerializationException,
						$"Expected NullReferenceException, ArgumentNullException, or SerializationException but got {serializationEx.GetType().Name}");
				}
			}
			catch (Exception ex)
			{
				// Constructor threw an exception which is also acceptable
				Assert.IsTrue(ex is ArgumentNullException || ex is NullReferenceException,
					$"Expected ArgumentNullException or NullReferenceException but got {ex.GetType().Name}");
			}
		}

		[TestMethod]
		[ExpectedException(typeof(UriFormatException))]
		public void Constructor_EmptySchemaRegistryUrl_ThrowsUriFormatException()
		{
			var serializer = new KafkaJsonSerializer<SimpleTestClass>(
				string.Empty,
				mockCredential.Object,
				ValidSchemaGroup);
		}

		[TestMethod]
		public void Constructor_EmptySchemaGroup_CreatesSerializerSuccessfully()
		{
			var serializer = new KafkaJsonSerializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				string.Empty);

			Assert.IsNotNull(serializer, "Serializer should be created with empty schema group");
		}

		[TestMethod]
		public void Constructor_InvalidSchemaRegistryUrl_CreatesSerializerSuccessfully()
		{
			var serializer = new KafkaJsonSerializer<SimpleTestClass>(
				"invalid-url",
				mockCredential.Object,
				ValidSchemaGroup);

			Assert.IsNotNull(serializer, "Serializer should be created with invalid schema registry URL");
		}

		#endregion

		#region Serialize Method Tests
		[TestMethod]
		public void Serialize_NullObject_ReturnsNull()
		{
			var serializer = new KafkaJsonSerializer<SimpleTestClass>(
				ValidSchemaRegistryUrl,
				mockCredential.Object,
				ValidSchemaGroup);

			var context = new SerializationContext(MessageComponentType.Value, "test-topic");

			var result = serializer.Serialize(null!, context);

			Assert.IsNull(result, "Serializing null object should return null");
		}

		#endregion
	}
}
