// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import org.apache.kafka.common.errors.SerializationException;

/**
 * Custom error class for exceptions thrown in Json serialization/deserialization steps
 */
public class JsonSerializerException extends SerializationException {
    /**
     * Constructor with message only
     * @param errorMessage Brief explination of exception source.
     */
    public JsonSerializerException(String errorMessage) {
        super(errorMessage);
    }

    /**
     * Constructor with message and throwable error
     * @param errorMessage Brief explination of exception source.
     * @param err Throwable error object
     */
    public JsonSerializerException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
