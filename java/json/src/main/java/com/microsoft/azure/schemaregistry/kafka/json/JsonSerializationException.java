// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.microsoft.azure.schemaregistry.kafka.json;

import org.apache.kafka.common.errors.SerializationException;

/**
 * Custom error class for exceptions thrown in Json serialization/deserialization steps
 */
public class JsonSerializationException extends SerializationException {
    /**
     * Constructor with message only
     * @param errorMessage Brief explination of exception source.
     */
    public JsonSerializationException(String errorMessage) {
        super(errorMessage);
    }

    /**
     * Constructor with message and throwable error
     * @param errorMessage Brief explination of exception source.
     * @param err Throwable error object
     */
    public JsonSerializationException(String errorMessage, Throwable err) {
        super(errorMessage, err);
    }
}
