/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

class RetriableException extends RuntimeException {

    /**
     * This exception indicates that KafkaRoller will re-attempt this node to either bring it to a healthy state, reconfigure or restart
     * @param message the detail message. The detail message is saved for later retrieval by the getMessage() method
     */
    public RetriableException(String message) {
        super(message);
    }

    RetriableException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
