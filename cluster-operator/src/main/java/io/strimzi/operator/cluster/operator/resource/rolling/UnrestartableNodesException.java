/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.operator.cluster.operator.resource.rolling;

class UnrestartableNodesException extends RuntimeException {

    /**
     * This exception indicates that a node cannot be attempted to restart or not ready after a restart
     * @param message the detail message. The detail message is saved for later retrieval by the getMessage() method
     */
    public UnrestartableNodesException(String message) {
        super(message);
    }

    UnrestartableNodesException(String message, Throwable throwable) {
        super(message, throwable);
    }
}
