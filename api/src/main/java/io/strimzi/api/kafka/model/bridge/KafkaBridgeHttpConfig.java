/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.bridge;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.kafka.model.common.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.crdgenerator.annotations.Description;
import io.strimzi.crdgenerator.annotations.DescriptionFile;
import io.strimzi.crdgenerator.annotations.Minimum;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * A representation of the HTTP configuration.
 */
@DescriptionFile
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"port", "sslEnable", "sslPort", "certificateAndKey", "config", "cors"})
@EqualsAndHashCode
@ToString
public class KafkaBridgeHttpConfig implements UnknownPropertyPreserving {
    public static final int HTTP_DEFAULT_PORT = 8080;
    public static final int HTTPS_DEFAULT_PORT = 8443;
    public static final int ADMIN_DEFAULT_PORT = 8081;
    public static final String HTTP_DEFAULT_HOST = "0.0.0.0";
    private int port = HTTP_DEFAULT_PORT;
    private int adminPort = ADMIN_DEFAULT_PORT;
    private boolean sslEnable;
    private CertAndKeySecretSource certificateAndKey = null;
    private Map<String, Object> config = new HashMap<>(0);
    private KafkaBridgeHttpCors cors;
    private Map<String, Object> additionalProperties;

    public KafkaBridgeHttpConfig() {
    }

    public KafkaBridgeHttpConfig(int port) {
        this.port = port;
    }

    @Description("The port which is the server listening on.")
    @JsonProperty(defaultValue = "8080")
    @Minimum(1023)
    public int getPort() {
        if (sslEnable && port == HTTP_DEFAULT_PORT) {
            return HTTPS_DEFAULT_PORT;
        }
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Description("SSL/TLS enablement for the HTTP Bridge server.")
    @JsonProperty(defaultValue = "false")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    public boolean isSslEnable() {
        return sslEnable;
    }

    public void setSslEnable(boolean sslEnable) {
        this.sslEnable = sslEnable;
    }

    @Description("The port which is the admin server listening on.")
    @JsonProperty(defaultValue = "8081")
    @Minimum(1023)
    public int getAdminPort() {
        return adminPort;
    }

    public void setAdminPort(int port) {
        this.adminPort = port;
    }

    @Description("Reference to the `Secret` which holds the certificate and private key pair.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public CertAndKeySecretSource getCertificateAndKey() {
        return certificateAndKey;
    }

    public void setCertificateAndKey(CertAndKeySecretSource certificateAndKey) {
        this.certificateAndKey = certificateAndKey;
    }

    @Description("Configurations for HTTP server")
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }

    @Description("CORS configuration for the HTTP Bridge.")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public KafkaBridgeHttpCors getCors() {
        return cors;
    }

    public void setCors(KafkaBridgeHttpCors cors) {
        this.cors = cors;
    }

    @Override
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties != null ? this.additionalProperties : Map.of();
    }

    @Override
    public void setAdditionalProperty(String name, Object value) {
        if (this.additionalProperties == null) {
            this.additionalProperties = new HashMap<>(2);
        }
        this.additionalProperties.put(name, value);
    }
}
