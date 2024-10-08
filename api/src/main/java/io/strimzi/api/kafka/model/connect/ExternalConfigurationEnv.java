/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.api.kafka.model.connect;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.strimzi.api.annotations.DeprecatedType;
import io.strimzi.api.kafka.model.common.Constants;
import io.strimzi.api.kafka.model.common.UnknownPropertyPreserving;
import io.strimzi.api.kafka.model.common.template.ContainerEnvVar;
import io.strimzi.crdgenerator.annotations.Description;
import io.sundr.builder.annotations.Buildable;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;

/**
 * Representation for environment variables which will be passed to Kafka Connect
 */
@Buildable(
        editableEnabled = false,
        builderPackage = Constants.FABRIC8_KUBERNETES_API
)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
@JsonPropertyOrder({"name", "valueFrom"})
@Deprecated
@DeprecatedType(replacedWithType = ContainerEnvVar.class)
@EqualsAndHashCode
@ToString
public class ExternalConfigurationEnv implements UnknownPropertyPreserving {
    private String name;
    private ExternalConfigurationEnvVarSource valueFrom;
    private Map<String, Object> additionalProperties;

    @Description("Name of the environment variable which will be passed to the Kafka Connect pods. " +
            "The name of the environment variable cannot start with `KAFKA_` or `STRIMZI_`.")
    @JsonProperty(required = true)
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Description("Value of the environment variable which will be passed to the Kafka Connect pods. " +
            "It can be passed either as a reference to Secret or ConfigMap field. " +
            "The field has to specify exactly one Secret or ConfigMap.")
    @JsonProperty(required = true)
    public ExternalConfigurationEnvVarSource getValueFrom() {
        return valueFrom;
    }

    public void setValueFrom(ExternalConfigurationEnvVarSource valueFrom) {
        this.valueFrom = valueFrom;
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
