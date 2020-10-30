package com.devshawn.kafka.gitops.domain.state;

import com.devshawn.kafka.gitops.domain.options.GetAclOptions;
import com.devshawn.kafka.gitops.domain.state.service.ApplicationService;
import com.devshawn.kafka.gitops.domain.state.service.KafkaConnectService;
import com.devshawn.kafka.gitops.domain.state.service.KafkaStreamsService;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;
import java.util.Optional;

@JsonSubTypes({
        @JsonSubTypes.Type(value = ApplicationService.class, name = "application"),
        @JsonSubTypes.Type(value = KafkaConnectService.class, name = "kafka-connect"),
        @JsonSubTypes.Type(value = KafkaStreamsService.class, name = "kafka-streams")
})
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public abstract class ServiceDetails extends AbstractService {

    public String type;

    @JsonProperty("service-account")
    public abstract Optional<String> getServiceAccount();

    public List<AclDetails.Builder> getAcls(GetAclOptions options) {
        throw new UnsupportedOperationException("Method getAcls is not implemented.");
    }
}
