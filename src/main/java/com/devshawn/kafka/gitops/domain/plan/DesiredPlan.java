package com.devshawn.kafka.gitops.domain.plan;

import com.devshawn.kafka.gitops.enums.PlanAction;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.List;

@FreeBuilder
@JsonDeserialize(builder = DesiredPlan.Builder.class)
public interface DesiredPlan {

    List<TopicPlan> getTopicPlans();

    List<AclPlan> getAclPlans();

    default DesiredPlan toChangesOnlyPlan() {
        DesiredPlan.Builder builder = new DesiredPlan.Builder();
        if (getTopicPlans() != null) {
            getTopicPlans().stream().filter(it -> !it.getAction().equals(PlanAction.NO_CHANGE)).map(TopicPlan::toChangesOnlyPlan).forEach(builder::addTopicPlans);
        }
        getAclPlans().stream().filter(it -> !it.getAction().equals(PlanAction.NO_CHANGE)).forEach(builder::addAclPlans);
        return builder.build();
    }

    class Builder extends DesiredPlan_Builder {
    }
}
