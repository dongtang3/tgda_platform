package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

import java.util.Set;

public class PersonalisedEigenvectorCentralityAlgorithmConfig extends EigenvectorCentralityAlgorithmConfig{

    private Set<String> personalizedEigenvectorCentralityEntityUIDs;

    public Set<String> getPersonalizedEigenvectorCentralityEntityUIDs() {
        return personalizedEigenvectorCentralityEntityUIDs;
    }

    public void setPersonalizedEigenvectorCentralityEntityUIDs(Set<String> personalizedEigenvectorCentralityEntityUIDs) {
        this.personalizedEigenvectorCentralityEntityUIDs = personalizedEigenvectorCentralityEntityUIDs;
    }
}
