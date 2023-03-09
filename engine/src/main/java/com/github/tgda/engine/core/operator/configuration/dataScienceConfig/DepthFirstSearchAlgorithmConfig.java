package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

import java.util.Set;

public class DepthFirstSearchAlgorithmConfig extends DataScienceBaseAlgorithmConfig {

    private String sourceEntityUID;
    private Set<String> terminateAtEntityUIDs;
    private int maxDepth = -1;

    public DepthFirstSearchAlgorithmConfig(String sourceEntityUID){
        this.sourceEntityUID = sourceEntityUID;
    }

    public String getSourceEntityUID() {
        return sourceEntityUID;
    }

    public Set<String> getTerminateAtEntityUIDs() {
        return terminateAtEntityUIDs;
    }

    public void setTerminateAtEntityUIDs(Set<String> terminateAtEntityUIDs) {
        this.terminateAtEntityUIDs = terminateAtEntityUIDs;
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public void setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
    }
}
