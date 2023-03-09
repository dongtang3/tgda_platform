package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

public class DijkstraSourceTargetAlgorithmConfig extends DataScienceBaseAlgorithmConfig {

    private Integer maxPathNumber;
    private String relationshipWeightAttribute;
    private String sourceEntityUID;
    private String targetEntityUID;

    public Integer getMaxPathNumber() {
        return maxPathNumber;
    }

    public void setMaxPathNumber(Integer maxPathNumber) {
        this.maxPathNumber = maxPathNumber;
    }

    public String getRelationshipWeightAttribute() {
        return relationshipWeightAttribute;
    }

    public void setRelationshipWeightAttribute(String relationshipWeightAttribute) {
        this.relationshipWeightAttribute = relationshipWeightAttribute;
    }

    public String getSourceEntityUID() {
        return sourceEntityUID;
    }

    public void setSourceEntityUID(String sourceEntityUID) {
        this.sourceEntityUID = sourceEntityUID;
    }

    public String getTargetEntityUID() {
        return targetEntityUID;
    }

    public void setTargetEntityUID(String targetEntityUID) {
        this.targetEntityUID = targetEntityUID;
    }
}
