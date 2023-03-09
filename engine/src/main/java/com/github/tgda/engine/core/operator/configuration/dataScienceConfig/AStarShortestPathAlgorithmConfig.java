package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

public class AStarShortestPathAlgorithmConfig extends DataScienceBaseAlgorithmConfig{

    private String relationshipWeightAttribute;
    private String sourceEntityUID;
    private String targetEntityUID;
    private String latitudeAttribute;
    private String longitudeAttribute;
    private Integer maxPathNumber;

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

    public String getLatitudeAttribute() {
        return latitudeAttribute;
    }

    public void setLatitudeAttribute(String latitudeAttribute) {
        this.latitudeAttribute = latitudeAttribute;
    }

    public String getLongitudeAttribute() {
        return longitudeAttribute;
    }

    public void setLongitudeAttribute(String longitudeAttribute) {
        this.longitudeAttribute = longitudeAttribute;
    }

    public Integer getMaxPathNumber() {
        return maxPathNumber;
    }

    public void setMaxPathNumber(Integer maxPathNumber) {
        this.maxPathNumber = maxPathNumber;
    }
}
