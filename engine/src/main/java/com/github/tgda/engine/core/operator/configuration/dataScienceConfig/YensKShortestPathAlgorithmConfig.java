package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

import com.github.tgda.engine.core.operator.DataScienceOperator;

public class YensKShortestPathAlgorithmConfig extends DataScienceBaseAlgorithmConfig{

    private String relationshipWeightAttribute;
    private String sourceEntityUID;
    private String targetEntityUID;
    private int k = 1;
    private DataScienceOperator.ValueSortingLogic valueSortingLogic;

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

    public int getK() {
        return k;
    }

    public void setK(int k) {
        this.k = k;
    }

    public DataScienceOperator.ValueSortingLogic getPathWeightSortingLogic() {
        return valueSortingLogic;
    }

    public void setPathWeightSortingLogic(DataScienceOperator.ValueSortingLogic valueSortingLogic) {
        this.valueSortingLogic = valueSortingLogic;
    }
}
