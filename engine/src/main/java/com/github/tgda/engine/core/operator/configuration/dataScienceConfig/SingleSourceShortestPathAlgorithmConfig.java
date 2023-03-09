package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

import com.github.tgda.engine.core.operator.DataScienceOperator;

public class SingleSourceShortestPathAlgorithmConfig extends ResultPaginationableConfig {

    private String relationshipWeightAttribute;
    private String sourceEntityUID;
    private float delta = 3.0f;
    private DataScienceOperator.ValueSortingLogic valueSortingLogic;

    public SingleSourceShortestPathAlgorithmConfig(String sourceEntityUID){
        this.sourceEntityUID = sourceEntityUID;
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

    public float getDelta() {
        return delta;
    }

    public void setDelta(float delta) {
        this.delta = delta;
    }

    public DataScienceOperator.ValueSortingLogic getDistanceSortingLogic() {
        return valueSortingLogic;
    }

    public void setDistanceSortingLogic(DataScienceOperator.ValueSortingLogic valueSortingLogic) {
        this.valueSortingLogic = valueSortingLogic;
    }
}
