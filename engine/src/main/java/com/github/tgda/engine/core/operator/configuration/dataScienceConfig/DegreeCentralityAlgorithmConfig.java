package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

import com.github.tgda.engine.core.operator.DataScienceOperator;

public class DegreeCentralityAlgorithmConfig extends ResultPaginationableConfig {

    private DataScienceOperator.ComputeOrientation computeOrientation;
    private String relationshipWeightAttribute;
    private DataScienceOperator.ValueScalerLogic valueScalerLogic;
    private DataScienceOperator.ValueSortingLogic valueSortingLogic;

    public DataScienceOperator.ComputeOrientation getComputeOrientation() {
        return computeOrientation;
    }

    public void setComputeOrientation(DataScienceOperator.ComputeOrientation computeOrientation) {
        this.computeOrientation = computeOrientation;
    }

    public String getRelationshipWeightAttribute() {
        return relationshipWeightAttribute;
    }

    public void setRelationshipWeightAttribute(String relationshipWeightAttribute) {
        this.relationshipWeightAttribute = relationshipWeightAttribute;
    }

    public DataScienceOperator.ValueScalerLogic getScoreScalerLogic() {
        return valueScalerLogic;
    }

    public void setScoreScalerLogic(DataScienceOperator.ValueScalerLogic valueScalerLogic) {
        this.valueScalerLogic = valueScalerLogic;
    }

    public DataScienceOperator.ValueSortingLogic getScoreSortingLogic() {
        return valueSortingLogic;
    }

    public void setScoreSortingLogic(DataScienceOperator.ValueSortingLogic valueSortingLogic) {
        this.valueSortingLogic = valueSortingLogic;
    }
}
