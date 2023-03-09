package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

import com.github.tgda.engine.core.operator.DataScienceOperator;

public class DijkstraSingleSourceAlgorithmConfig extends ResultPaginationableConfig{

    private String relationshipWeightAttribute;
    private String sourceEntityUID;
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

    public DataScienceOperator.ValueSortingLogic getPathWeightSortingLogic() {
        return valueSortingLogic;
    }

    public void setPathWeightSortingLogic(DataScienceOperator.ValueSortingLogic valueSortingLogic) {
        this.valueSortingLogic = valueSortingLogic;
    }
}
