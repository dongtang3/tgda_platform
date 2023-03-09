package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

import com.github.tgda.engine.core.operator.DataScienceOperator;

public class LocalClusteringCoefficientAlgorithmConfig extends ResultPaginationableConfig{

    private String triangleCountProperty;
    private DataScienceOperator.ValueSortingLogic valueSortingLogic;

    public String getTriangleCountProperty() {
        return triangleCountProperty;
    }

    public void setTriangleCountProperty(String triangleCountProperty) {
        this.triangleCountProperty = triangleCountProperty;
    }

    public DataScienceOperator.ValueSortingLogic getCoefficientSortingLogic() {
        return valueSortingLogic;
    }

    public void setCoefficientSortingLogic(DataScienceOperator.ValueSortingLogic valueSortingLogic) {
        this.valueSortingLogic = valueSortingLogic;
    }
}
