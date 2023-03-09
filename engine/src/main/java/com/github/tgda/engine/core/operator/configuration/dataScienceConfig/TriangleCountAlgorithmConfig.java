package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

import com.github.tgda.engine.core.operator.DataScienceOperator;

public class TriangleCountAlgorithmConfig extends ResultPaginationableConfig{

    private Integer maxDegree;
    private DataScienceOperator.ValueSortingLogic valueSortingLogic;

    public Integer getMaxDegree() {
        return maxDegree;
    }

    public void setMaxDegree(Integer maxDegree) {
        this.maxDegree = maxDegree;
    }

    public DataScienceOperator.ValueSortingLogic getTriangleCountSortingLogic() {
        return valueSortingLogic;
    }

    public void setTriangleCountSortingLogic(DataScienceOperator.ValueSortingLogic valueSortingLogic) {
        this.valueSortingLogic = valueSortingLogic;
    }
}
