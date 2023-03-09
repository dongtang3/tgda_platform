package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

import com.github.tgda.engine.core.operator.DataScienceOperator;

public class ClosenessCentralityAlgorithmConfig extends ResultPaginationableConfig {

    private DataScienceOperator.ValueSortingLogic valueSortingLogic;

    public DataScienceOperator.ValueSortingLogic getCentralityWeightSortingLogic() {
        return valueSortingLogic;
    }

    public void setCentralityWeightSortingLogic(DataScienceOperator.ValueSortingLogic valueSortingLogic) {
        this.valueSortingLogic = valueSortingLogic;
    }
}
