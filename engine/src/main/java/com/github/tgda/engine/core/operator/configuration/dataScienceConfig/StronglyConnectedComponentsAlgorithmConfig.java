package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

import com.github.tgda.engine.core.operator.DataScienceOperator;

public class StronglyConnectedComponentsAlgorithmConfig extends ResultPaginationableConfig {

    private DataScienceOperator.ValueSortingLogic valueSortingLogic;

    public DataScienceOperator.ValueSortingLogic getCommunityIdSortingLogic() {
        return valueSortingLogic;
    }

    public void setCommunityIdSortingLogic(DataScienceOperator.ValueSortingLogic valueSortingLogic) {
        this.valueSortingLogic = valueSortingLogic;
    }
}
