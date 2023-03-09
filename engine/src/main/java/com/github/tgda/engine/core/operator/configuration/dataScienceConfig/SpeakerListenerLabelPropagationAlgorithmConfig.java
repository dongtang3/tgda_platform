package com.github.tgda.engine.core.operator.configuration.dataScienceConfig;

import com.github.tgda.engine.core.operator.DataScienceOperator;

public class SpeakerListenerLabelPropagationAlgorithmConfig extends ResultPaginationableConfig{

    private int maxIterations = 0;
    private float minAssociationStrength = 0.2f;
    private DataScienceOperator.ValueSortingLogic valueSortingLogic;

    public SpeakerListenerLabelPropagationAlgorithmConfig(int maxIterations){
        this.maxIterations = maxIterations;
    }

    public DataScienceOperator.ValueSortingLogic getCommunityIdsSortingLogic() {
        return valueSortingLogic;
    }

    public void setCommunityIdsSortingLogic(DataScienceOperator.ValueSortingLogic valueSortingLogic) {
        this.valueSortingLogic = valueSortingLogic;
    }

    public int getMaxIterations() {
        return maxIterations;
    }

    public void setMaxIterations(int maxIterations) {
        this.maxIterations = maxIterations;
    }

    public float getMinAssociationStrength() {
        return minAssociationStrength;
    }

    public void setMinAssociationStrength(float minAssociationStrength) {
        this.minAssociationStrength = minAssociationStrength;
    }
}
