package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import com.github.tgda.engine.core.operator.configuration.dataScienceConfig.LocalClusteringCoefficientAlgorithmConfig;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LocalClusteringCoefficientAlgorithmResult {

    private String graphName;
    private LocalClusteringCoefficientAlgorithmConfig localClusteringCoefficientAlgorithmConfig;
    private Date algorithmExecuteStartTime;
    private Date algorithmExecuteEndTime;
    private List<EntityAnalyzeResult> localClusteringCoefficients;

    public LocalClusteringCoefficientAlgorithmResult(String graphName, LocalClusteringCoefficientAlgorithmConfig localClusteringCoefficientAlgorithmConfig){
        this.graphName = graphName;
        this.localClusteringCoefficientAlgorithmConfig = localClusteringCoefficientAlgorithmConfig;
        this.localClusteringCoefficients = new ArrayList<>();
        this.algorithmExecuteStartTime = new Date();
    }

    public String getGraphName() {
        return graphName;
    }

    public LocalClusteringCoefficientAlgorithmConfig getLocalClusteringCoefficientAlgorithmConfig() {
        return localClusteringCoefficientAlgorithmConfig;
    }

    public Date getAlgorithmExecuteStartTime() {
        return algorithmExecuteStartTime;
    }

    public Date getAlgorithmExecuteEndTime() {
        return algorithmExecuteEndTime;
    }

    public void setAlgorithmExecuteEndTime(Date algorithmExecuteEndTime) {
        this.algorithmExecuteEndTime = algorithmExecuteEndTime;
    }

    public List<EntityAnalyzeResult> getLocalClusteringCoefficients() {
        return localClusteringCoefficients;
    }
}
