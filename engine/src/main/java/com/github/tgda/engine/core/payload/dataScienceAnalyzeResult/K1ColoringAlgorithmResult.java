package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import com.github.tgda.engine.core.operator.configuration.dataScienceConfig.K1ColoringAlgorithmConfig;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class K1ColoringAlgorithmResult {

    private String graphName;
    private K1ColoringAlgorithmConfig k1ColoringAlgorithmConfig;
    private Date algorithmExecuteStartTime;
    private Date algorithmExecuteEndTime;
    private List<CommunityDetectionResult> k1Colors;

    public K1ColoringAlgorithmResult(String graphName, K1ColoringAlgorithmConfig k1ColoringAlgorithmConfig){
        this.graphName = graphName;
        this.k1ColoringAlgorithmConfig = k1ColoringAlgorithmConfig;
        this.k1Colors = new ArrayList<>();
        this.algorithmExecuteStartTime = new Date();
    }

    public String getGraphName() {
        return graphName;
    }

    public K1ColoringAlgorithmConfig getK1ColoringAlgorithmConfig() {
        return k1ColoringAlgorithmConfig;
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

    public List<CommunityDetectionResult> getK1Colors() {
        return k1Colors;
    }
}
