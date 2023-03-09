package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import com.github.tgda.engine.core.operator.configuration.dataScienceConfig.LouvainAlgorithmConfig;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class LouvainAlgorithmResult {

    private String graphName;
    private LouvainAlgorithmConfig louvainAlgorithmConfig;
    private Date algorithmExecuteStartTime;
    private Date algorithmExecuteEndTime;
    private List<CommunityDetectionResult> communityDetectionResults;

    public LouvainAlgorithmResult(String graphName, LouvainAlgorithmConfig louvainAlgorithmConfig){
        this.graphName = graphName;
        this.louvainAlgorithmConfig = louvainAlgorithmConfig;
        this.communityDetectionResults = new ArrayList<>();
        this.algorithmExecuteStartTime = new Date();
    }

    public String getGraphName() {
        return graphName;
    }

    public LouvainAlgorithmConfig getLouvainAlgorithmConfig() {
        return louvainAlgorithmConfig;
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

    public List<CommunityDetectionResult> getLouvainCommunities() {
        return communityDetectionResults;
    }
}

