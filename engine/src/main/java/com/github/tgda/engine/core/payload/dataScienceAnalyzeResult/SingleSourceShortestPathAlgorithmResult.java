package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import com.github.tgda.engine.core.operator.configuration.dataScienceConfig.SingleSourceShortestPathAlgorithmConfig;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SingleSourceShortestPathAlgorithmResult {

    private String graphName;
    private SingleSourceShortestPathAlgorithmConfig singleSourceShortestPathAlgorithmConfig;
    private Date algorithmExecuteStartTime;
    private Date algorithmExecuteEndTime;
    private List<EntityAnalyzeResult> singleSourceShortestPathCosts;

    public SingleSourceShortestPathAlgorithmResult(String graphName, SingleSourceShortestPathAlgorithmConfig singleSourceShortestPathAlgorithmConfig){
        this.graphName = graphName;
        this.singleSourceShortestPathAlgorithmConfig = singleSourceShortestPathAlgorithmConfig;
        this.singleSourceShortestPathCosts = new ArrayList<>();
        this.algorithmExecuteStartTime = new Date();
    }

    public String getGraphName() {
        return graphName;
    }

    public SingleSourceShortestPathAlgorithmConfig getSingleSourceShortestPathAlgorithmConfig() {
        return singleSourceShortestPathAlgorithmConfig;
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

    public List<EntityAnalyzeResult> getSingleSourceShortestPathDistances() {
        return singleSourceShortestPathCosts;
    }
}
