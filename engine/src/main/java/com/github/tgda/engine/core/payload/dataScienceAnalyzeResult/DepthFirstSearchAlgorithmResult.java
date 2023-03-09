package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import com.github.tgda.engine.core.operator.configuration.dataScienceConfig.DepthFirstSearchAlgorithmConfig;

import java.util.Date;

public class DepthFirstSearchAlgorithmResult {

    private EntityTraversalResult entityTraversalResult;
    private String graphName;
    private DepthFirstSearchAlgorithmConfig depthFirstSearchAlgorithmConfig;
    private Date algorithmExecuteStartTime;
    private Date algorithmExecuteEndTime;

    public DepthFirstSearchAlgorithmResult(String graphName, DepthFirstSearchAlgorithmConfig depthFirstSearchAlgorithmConfig){
        this.graphName = graphName;
        this.depthFirstSearchAlgorithmConfig = depthFirstSearchAlgorithmConfig;
        this.algorithmExecuteStartTime = new Date();
    }

    public String getGraphName() {
        return graphName;
    }

    public DepthFirstSearchAlgorithmConfig getDepthFirstSearchAlgorithmConfig() {
        return depthFirstSearchAlgorithmConfig;
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

    public EntityTraversalResult getEntityTraversalResult() {
        return entityTraversalResult;
    }

    public void setEntityTraversalResult(EntityTraversalResult entityTraversalResult) {
        this.entityTraversalResult = entityTraversalResult;
    }
}
