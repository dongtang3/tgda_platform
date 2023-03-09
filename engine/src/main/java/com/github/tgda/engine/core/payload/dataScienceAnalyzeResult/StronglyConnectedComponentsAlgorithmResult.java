package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import com.github.tgda.engine.core.operator.configuration.dataScienceConfig.StronglyConnectedComponentsAlgorithmConfig;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class StronglyConnectedComponentsAlgorithmResult {

    private String graphName;
    private StronglyConnectedComponentsAlgorithmConfig stronglyConnectedComponentsAlgorithmConfig;
    private Date algorithmExecuteStartTime;
    private Date algorithmExecuteEndTime;
    private List<ComponentDetectionResult> componentDetectionResults;

    public StronglyConnectedComponentsAlgorithmResult(String graphName, StronglyConnectedComponentsAlgorithmConfig stronglyConnectedComponentsAlgorithmConfig){
        this.graphName = graphName;
        this.stronglyConnectedComponentsAlgorithmConfig = stronglyConnectedComponentsAlgorithmConfig;
        this.componentDetectionResults = new ArrayList<>();
        this.algorithmExecuteStartTime = new Date();
    }

    public String getGraphName() {
        return graphName;
    }

    public StronglyConnectedComponentsAlgorithmConfig getStronglyConnectedComponentsAlgorithmConfig() {
        return stronglyConnectedComponentsAlgorithmConfig;
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

    public List<ComponentDetectionResult> getSCCComponents() {
        return componentDetectionResults;
    }
}
