package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import com.github.tgda.engine.core.operator.configuration.dataScienceConfig.WeaklyConnectedComponentsAlgorithmConfig;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class WeaklyConnectedComponentsAlgorithmResult {

    private String graphName;
    private WeaklyConnectedComponentsAlgorithmConfig weaklyConnectedComponentsAlgorithmConfig;
    private Date algorithmExecuteStartTime;
    private Date algorithmExecuteEndTime;
    private List<ComponentDetectionResult> componentDetectionResults;

    public WeaklyConnectedComponentsAlgorithmResult(String graphName, WeaklyConnectedComponentsAlgorithmConfig weaklyConnectedComponentsAlgorithmConfig){
        this.graphName = graphName;
        this.weaklyConnectedComponentsAlgorithmConfig = weaklyConnectedComponentsAlgorithmConfig;
        this.componentDetectionResults = new ArrayList<>();
        this.algorithmExecuteStartTime = new Date();
    }

    public String getGraphName() {
        return graphName;
    }

    public WeaklyConnectedComponentsAlgorithmConfig getWeaklyConnectedComponentsAlgorithmConfig() {
        return weaklyConnectedComponentsAlgorithmConfig;
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

    public List<ComponentDetectionResult> getWCCComponents() {
        return componentDetectionResults;
    }
}
