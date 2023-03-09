package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import com.github.tgda.engine.core.operator.configuration.dataScienceConfig.TriangleCountAlgorithmConfig;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class TriangleCountAlgorithmResult {

    private String graphName;
    private TriangleCountAlgorithmConfig triangleCountAlgorithmConfig;
    private Date algorithmExecuteStartTime;
    private Date algorithmExecuteEndTime;
    private List<TriangleCountDetectionResult> triangleCountDetectionResult;

    public TriangleCountAlgorithmResult(String graphName, TriangleCountAlgorithmConfig triangleCountAlgorithmConfig){
        this.graphName = graphName;
        this.triangleCountAlgorithmConfig = triangleCountAlgorithmConfig;
        this.triangleCountDetectionResult = new ArrayList<>();
        this.algorithmExecuteStartTime = new Date();
    }

    public String getGraphName() {
        return graphName;
    }

    public TriangleCountAlgorithmConfig getTriangleCountAlgorithmConfig() {
        return triangleCountAlgorithmConfig;
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

    public List<TriangleCountDetectionResult> getTriangleCounts() {
        return triangleCountDetectionResult;
    }
}
