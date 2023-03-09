package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import com.github.tgda.engine.core.operator.configuration.dataScienceConfig.HarmonicCentralityAlgorithmConfig;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HarmonicCentralityAlgorithmResult {

    private String graphName;
    private HarmonicCentralityAlgorithmConfig harmonicCentralityAlgorithmConfig;
    private Date algorithmExecuteStartTime;
    private Date algorithmExecuteEndTime;
    private List<EntityAnalyzeResult> harmonicCentralityWeights;

    public HarmonicCentralityAlgorithmResult(String graphName, HarmonicCentralityAlgorithmConfig harmonicCentralityAlgorithmConfig){
        this.graphName = graphName;
        this.harmonicCentralityAlgorithmConfig = harmonicCentralityAlgorithmConfig;
        this.harmonicCentralityWeights = new ArrayList<>();
        this.algorithmExecuteStartTime = new Date();
    }

    public String getGraphName() {
        return graphName;
    }

    public HarmonicCentralityAlgorithmConfig getHarmonicCentralityAlgorithmConfig() {
        return harmonicCentralityAlgorithmConfig;
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

    public List<EntityAnalyzeResult> getHarmonicCentralityWeights() {
        return harmonicCentralityWeights;
    }
}
