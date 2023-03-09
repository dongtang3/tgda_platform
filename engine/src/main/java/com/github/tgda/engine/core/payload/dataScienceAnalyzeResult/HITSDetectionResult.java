package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

public class HITSDetectionResult {

    private String conceptionEntityUID;
    private double authScore;
    private double hubScore;

    public HITSDetectionResult(String conceptionEntityUID,double authScore,double hubScore){
        this.conceptionEntityUID = conceptionEntityUID;
        this.authScore = authScore;
        this.hubScore = hubScore;
    }

    public String getEntityUID() {
        return conceptionEntityUID;
    }

    public double getAuthScore() {
        return authScore;
    }

    public double getHubScore() {
        return hubScore;
    }

    public String toString(){
        return this.conceptionEntityUID+" -> "+"authScore"+": "+this.authScore+" | hubScore: "+this.hubScore;
    }
}
