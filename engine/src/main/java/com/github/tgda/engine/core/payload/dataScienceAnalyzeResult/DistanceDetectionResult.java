package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

public class DistanceDetectionResult {

    private String sourceEntityUID;
    private String targetEntityUID;
    private double distance;

    public DistanceDetectionResult(String sourceEntityUID,String targetEntityUID,double distance){
        this.sourceEntityUID = sourceEntityUID;
        this.targetEntityUID = targetEntityUID;
        this.distance = distance;
    }

    public String getSourceEntityUID() {
        return sourceEntityUID;
    }

    public String getTargetEntityUID() {
        return targetEntityUID;
    }

    public double getDistance() {
        return distance;
    }

    public String toString(){
        return this.sourceEntityUID+"|"+this.targetEntityUID+" -> distance: "+this.distance;
    }
}
