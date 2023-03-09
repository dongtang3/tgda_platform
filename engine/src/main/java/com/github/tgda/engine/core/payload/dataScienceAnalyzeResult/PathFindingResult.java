package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import com.github.tgda.engine.core.term.Entity;

import java.util.List;
import java.util.Map;

public class PathFindingResult {

    private String startEntityUID;
    private String endEntityUID;
    private String startEntityType;
    private String endEntityType;
    private double pathWeight;
    private List<String> pathEntityUIDs;
    private Map<String,Double> pathEntityTraversalWeights;
    private List<Entity> pathConceptionEntities;

    public PathFindingResult(String startEntityUID, String startEntityType, String endEntityUID,
                             String endEntityType, double pathWeight, List<String> pathEntityUIDs,
                             Map<String,Double> pathEntityTraversalWeights, List<Entity> pathConceptionEntities){
        this.startEntityUID = startEntityUID;
        this.startEntityType = startEntityType;
        this.endEntityUID = endEntityUID;
        this.endEntityType = endEntityType;
        this.pathWeight = pathWeight;
        this.pathEntityUIDs = pathEntityUIDs;
        this.pathEntityTraversalWeights = pathEntityTraversalWeights;
        this.pathConceptionEntities = pathConceptionEntities;
    }

    public String getStartEntityUID() {
        return startEntityUID;
    }

    public String getEndEntityUID() {
        return endEntityUID;
    }

    public String getStartEntityType() {
        return startEntityType;
    }

    public String getEndEntityType() {
        return endEntityType;
    }

    public double getPathWeight() {
        return pathWeight;
    }

    public List<String> getPathEntityUIDs() {
        return pathEntityUIDs;
    }

    public Map<String, Double> getPathEntityTraversalWeights() {
        return pathEntityTraversalWeights;
    }

    public List<Entity> getPathConceptionEntities() {
        return pathConceptionEntities;
    }
}
