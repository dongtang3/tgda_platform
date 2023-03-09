package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

import java.util.List;

public class PathWalkResult {

    private String startEntityUID;
    private List<String> walkEntitiesFootprints;

    public PathWalkResult(String startEntityUID,List<String> walkEntitiesFootprints){
        this.startEntityUID = startEntityUID;
        this.walkEntitiesFootprints = walkEntitiesFootprints;
    }

    public String getStartEntityUID() {
        return startEntityUID;
    }

    public List<String> getWalkEntitiesFootprints() {
        return walkEntitiesFootprints;
    }
}
