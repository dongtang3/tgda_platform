package com.github.tgda.engine.core.payload.dataScienceAnalyzeResult;

public class ComponentDetectionResult {

    private String conceptionEntityUID;
    private int componentId;

    public ComponentDetectionResult(String conceptionEntityUID,int componentId){
        this.conceptionEntityUID = conceptionEntityUID;
        this.componentId = componentId;
    }

    public String getEntityUID() {
        return conceptionEntityUID;
    }

    public int getComponentId() {
        return componentId;
    }

    public String toString(){
        return this.conceptionEntityUID+" -> componentId: "+this.componentId;
    }
}
