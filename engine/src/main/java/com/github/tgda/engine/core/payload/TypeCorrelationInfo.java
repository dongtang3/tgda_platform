package com.github.tgda.engine.core.payload;

public class TypeCorrelationInfo {

    private String sourceConceptionKindName;
    private String targetConceptionKindName;
    private String relationKindName;
    private long relationEntityCount;

    public TypeCorrelationInfo(String sourceConceptionKindName, String targetConceptionKindName, String relationKindName, long relationEntityCount){
        this.sourceConceptionKindName = sourceConceptionKindName;
        this.targetConceptionKindName = targetConceptionKindName;
        this.relationKindName = relationKindName;
        this.relationEntityCount = relationEntityCount;
    }

    public String getSourceConceptionKindName() {
        return sourceConceptionKindName;
    }

    public String getTargetConceptionKindName() {
        return targetConceptionKindName;
    }

    public String getRelationKindName() {
        return relationKindName;
    }

    public long getRelationshipEntityCount(){
        return relationEntityCount;
    }
}
