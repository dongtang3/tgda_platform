package com.github.tgda.dataCollector.payload;

public class RelationshipEntityMetaInfo {

    private String relationEntityUID;
    private String sourceEntityUID;
    private String targetEntityUID;
    private String relationKind;

    public RelationshipEntityMetaInfo(String relationKind,String relationEntityUID,String sourceEntityUID,String targetEntityUID){
        this.relationKind = relationKind;
        this.relationEntityUID = relationEntityUID;
        this.sourceEntityUID = sourceEntityUID;
        this.targetEntityUID = targetEntityUID;
    }

    public String getRelationshipEntityUID() {
        return relationEntityUID;
    }

    public String getSourceEntityUID() {
        return sourceEntityUID;
    }

    public String getTargetEntityUID() {
        return targetEntityUID;
    }

    public String getRelationKind() {
        return relationKind;
    }
}
