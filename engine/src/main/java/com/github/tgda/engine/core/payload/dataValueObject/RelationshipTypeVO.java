package com.github.tgda.engine.core.payload.dataValueObject;

import java.io.Serializable;

public class RelationshipTypeVO implements Serializable {

    private String relationKindName;
    private String relationKindDesc;

    public RelationshipTypeVO(String relationKindName, String relationKindDesc){
        this.relationKindName = relationKindName;
        this.relationKindDesc = relationKindDesc;
    }

    public RelationshipTypeVO(){}

    public String getRelationKindName() {
        return relationKindName;
    }

    public void setRelationKindName(String relationKindName) {
        this.relationKindName = relationKindName;
    }

    public String getRelationKindDesc() {
        return relationKindDesc;
    }

    public void setRelationKindDesc(String relationKindDesc) {
        this.relationKindDesc = relationKindDesc;
    }
}
