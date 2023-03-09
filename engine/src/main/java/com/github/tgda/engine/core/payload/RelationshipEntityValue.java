package com.github.tgda.engine.core.payload;

import java.util.Map;

public class RelationshipEntityValue {

    private String relationEntityUID;
    private String fromEntityUID;
    private String toEntityUID;
    private Map<String,Object> entityAttributesValue;

    public RelationshipEntityValue(){}

    public RelationshipEntityValue(String relationEntityUID, String fromEntityUID, String toEntityUID,
                                   Map<String,Object> entityAttributesValue){
        this.relationEntityUID = relationEntityUID;
        this.fromEntityUID = fromEntityUID;
        this.toEntityUID = toEntityUID;
        this.entityAttributesValue = entityAttributesValue;
    }

    public String getRelationshipEntityUID() {
        return relationEntityUID;
    }

    public void setRelationshipEntityUID(String relationEntityUID) {
        this.relationEntityUID = relationEntityUID;
    }

    public String getFromEntityUID() {
        return fromEntityUID;
    }

    public void setFromEntityUID(String fromEntityUID) {
        this.fromEntityUID = fromEntityUID;
    }

    public String getToEntityUID() {
        return toEntityUID;
    }

    public void setToEntityUID(String toEntityUID) {
        this.toEntityUID = toEntityUID;
    }

    public Map<String, Object> getEntityAttributesValue() {
        return entityAttributesValue;
    }

    public void setEntityAttributesValue(Map<String, Object> entityAttributesValue) {
        this.entityAttributesValue = entityAttributesValue;
    }
}
