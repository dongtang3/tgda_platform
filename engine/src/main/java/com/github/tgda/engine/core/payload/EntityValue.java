package com.github.tgda.engine.core.payload;

import java.util.Map;

public class EntityValue {

    private String conceptionEntityUID;
    private Map<String,Object> entityAttributesValue;

    public EntityValue(){}

    public EntityValue(String conceptionEntityUID){
        this.conceptionEntityUID = conceptionEntityUID;
    }

    public EntityValue(Map<String,Object> entityAttributesValue){
        this.entityAttributesValue = entityAttributesValue;
    }

    public EntityValue(String conceptionEntityUID, Map<String,Object> entityAttributesValue){
        this.conceptionEntityUID = conceptionEntityUID;
        this.entityAttributesValue = entityAttributesValue;
    }

    public String getEntityUID() {
        return conceptionEntityUID;
    }

    public void setEntityUID(String conceptionEntityUID) {
        this.conceptionEntityUID = conceptionEntityUID;
    }

    public Map<String, Object> getEntityAttributesValue() {
        return entityAttributesValue;
    }

    public void setEntityAttributesValue(Map<String, Object> entityAttributesValue) {
        this.entityAttributesValue = entityAttributesValue;
    }
}
