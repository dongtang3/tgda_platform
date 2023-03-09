package com.github.tgda.engine.core.payload.dataValueObject;

import com.github.tgda.engine.core.term.AttributeDataType;

import java.io.Serializable;

public class AttributeVO implements Serializable {

    private String attributeKindName;
    private String attributeKindUID;
    private String attributeKindDesc;
    private AttributeDataType attributeDataType;

    public AttributeVO(String attributeKindName, String attributeKindDesc, AttributeDataType attributeDataType, String attributeKindUID){
        this.attributeKindName = attributeKindName;
        this.attributeKindUID = attributeKindUID;
        this.attributeKindDesc = attributeKindDesc;
        this.attributeDataType = attributeDataType;
    }

    public AttributeVO(){

    }

    public String getAttributeKindName() {
        return attributeKindName;
    }

    public void setAttributeKindName(String attributeKindName) {
        this.attributeKindName = attributeKindName;
    }

    public String getAttributeKindUID() {
        return attributeKindUID;
    }

    public void setAttributeKindUID(String attributeKindUID) {
        this.attributeKindUID = attributeKindUID;
    }

    public String getAttributeKindDesc() {
        return attributeKindDesc;
    }

    public void setAttributeKindDesc(String attributeKindDesc) {
        this.attributeKindDesc = attributeKindDesc;
    }

    public AttributeDataType getAttributeDataType() {
        return attributeDataType;
    }

    public void setAttributeDataType(AttributeDataType attributeDataType) {
        this.attributeDataType = attributeDataType;
    }
}
