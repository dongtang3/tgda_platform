package com.github.tgda.engine.core.payload.dataValueObject;

import java.io.Serializable;

public class TypeVO implements Serializable {

    private String conceptionKindName;
    private String conceptionKindDesc;

    public TypeVO(String conceptionKindName, String conceptionKindDesc){
        this.conceptionKindName = conceptionKindName;
        this.conceptionKindDesc = conceptionKindDesc;
    }

    public TypeVO(){}

    public String getConceptionKindName() {
        return conceptionKindName;
    }

    public void setConceptionKindName(String conceptionKindName) {
        this.conceptionKindName = conceptionKindName;
    }

    public String getConceptionKindDesc() {
        return conceptionKindDesc;
    }

    public void setConceptionKindDesc(String conceptionKindDesc) {
        this.conceptionKindDesc = conceptionKindDesc;
    }
}
