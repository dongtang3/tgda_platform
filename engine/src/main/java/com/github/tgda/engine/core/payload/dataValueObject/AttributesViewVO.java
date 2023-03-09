package com.github.tgda.engine.core.payload.dataValueObject;

import com.github.tgda.engine.core.term.AttributesView;

import java.io.Serializable;

public class AttributesViewVO implements Serializable {

    private String attributesViewKindUID;
    private String attributesViewKindName;
    private String attributesViewKindDesc;
    private AttributesView.AttributesViewKindDataForm attributesViewKindDataForm;

    public AttributesViewVO(String attributesViewKindName, String attributesViewKindDesc,
                            AttributesView.AttributesViewKindDataForm attributesViewKindDataForm, String attributesViewKindUID){
        this.attributesViewKindName = attributesViewKindName;
        this.attributesViewKindDesc = attributesViewKindDesc;
        this.attributesViewKindDataForm = attributesViewKindDataForm;
        this.attributesViewKindUID = attributesViewKindUID;
    }

    public AttributesViewVO(){}

    public String getAttributesViewKindUID() {
        return attributesViewKindUID;
    }

    public void setAttributesViewKindUID(String attributesViewKindUID) {
        this.attributesViewKindUID = attributesViewKindUID;
    }

    public String getAttributesViewKindName() {
        return attributesViewKindName;
    }

    public void setAttributesViewKindName(String attributesViewKindName) {
        this.attributesViewKindName = attributesViewKindName;
    }

    public String getAttributesViewKindDesc() {
        return attributesViewKindDesc;
    }

    public void setAttributesViewKindDesc(String attributesViewKindDesc) {
        this.attributesViewKindDesc = attributesViewKindDesc;
    }

    public AttributesView.AttributesViewKindDataForm getAttributesViewKindDataForm() {
        return attributesViewKindDataForm;
    }

    public void setAttributesViewKindDataForm(AttributesView.AttributesViewKindDataForm attributesViewKindDataForm) {
        this.attributesViewKindDataForm = attributesViewKindDataForm;
    }
}
