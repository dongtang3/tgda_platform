package com.github.tgda.engine.core.payload;

public class AttributeDistributionInfo {

    private String[] kindNames;
    private String[] attributeNames;

    public AttributeDistributionInfo(String[] kindNames, String[] attributeNames){
        this.kindNames = kindNames;
        this.attributeNames = attributeNames;
    }

    public String[] getKindNames() {
        return kindNames;
    }

    public String[] getAttributeNames() {
        return attributeNames;
    }
}
