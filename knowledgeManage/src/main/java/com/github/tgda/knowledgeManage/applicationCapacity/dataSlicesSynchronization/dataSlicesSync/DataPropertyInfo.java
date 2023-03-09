package com.github.tgda.knowledgeManage.applicationCapacity.dataSlicesSynchronization.dataSlicesSync;

import com.github.tgda.compute.applicationCapacity.compute.dataComputeUnit.dataService.DataSlicePropertyType;

public class DataPropertyInfo {

    private String propertyName;
    private DataSlicePropertyType propertyType;

    public DataPropertyInfo(String propertyName,DataSlicePropertyType propertyType){
        this.propertyName = propertyName;
        this.propertyType = propertyType;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public DataSlicePropertyType getPropertyType() {
        return propertyType;
    }
}
