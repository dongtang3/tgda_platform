package com.github.tgda.engine.core.payload.spi.common.payloadImpl;

import com.github.tgda.engine.core.payload.EntitiesRetrieveStatistics;
import com.github.tgda.engine.core.payload.RelationEntitiesAttributesRetrieveResult;
import com.github.tgda.engine.core.payload.RelationshipEntityValue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CommonRelationEntitiesAttributesRetrieveResultImpl implements RelationEntitiesAttributesRetrieveResult {

    private List<RelationshipEntityValue> relationshipEntityValueList;
    private EntitiesRetrieveStatistics entitiesRetrieveStatistics;

    public CommonRelationEntitiesAttributesRetrieveResultImpl(){
        this.relationshipEntityValueList = new ArrayList<>();
        this.entitiesRetrieveStatistics = new EntitiesRetrieveStatistics();
        this.entitiesRetrieveStatistics.setStartTime(new Date());
    }

    public void finishEntitiesRetrieving(){
        this.entitiesRetrieveStatistics.setFinishTime(new Date());
    }

    public void addRelationEntitiesAttributes(List<RelationshipEntityValue> relationshipEntityValueList){
        this.relationshipEntityValueList.addAll(relationshipEntityValueList);
    }

    @Override
    public List<RelationshipEntityValue> getRelationshipEntityValues() {
        return relationshipEntityValueList;
    }

    @Override
    public EntitiesRetrieveStatistics getOperationStatistics() {
        return entitiesRetrieveStatistics;
    }
}
