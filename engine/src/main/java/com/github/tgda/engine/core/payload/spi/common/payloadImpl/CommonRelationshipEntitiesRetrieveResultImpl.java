package com.github.tgda.engine.core.payload.spi.common.payloadImpl;

import com.github.tgda.engine.core.payload.EntitiesRetrieveStatistics;
import com.github.tgda.engine.core.payload.RelationshipEntitiesRetrieveResult;
import com.github.tgda.engine.core.term.RelationshipEntity;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CommonRelationshipEntitiesRetrieveResultImpl implements RelationshipEntitiesRetrieveResult {

    private List<RelationshipEntity> relationshipEntityList;
    private EntitiesRetrieveStatistics entitiesRetrieveStatistics;

    public CommonRelationshipEntitiesRetrieveResultImpl(){
        this.relationshipEntityList = new ArrayList<>();
        this.entitiesRetrieveStatistics = new EntitiesRetrieveStatistics();
        this.entitiesRetrieveStatistics.setStartTime(new Date());
    }

    public void finishEntitiesRetrieving(){
        this.entitiesRetrieveStatistics.setFinishTime(new Date());
    }

    public void addRelationEntities(List<RelationshipEntity> relationshipEntityList){
        this.relationshipEntityList.addAll(relationshipEntityList);
    }

    @Override
    public List<RelationshipEntity> getRelationEntities() {
        return this.relationshipEntityList;
    }

    @Override
    public EntitiesRetrieveStatistics getOperationStatistics() {
        return this.entitiesRetrieveStatistics;
    }
}
