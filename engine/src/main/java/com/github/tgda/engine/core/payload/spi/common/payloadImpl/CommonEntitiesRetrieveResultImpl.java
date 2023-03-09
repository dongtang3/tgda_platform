package com.github.tgda.engine.core.payload.spi.common.payloadImpl;

import com.github.tgda.engine.core.payload.EntitiesRetrieveResult;
import com.github.tgda.engine.core.payload.EntitiesRetrieveStatistics;
import com.github.tgda.engine.core.term.Entity;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CommonEntitiesRetrieveResultImpl implements EntitiesRetrieveResult {

    private List<Entity> entityList;
    private EntitiesRetrieveStatistics entitiesRetrieveStatistics;

    public CommonEntitiesRetrieveResultImpl(){
        this.entityList = new ArrayList<>();
        this.entitiesRetrieveStatistics = new EntitiesRetrieveStatistics();
        this.entitiesRetrieveStatistics.setStartTime(new Date());
    }

    public void finishEntitiesRetrieving(){
        this.entitiesRetrieveStatistics.setFinishTime(new Date());
    }

    public void addConceptionEntities(List<Entity> entityList){
        this.entityList.addAll(entityList);
    }

    @Override
    public List<Entity> getConceptionEntities() {
        return this.entityList;
    }

    @Override
    public EntitiesRetrieveStatistics getOperationStatistics() {
        return this.entitiesRetrieveStatistics;
    }
}
