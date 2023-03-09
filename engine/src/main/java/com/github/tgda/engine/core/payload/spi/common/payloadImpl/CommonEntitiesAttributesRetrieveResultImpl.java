package com.github.tgda.engine.core.payload.spi.common.payloadImpl;

import com.github.tgda.engine.core.payload.EntitiesAttributesRetrieveResult;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.EntitiesRetrieveStatistics;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CommonEntitiesAttributesRetrieveResultImpl implements EntitiesAttributesRetrieveResult {

    private List<EntityValue> entityValueList;
    private EntitiesRetrieveStatistics entitiesRetrieveStatistics;

    public CommonEntitiesAttributesRetrieveResultImpl(){
        this.entityValueList = new ArrayList<>();
        this.entitiesRetrieveStatistics = new EntitiesRetrieveStatistics();
        this.entitiesRetrieveStatistics.setStartTime(new Date());
    }

    public void finishEntitiesRetrieving(){
        this.entitiesRetrieveStatistics.setFinishTime(new Date());
    }

    public void addConceptionEntitiesAttributes(List<EntityValue> entityValueList){
        this.entityValueList.addAll(entityValueList);
    }

    @Override
    public List<EntityValue> getEntityValues() {
        return entityValueList;
    }

    @Override
    public EntitiesRetrieveStatistics getOperationStatistics() {
        return entitiesRetrieveStatistics;
    }
}
