package com.github.tgda.engine.core.payload.spi.common.payloadImpl;

import com.github.tgda.engine.core.payload.EntitiesOperationResult;
import com.github.tgda.engine.core.payload.EntitiesOperationStatistics;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CommonEntitiesOperationResultImpl implements EntitiesOperationResult {

    private List<String> successEntityUIDs;
    private EntitiesOperationStatistics entitiesOperationStatistics;

    public CommonEntitiesOperationResultImpl(){
        this.successEntityUIDs = new ArrayList<>();
        this.entitiesOperationStatistics = new EntitiesOperationStatistics();
        this.entitiesOperationStatistics.setStartTime(new Date());
    }

    public void finishEntitiesOperation(){
        this.entitiesOperationStatistics.setFinishTime(new Date());
    }

    @Override
    public List<String> getSuccessEntityUIDs(){
        return successEntityUIDs;
    }

    @Override
    public EntitiesOperationStatistics getOperationStatistics() {
        return entitiesOperationStatistics;
    }
}
