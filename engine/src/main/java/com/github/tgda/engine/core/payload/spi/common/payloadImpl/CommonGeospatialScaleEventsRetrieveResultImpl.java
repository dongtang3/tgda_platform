package com.github.tgda.engine.core.payload.spi.common.payloadImpl;

import com.github.tgda.engine.core.payload.EntitiesRetrieveStatistics;
import com.github.tgda.engine.core.payload.GeospatialScaleEventsRetrieveResult;
import com.github.tgda.engine.core.term.GeospatialScaleEvent;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CommonGeospatialScaleEventsRetrieveResultImpl implements GeospatialScaleEventsRetrieveResult {

    private List<GeospatialScaleEvent> geospatialScaleEventList;
    private EntitiesRetrieveStatistics entitiesRetrieveStatistics;

    public CommonGeospatialScaleEventsRetrieveResultImpl(){
        this.geospatialScaleEventList = new ArrayList<>();
        this.entitiesRetrieveStatistics = new EntitiesRetrieveStatistics();
        this.entitiesRetrieveStatistics.setStartTime(new Date());
    }

    public void finishEntitiesRetrieving(){
        this.entitiesRetrieveStatistics.setFinishTime(new Date());
    }

    public void addGeospatialScaleEvents(List<GeospatialScaleEvent> geospatialScaleEventList){
        this.geospatialScaleEventList.addAll(geospatialScaleEventList);
    }

    @Override
    public List<GeospatialScaleEvent> getGeospatialScaleEvents() {
        return this.geospatialScaleEventList;
    }

    @Override
    public EntitiesRetrieveStatistics getOperationStatistics() {
        return this.entitiesRetrieveStatistics;
    }
}
