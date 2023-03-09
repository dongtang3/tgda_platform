package com.github.tgda.engine.core.payload;

import com.github.tgda.engine.core.term.GeospatialScaleEntity;
import com.github.tgda.engine.core.term.GeospatialScaleEvent;

public class GeospatialScaleDataPair {

    private GeospatialScaleEvent geospatialScaleEvent;
    private GeospatialScaleEntity geospatialScaleEntity;

    public GeospatialScaleDataPair(GeospatialScaleEvent geospatialScaleEvent,GeospatialScaleEntity geospatialScaleEntity){
        this.geospatialScaleEvent = geospatialScaleEvent;
        this.geospatialScaleEntity = geospatialScaleEntity;
    }

    public GeospatialScaleEvent getGeospatialScaleEvent() {
        return geospatialScaleEvent;
    }

    public GeospatialScaleEntity getGeospatialScaleEntity() {
        return geospatialScaleEntity;
    }
}
