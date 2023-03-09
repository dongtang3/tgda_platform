package com.github.tgda.engine.core.payload;

import com.github.tgda.engine.core.term.GeospatialScaleEvent;

import java.util.List;

public interface GeospatialScaleEventsRetrieveResult {
    public List<GeospatialScaleEvent> getGeospatialScaleEvents();
    public EntitiesRetrieveStatistics getOperationStatistics();
}
