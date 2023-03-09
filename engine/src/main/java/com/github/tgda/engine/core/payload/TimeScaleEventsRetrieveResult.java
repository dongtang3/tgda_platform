package com.github.tgda.engine.core.payload;

import com.github.tgda.engine.core.term.TimeScaleEvent;

import java.util.List;

public interface TimeScaleEventsRetrieveResult {
    public List<TimeScaleEvent> getTimeScaleEvents();
    public EntitiesRetrieveStatistics getOperationStatistics();
}
