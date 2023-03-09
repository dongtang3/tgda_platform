package com.github.tgda.engine.core.payload;

import com.github.tgda.engine.core.term.TimeScaleEntity;
import com.github.tgda.engine.core.term.TimeScaleEvent;

public class TimeScaleDataPair {

    private TimeScaleEvent timeScaleEvent;
    private TimeScaleEntity timeScaleEntity;

    public TimeScaleDataPair(TimeScaleEvent timeScaleEvent,TimeScaleEntity timeScaleEntity){
        this.timeScaleEvent = timeScaleEvent;
        this.timeScaleEntity = timeScaleEntity;
    }

    public TimeScaleEvent getTimeScaleEvent() {
        return timeScaleEvent;
    }

    public TimeScaleEntity getTimeScaleEntity() {
        return this.timeScaleEntity;
    }
}
