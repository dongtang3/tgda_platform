package com.github.tgda.engine.core.payload;

import com.github.tgda.engine.core.term.Direction;

public class RuntimeRelationAndConceptionKindAttachInfo {

    private String conceptionKind;
    private String relationKind;
    private Direction direction;
    private long relationEntityCount;

    public RuntimeRelationAndConceptionKindAttachInfo(String relationKind, String conceptionKind,
                                                      Direction direction, long relationEntityCount){
        this.relationKind = relationKind;
        this.conceptionKind = conceptionKind;
        this.direction = direction;
        this.relationEntityCount = relationEntityCount;
    }

    public String getConceptionKind() {
        return conceptionKind;
    }

    public String getRelationKind() {
        return relationKind;
    }

    public Direction getDirection() {
        return direction;
    }

    public long getRelationshipEntityCount() {
        return relationEntityCount;
    }
}
