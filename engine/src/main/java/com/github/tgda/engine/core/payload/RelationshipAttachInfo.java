package com.github.tgda.engine.core.payload;

import com.github.tgda.engine.core.term.Direction;

import java.util.Map;

public class RelationshipAttachInfo {
    private String relationKind;
    private Direction direction;
    private Map<String, Object> relationData;

    public String getRelationKind() {
        return relationKind;
    }

    public void setRelationKind(String relationKind) {
        this.relationKind = relationKind;
    }

    public Direction getDirection() {
        return direction;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public Map<String, Object> getRelationData() {
        return relationData;
    }

    public void setRelationData(Map<String, Object> relationData) {
        this.relationData = relationData;
    }
}
