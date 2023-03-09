package com.github.tgda.engine.core.analysis.query;

import com.github.tgda.engine.core.term.Direction;

public class RelationKindMatchLogic implements EntityKindMatchLogic{

    private String relationKindName;
    private Direction direction;

    public RelationKindMatchLogic(String relationKindName, Direction direction){
        this.relationKindName = relationKindName;
        this.direction = direction != null ? direction : Direction.TWO_WAY;
    }

    public String getRelationKindName() {
        return relationKindName;
    }

    public Direction getDirection() {
        return direction;
    }
}
