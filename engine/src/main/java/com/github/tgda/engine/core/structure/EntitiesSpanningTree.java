package com.github.tgda.engine.core.structure;

import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.RelationshipEntity;

import java.util.List;

public class EntitiesSpanningTree {

    private String rootEntityType;
    private String rootEntityUID;
    private List<Entity> treeConceptionEntities;
    private List<RelationshipEntity> treeRelationEntities;

    public EntitiesSpanningTree(String rootEntityType, String rootEntityUID,
                                List<Entity> treeConceptionEntities, List<RelationshipEntity> treeRelationEntities){
        this.rootEntityType = rootEntityType;
        this.rootEntityUID = rootEntityUID;
        this.treeConceptionEntities = treeConceptionEntities;
        this.treeRelationEntities = treeRelationEntities;
    }

    public String getRootEntityType() {
        return rootEntityType;
    }

    public String getRootEntityUID() {
        return rootEntityUID;
    }

    public List<Entity> getTreeConceptionEntities() {
        return treeConceptionEntities;
    }

    public List<RelationshipEntity> getTreeRelationEntities() {
        return treeRelationEntities;
    }
}