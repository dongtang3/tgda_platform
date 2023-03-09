package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.feature.spi.neo4j.featureImpl.Neo4JAttributesMeasurableImpl;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JRelationshipEntity;

import java.util.List;

public class Neo4JRelationshipEntityImpl extends Neo4JAttributesMeasurableImpl implements Neo4JRelationshipEntity {

    private String relationEntityUID;
    private String relationTypeName;
    private String fromEntityUID;
    private String toEntityUID;
    private List<String> fromEntityTypeList;
    private List<String> toEntityTypeList;
    public Neo4JRelationshipEntityImpl(String relationTypeName, String relationEntityUID, String fromEntityUID, String toEntityUID){
        super(relationEntityUID,true);
        this.relationTypeName = relationTypeName;
        this.relationEntityUID = relationEntityUID;
        this.fromEntityUID = fromEntityUID;
        this.toEntityUID = toEntityUID;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    @Override
    public String getRelationshipEntityUID() {
        return relationEntityUID;
    }

    @Override
    public String getRelationTypeName() {
        return relationTypeName;
    }

    @Override
    public String getFromEntityUID() {
        return fromEntityUID;
    }

    @Override
    public String getToEntityUID() {
        return toEntityUID;
    }

    @Override
    public List<String> getFromEntityKinds() {
        return fromEntityTypeList;
    }

    @Override
    public List<String> getToEntityKinds() {
        return toEntityTypeList;
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        super.setGlobalGraphOperationExecutor(graphOperationExecutor);
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }

    public void setFromEntityTypeList(List<String> fromEntityTypeList) {
        this.fromEntityTypeList = fromEntityTypeList;
    }

    public void setToEntityTypeList(List<String> toEntityTypeList) {
        this.toEntityTypeList = toEntityTypeList;
    }
}
