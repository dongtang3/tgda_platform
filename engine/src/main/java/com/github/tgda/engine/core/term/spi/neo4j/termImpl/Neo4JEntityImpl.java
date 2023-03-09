package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.feature.spi.neo4j.featureImpl.Neo4JAttributesMeasurableImpl;
import com.github.tgda.engine.core.feature.spi.neo4j.featureInf.*;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JEntity;

import java.util.ArrayList;
import java.util.List;

public class Neo4JEntityImpl extends Neo4JAttributesMeasurableImpl implements Neo4JEntity, Neo4JEntityRelationable,
        Neo4JClassificationAttachable, Neo4JMultiConceptionKindsSupportable, Neo4JTimeScaleFeatureSupportable, Neo4JGeospatialScaleFeatureSupportable,
        Neo4JPathTravelable, Neo4JGeospatialScaleCalculable{

    private String conceptionEntityUID;
    private String conceptionKindName;
    private List<String> allConceptionKindNames;

    public Neo4JEntityImpl(String conceptionKindName, String conceptionEntityUID){
        super(conceptionEntityUID);
        this.conceptionKindName = conceptionKindName;
        this.conceptionEntityUID = conceptionEntityUID;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    @Override
    public String getEntityUID() {
        return this.conceptionEntityUID;
    }

    @Override
    public String getConceptionKindName() {
        return this.conceptionKindName;
    }

    @Override
    public List<String> getAllConceptionKindNames() {
        return this.allConceptionKindNames != null ? this.allConceptionKindNames : new ArrayList<>();
    }

    public void setAllConceptionKindNames(List<String> allConceptionKindNames) {
        this.allConceptionKindNames = allConceptionKindNames;
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        super.setGlobalGraphOperationExecutor(graphOperationExecutor);
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }

    @Override
    public String getEntityUID() {
        return conceptionEntityUID;
    }

    @Override
    public GraphOperationExecutorHelper getGraphOperationExecutorHelper() {
        return graphOperationExecutorHelper;
    }
}
