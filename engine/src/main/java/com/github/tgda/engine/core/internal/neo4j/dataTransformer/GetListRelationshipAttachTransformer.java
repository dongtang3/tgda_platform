package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.term.RelationshipAttach;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JRelationshipAttachImpl;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.util.Constant;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.List;

public class GetListRelationshipAttachTransformer implements DataTransformer<List<RelationshipAttach>>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String currentCoreRealmName;

    public GetListRelationshipAttachTransformer(String currentCoreRealmName, GraphOperationExecutor workingGraphOperationExecutor){
        this.currentCoreRealmName= currentCoreRealmName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public List<RelationshipAttach> transformResult(Result result) {
        List<RelationshipAttach> relationshipAttachKindsList = new ArrayList<>();
        if(result.hasNext()){
            while(result.hasNext()){
                Record nodeRecord = result.next();
                if(nodeRecord != null){
                    Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                    List<String> allLabelNames = Lists.newArrayList(resultNode.labels());
                    boolean isMatchedKind = true;
                    if(allLabelNames.size()>0){
                        isMatchedKind = allLabelNames.contains(Constant.RelationAttachKindClass);
                    }
                    if(isMatchedKind){
                        long nodeUID = resultNode.id();
                        String coreRealmName = this.currentCoreRealmName;
                        String relationAttachKindName = resultNode.get(Constant._NameProperty).asString();
                        String relationAttachKindNameDesc = null;
                        if(resultNode.get(Constant._DescProperty) != null){
                            relationAttachKindNameDesc = resultNode.get(Constant._DescProperty).asString();
                        }

                        String relationAttachSourceKind = resultNode.get(Constant._relationAttachSourceKind).asString();
                        String relationAttachTargetKind = resultNode.get(Constant._relationAttachTargetKind).asString();
                        String relationAttachRelationKind = resultNode.get(Constant._relationAttachRelationKind).asString();
                        boolean relationAttachRepeatableRelationKind = resultNode.get(Constant._relationAttachRepeatableRelationKind).asBoolean();

                        String relationAttachKindUID = ""+nodeUID;
                        Neo4JRelationshipAttachImpl neo4JRelationAttachKindImpl =
                                new Neo4JRelationshipAttachImpl(coreRealmName,relationAttachKindName,relationAttachKindNameDesc,relationAttachKindUID,
                                        relationAttachSourceKind,relationAttachTargetKind,relationAttachRelationKind,relationAttachRepeatableRelationKind);
                        neo4JRelationAttachKindImpl.setGlobalGraphOperationExecutor(this.workingGraphOperationExecutor);
                        relationshipAttachKindsList.add(neo4JRelationAttachKindImpl);
                    }
                }
            }
        }
        return relationshipAttachKindsList;
    }
}
