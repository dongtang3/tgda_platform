package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.term.RelationshipType;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JRelationshipImplType;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.util.Constant;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.List;

public class GetListRelationshipTypeTransformer implements DataTransformer<List<RelationshipType>>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String currentCoreRealmName;

    public GetListRelationshipTypeTransformer(String currentCoreRealmName, GraphOperationExecutor workingGraphOperationExecutor){
        this.currentCoreRealmName= currentCoreRealmName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public List<RelationshipType> transformResult(Result result) {
        List<RelationshipType> relationshipTypeList = new ArrayList<>();
        if(result.hasNext()){
            while(result.hasNext()){
                Record nodeRecord = result.next();
                if(nodeRecord != null){
                    Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                    List<String> allLabelNames = Lists.newArrayList(resultNode.labels());
                    boolean isMatchedKind = true;
                    if(allLabelNames.size()>0){
                        isMatchedKind = allLabelNames.contains(Constant.RelationKindClass);
                    }
                    if(isMatchedKind){
                        long nodeUID = resultNode.id();
                        String coreRealmName = this.currentCoreRealmName;
                        String relationKindName = resultNode.get(Constant._NameProperty).asString();
                        String relationKindNameDesc = null;
                        if(resultNode.get(Constant._DescProperty) != null){
                            relationKindNameDesc = resultNode.get(Constant._DescProperty).asString();
                        }
                        String relationKindUID = ""+nodeUID;
                        Neo4JRelationshipImplType neo4JRelationKindImpl =
                                new Neo4JRelationshipImplType(coreRealmName,relationKindName,relationKindNameDesc,relationKindUID);
                        neo4JRelationKindImpl.setGlobalGraphOperationExecutor(this.workingGraphOperationExecutor);
                        relationshipTypeList.add(neo4JRelationKindImpl);
                    }
                }
            }
        }
        return relationshipTypeList;
    }
}
