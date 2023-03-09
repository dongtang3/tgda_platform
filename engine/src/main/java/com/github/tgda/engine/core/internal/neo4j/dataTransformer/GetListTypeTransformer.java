package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JTypeImpl;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.util.Constant;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.List;

public class GetListTypeTransformer implements DataTransformer<List<Type>>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String currentCoreRealmName;

    public GetListTypeTransformer(String currentCoreRealmName, GraphOperationExecutor workingGraphOperationExecutor){
        this.currentCoreRealmName= currentCoreRealmName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public List<Type> transformResult(Result result) {
        List<Type> typeList = new ArrayList<>();
        if(result.hasNext()){
            while(result.hasNext()){
                Record nodeRecord = result.next();
                if(nodeRecord != null){
                    Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                    List<String> allLabelNames = Lists.newArrayList(resultNode.labels());
                    boolean isMatchedKind = true;
                    if(allLabelNames.size()>0){
                        isMatchedKind = allLabelNames.contains(Constant.ConceptionKindClass);
                    }
                    if(isMatchedKind){
                        long nodeUID = resultNode.id();
                        String coreRealmName = this.currentCoreRealmName;
                        String conceptionKindName = resultNode.get(Constant._NameProperty).asString();
                        String conceptionKindDesc = null;
                        if(resultNode.get(Constant._DescProperty) != null){
                            conceptionKindDesc = resultNode.get(Constant._DescProperty).asString();
                        }
                        String conceptionKindUID = ""+nodeUID;
                        Neo4JTypeImpl neo4JConceptionKindImpl =
                                new Neo4JTypeImpl(coreRealmName,conceptionKindName,conceptionKindDesc,conceptionKindUID);
                        neo4JConceptionKindImpl.setGlobalGraphOperationExecutor(this.workingGraphOperationExecutor);
                        typeList.add(neo4JConceptionKindImpl);
                    }
                }
            }
        }
        return typeList;
    }
}
