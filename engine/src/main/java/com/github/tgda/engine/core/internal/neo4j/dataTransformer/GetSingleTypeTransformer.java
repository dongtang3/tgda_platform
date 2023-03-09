package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JTypeImpl;
import com.github.tgda.engine.core.util.Constant;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

public class GetSingleTypeTransformer implements DataTransformer<Type>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String currentCoreRealmName;

    public GetSingleTypeTransformer(String currentCoreRealmName, GraphOperationExecutor workingGraphOperationExecutor){
        this.currentCoreRealmName= currentCoreRealmName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public Type transformResult(Result result) {
        if(result.hasNext()){
            Record nodeRecord = result.next();
            if(nodeRecord != null){
                Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
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
                return neo4JConceptionKindImpl;
            }
        }
       return null;
    }
}
