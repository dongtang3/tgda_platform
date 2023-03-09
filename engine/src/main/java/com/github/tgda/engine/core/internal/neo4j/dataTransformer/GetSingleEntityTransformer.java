package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEntityImpl;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import com.google.common.collect.Lists;

import java.util.List;

public class GetSingleEntityTransformer implements DataTransformer<Entity>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String targetConceptionKindName;

    public GetSingleEntityTransformer(String targetConceptionKindName, GraphOperationExecutor workingGraphOperationExecutor){
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
        this.targetConceptionKindName = targetConceptionKindName;
    }

    @Override
    public Entity transformResult(Result result) {
        if(result.hasNext()){
            Record nodeRecord = result.next();
            if(nodeRecord != null){
                Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                List<String> allConceptionKindNames = Lists.newArrayList(resultNode.labels());
                boolean isMatchedConceptionKind = true;
                if(allConceptionKindNames.size()>0 && targetConceptionKindName != null){
                    isMatchedConceptionKind = allConceptionKindNames.contains(targetConceptionKindName);
                }
                if(isMatchedConceptionKind){
                    long nodeUID = resultNode.id();
                    String conceptionEntityUID = ""+nodeUID;
                    Neo4JEntityImpl neo4jEntityImpl = targetConceptionKindName != null ?
                            new Neo4JEntityImpl(targetConceptionKindName,conceptionEntityUID):
                            new Neo4JEntityImpl(allConceptionKindNames.get(0),conceptionEntityUID);
                    neo4jEntityImpl.setAllConceptionKindNames(allConceptionKindNames);
                    neo4jEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                    return neo4jEntityImpl;
                }else{
                    return null;
                }
            }
        }
        return null;
    }
}
