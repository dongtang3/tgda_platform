package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEntityImpl;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.List;

public class GetListEntityTransformer implements DataTransformer<List<Entity>>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String targetConceptionKindName;

    public GetListEntityTransformer(String targetConceptionKindName, GraphOperationExecutor workingGraphOperationExecutor){
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
        this.targetConceptionKindName = targetConceptionKindName;
    }

    @Override
    public List<Entity> transformResult(Result result) {
        List<Entity> entityList = new ArrayList<>();
        while(result.hasNext()){
            Record nodeRecord = result.next();
            Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
            List<String> allConceptionKindNames = Lists.newArrayList(resultNode.labels());
            boolean isMatchedConceptionKind = true;
            if(allConceptionKindNames.size()>0){
                if(targetConceptionKindName != null){
                    isMatchedConceptionKind = allConceptionKindNames.contains(targetConceptionKindName);
                }else{
                    isMatchedConceptionKind = true;
                }
            }
            if(isMatchedConceptionKind){
                long nodeUID = resultNode.id();
                String conceptionEntityUID = ""+nodeUID;
                String resultConceptionKindName = targetConceptionKindName != null? targetConceptionKindName:allConceptionKindNames.get(0);
                Neo4JEntityImpl neo4jEntityImpl =
                        new Neo4JEntityImpl(resultConceptionKindName,conceptionEntityUID);
                neo4jEntityImpl.setAllConceptionKindNames(allConceptionKindNames);
                neo4jEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                entityList.add(neo4jEntityImpl);
            }
        }
        return entityList;
    }
}
