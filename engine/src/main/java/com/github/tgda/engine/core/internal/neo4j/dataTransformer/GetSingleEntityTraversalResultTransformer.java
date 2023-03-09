package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.payload.dataScienceAnalyzeResult.EntityTraversalResult;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEntityImpl;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;

import java.util.ArrayList;
import java.util.List;

public class GetSingleEntityTraversalResultTransformer implements DataTransformer<EntityTraversalResult>{

    private GraphOperationExecutor workingGraphOperationExecutor;

    public GetSingleEntityTraversalResultTransformer(GraphOperationExecutor workingGraphOperationExecutor){
        this.workingGraphOperationExecutor =  workingGraphOperationExecutor;
    }

    @Override
    public EntityTraversalResult transformResult(Result result) {
        while(result.hasNext()){
            Record nodeRecord = result.next();
            long sourceNodeId = nodeRecord.get("startNodeId").asLong();
            List targetNodeIds = nodeRecord.get("nodeIds").asList();
            Path resultPath = nodeRecord.get("path").asPath();

            String sourceNodeUID = ""+sourceNodeId;
            List<String> entityTraversalFootprints = new ArrayList<>();
            for(Object currentId:targetNodeIds){
                entityTraversalFootprints.add(""+currentId);
            }
            List<Entity> entityList = new ArrayList<>();

            Iterable<Node> entityNode = resultPath.nodes();
            for(Node currentEntityNode:entityNode){
                long nodeUID = currentEntityNode.id();
                List<String> allConceptionKindNames = Lists.newArrayList(currentEntityNode.labels());
                String conceptionEntityUID = ""+nodeUID;
                Neo4JEntityImpl neo4jEntityImpl =
                        new Neo4JEntityImpl(allConceptionKindNames.get(0),conceptionEntityUID);
                neo4jEntityImpl.setAllConceptionKindNames(allConceptionKindNames);
                neo4jEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                entityList.add(neo4jEntityImpl);
            }
            EntityTraversalResult entityTraversalResult =
                    new EntityTraversalResult(sourceNodeUID,entityTraversalFootprints, entityList);
            return entityTraversalResult;
        }
        return null;
    }
}
