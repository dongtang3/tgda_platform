package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEntityImpl;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.payload.dataScienceAnalyzeResult.PathFindingResult;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetListPathFindingResultTransformer implements DataTransformer<List<PathFindingResult>>{

    private GraphOperationExecutor workingGraphOperationExecutor;

    public GetListPathFindingResultTransformer(GraphOperationExecutor workingGraphOperationExecutor){
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public List<PathFindingResult> transformResult(Result result) {
        List<PathFindingResult> pathFindingResultList = new ArrayList<>();
        while(result.hasNext()){
            Record nodeRecord = result.next();

            String sourceEntityUID = ""+nodeRecord.get("sourceEntityUID").asLong();
            String targetEntityUID = ""+nodeRecord.get("targetEntityUID").asLong();
            double totalCost = nodeRecord.get("totalCost").asNumber().doubleValue();

            Node sourceEntity = nodeRecord.get("sourceEntity").asNode();
            Node targetEntity = nodeRecord.get("targetEntity").asNode();
            List<Object> nodeIdsList = nodeRecord.get("nodeIds").asList();
            List<Object> costsList = nodeRecord.get("costs").asList();

            String sourceEntityKind = sourceEntity.labels().iterator().next();
            String targetEntityKind = targetEntity.labels().iterator().next();
            Map<String,Double> pathEntityTraversalWeightMap = new HashMap<>();
            List<String> pathEntityUIDs = new ArrayList<>();

            for(int i=0; i< nodeIdsList.size(); i++){
                String entityId = nodeIdsList.get(i).toString();
                Double entityCost = (Double) costsList.get(i);
                pathEntityTraversalWeightMap.put(entityId,entityCost);
                pathEntityUIDs.add(entityId);
            }

            List<Entity> pathConceptionEntities = new ArrayList<>();
            List<Object> pathNodes = nodeRecord.get("path").asList();
            for(Object currentNode:pathNodes){
                Node currentEntityNode = (Node)currentNode;
                long nodeUID = currentEntityNode.id();
                List<String> allConceptionKindNames = Lists.newArrayList(currentEntityNode.labels());
                String conceptionEntityUID = ""+nodeUID;
                Neo4JEntityImpl neo4jEntityImpl =
                        new Neo4JEntityImpl(allConceptionKindNames.get(0),conceptionEntityUID);
                neo4jEntityImpl.setAllConceptionKindNames(allConceptionKindNames);
                neo4jEntityImpl.setGlobalGraphOperationExecutor(this.workingGraphOperationExecutor);
                pathConceptionEntities.add(neo4jEntityImpl);
            }

            PathFindingResult currentPathFindingResult = new PathFindingResult(
                    sourceEntityUID,sourceEntityKind,targetEntityUID,targetEntityKind,totalCost,
                    pathEntityUIDs,pathEntityTraversalWeightMap,pathConceptionEntities
            );

            pathFindingResultList.add(currentPathFindingResult);
        }
        return pathFindingResultList;
    }
}
