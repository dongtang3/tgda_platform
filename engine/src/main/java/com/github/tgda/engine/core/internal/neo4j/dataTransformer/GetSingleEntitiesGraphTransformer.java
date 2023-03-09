package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.structure.EntitiesGraph;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.RelationshipEntity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEntityImpl;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JRelationshipEntityImpl;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.ArrayList;
import java.util.List;

public class GetSingleEntitiesGraphTransformer implements DataTransformer<EntitiesGraph>{

    private GraphOperationExecutor workingGraphOperationExecutor;

    public GetSingleEntitiesGraphTransformer(GraphOperationExecutor workingGraphOperationExecutor){
        this.workingGraphOperationExecutor =  workingGraphOperationExecutor;
    }

    @Override
    public EntitiesGraph transformResult(Result result) {
        if(result.hasNext()){
            Record currentRecord = result.next();
            List<Object> nodeObjectList =  currentRecord.get("nodes").asList();
            List<Object> relationObjectList =  currentRecord.get("relationships").asList();

            List<Entity> graphConceptionEntities = new ArrayList<>();
            List<RelationshipEntity> graphRelationEntities = new ArrayList<>();
            EntitiesGraph entitiesGraph = new EntitiesGraph(graphConceptionEntities,graphRelationEntities);

            if(nodeObjectList != null){
                for(Object currentNodeObject:nodeObjectList){
                    Node currentNode = (Node)currentNodeObject;
                    List<String> allConceptionKindNames = Lists.newArrayList(currentNode.labels());
                    long nodeUID = currentNode.id();
                    String conceptionEntityUID = ""+nodeUID;
                    Neo4JEntityImpl neo4jEntityImpl =
                            new Neo4JEntityImpl(allConceptionKindNames.get(0),conceptionEntityUID);
                    neo4jEntityImpl.setAllConceptionKindNames(allConceptionKindNames);
                    neo4jEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                    graphConceptionEntities.add(neo4jEntityImpl);
                    entitiesGraph.countConceptionKindsData(allConceptionKindNames.get(0));
                }
            }
            if(relationObjectList != null){
                for(Object currentRelationObject:relationObjectList){
                    Relationship resultRelationship = (Relationship)currentRelationObject;
                    String relationType = resultRelationship.type();
                    long relationUID = resultRelationship.id();
                    String relationEntityUID = ""+relationUID;
                    String fromEntityUID = ""+resultRelationship.startNodeId();
                    String toEntityUID = ""+resultRelationship.endNodeId();
                    Neo4JRelationshipEntityImpl neo4jRelationshipEntityImpl =
                            new Neo4JRelationshipEntityImpl(relationType,relationEntityUID,fromEntityUID,toEntityUID);
                    neo4jRelationshipEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                    graphRelationEntities.add(neo4jRelationshipEntityImpl);
                    entitiesGraph.countRelationKindsData(relationType);
                }
            }
            return entitiesGraph;
        }
        return null;
    }
}
