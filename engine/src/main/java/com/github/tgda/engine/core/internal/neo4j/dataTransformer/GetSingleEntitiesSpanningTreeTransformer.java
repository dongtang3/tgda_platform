package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.structure.EntitiesSpanningTree;
import com.github.tgda.engine.core.term.Entity;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.RelationshipEntity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEntityImpl;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JRelationshipEntityImpl;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class GetSingleEntitiesSpanningTreeTransformer implements DataTransformer<EntitiesSpanningTree>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String rootEntityType;
    private String rootEntityUID;

    public GetSingleEntitiesSpanningTreeTransformer(String rootEntityUID,GraphOperationExecutor workingGraphOperationExecutor){
        this.rootEntityUID = rootEntityUID;
        this.workingGraphOperationExecutor =  workingGraphOperationExecutor;
    }

    @Override
    public EntitiesSpanningTree transformResult(Result result) {
        List<Entity> treeConceptionEntities = new ArrayList<>();
        List<RelationshipEntity> treeRelationEntities = new ArrayList<>();
        List<String> treeEntityUIDList = new ArrayList<>();
        List<String> treeRelationshipEntityUIDList = new ArrayList<>();

        while(result.hasNext()){
            Record currentRecord = result.next();
            org.neo4j.driver.types.Path currentPath = currentRecord.get("path").asPath();
            String startEntityType = currentPath.start().labels().iterator().next();
            String startEntityUID = ""+currentPath.start().id();

            if(startEntityUID.equals(this.rootEntityUID)){
                this.rootEntityType = startEntityType;
            }

            Iterator<Node> nodeIterator = currentPath.nodes().iterator();
            while(nodeIterator.hasNext()){
                Node currentNode = nodeIterator.next();
                List<String> allConceptionKindNames = Lists.newArrayList(currentNode.labels());
                long nodeUID = currentNode.id();
                String conceptionEntityUID = ""+nodeUID;
                if(!treeEntityUIDList.contains(conceptionEntityUID)){
                    treeEntityUIDList.add(conceptionEntityUID);
                    Neo4JEntityImpl neo4jEntityImpl =
                            new Neo4JEntityImpl(allConceptionKindNames.get(0),conceptionEntityUID);
                    neo4jEntityImpl.setAllConceptionKindNames(allConceptionKindNames);
                    neo4jEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                    treeConceptionEntities.add(neo4jEntityImpl);
                }
            }

            Iterator<Relationship> relationIterator = currentPath.relationships().iterator();
            while(relationIterator.hasNext()){
                Relationship resultRelationship = relationIterator.next();
                String relationType = resultRelationship.type();
                long relationUID = resultRelationship.id();
                String relationEntityUID = ""+relationUID;
                if(!treeRelationshipEntityUIDList.contains(relationEntityUID)){
                    treeRelationshipEntityUIDList.add(relationEntityUID);
                    String fromEntityUID = ""+resultRelationship.startNodeId();
                    String toEntityUID = ""+resultRelationship.endNodeId();
                    Neo4JRelationshipEntityImpl neo4jRelationshipEntityImpl =
                            new Neo4JRelationshipEntityImpl(relationType,relationEntityUID,fromEntityUID,toEntityUID);
                    neo4jRelationshipEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                    treeRelationEntities.add(neo4jRelationshipEntityImpl);
                }
            }
        }

        treeEntityUIDList.clear();
        treeRelationshipEntityUIDList.clear();

        EntitiesSpanningTree entitiesSpanningTree = new EntitiesSpanningTree(this.rootEntityType,this.rootEntityUID,
                treeConceptionEntities,treeRelationEntities);
        return entitiesSpanningTree;
    }
}