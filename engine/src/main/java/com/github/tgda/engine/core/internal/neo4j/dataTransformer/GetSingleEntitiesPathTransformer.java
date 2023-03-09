package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.structure.EntitiesPath;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEntityImpl;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JRelationshipEntityImpl;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.RelationshipEntity;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class GetSingleEntitiesPathTransformer implements DataTransformer<EntitiesPath>{

    private GraphOperationExecutor workingGraphOperationExecutor;

    public GetSingleEntitiesPathTransformer(GraphOperationExecutor workingGraphOperationExecutor){
        this.workingGraphOperationExecutor =  workingGraphOperationExecutor;
    }

    @Override
    public EntitiesPath transformResult(Result result) {
        if(result.hasNext()){
            Record currentRecord = result.next();
            org.neo4j.driver.types.Path currentPath = currentRecord.get("path").asPath();
            String startEntityType = currentPath.start().labels().iterator().next();
            String startEntityUID = ""+currentPath.start().id();
            String endEntityType = currentPath.end().labels().iterator().next();
            String endEntityUID = ""+currentPath.end().id();
            int pathJumps = currentPath.length();
            LinkedList<Entity> pathConceptionEntities = new LinkedList<>();
            LinkedList<RelationshipEntity> pathRelationEntities = new LinkedList<>();

            EntitiesPath currentEntitiesPath = new EntitiesPath(startEntityType,startEntityUID,
                    endEntityType,endEntityUID,pathJumps,pathConceptionEntities,pathRelationEntities);
            Iterator<Node> nodeIterator = currentPath.nodes().iterator();
            while(nodeIterator.hasNext()){
                Node currentNode = nodeIterator.next();
                List<String> allConceptionKindNames = Lists.newArrayList(currentNode.labels());
                long nodeUID = currentNode.id();
                String conceptionEntityUID = ""+nodeUID;
                Neo4JEntityImpl neo4jEntityImpl =
                        new Neo4JEntityImpl(allConceptionKindNames.get(0),conceptionEntityUID);
                neo4jEntityImpl.setAllConceptionKindNames(allConceptionKindNames);
                neo4jEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                pathConceptionEntities.add(neo4jEntityImpl);
            }

            Iterator<Relationship> relationIterator = currentPath.relationships().iterator();
            while(relationIterator.hasNext()){
                Relationship resultRelationship = relationIterator.next();
                String relationType = resultRelationship.type();
                long relationUID = resultRelationship.id();
                String relationEntityUID = ""+relationUID;
                String fromEntityUID = ""+resultRelationship.startNodeId();
                String toEntityUID = ""+resultRelationship.endNodeId();
                Neo4JRelationshipEntityImpl neo4jRelationshipEntityImpl =
                        new Neo4JRelationshipEntityImpl(relationType,relationEntityUID,fromEntityUID,toEntityUID);
                neo4jRelationshipEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                pathRelationEntities.add(neo4jRelationshipEntityImpl);
            }
            return currentEntitiesPath;
        }
        return null;
    }
}
