package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.RelationshipEntity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JRelationshipEntityImpl;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.List;

public class GetSingleRelationshipEntityTransformer implements DataTransformer<RelationshipEntity>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String targetRelationKindName;

    public GetSingleRelationshipEntityTransformer(String targetRelationKindName, GraphOperationExecutor workingGraphOperationExecutor){
        this.targetRelationKindName = targetRelationKindName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public RelationshipEntity transformResult(Result result) {
        if(result.hasNext()){
            Record nodeRecord = result.next();
            if(nodeRecord != null){
                Relationship resultRelationship = nodeRecord.get(CypherBuilder.operationResultName).asRelationship();
                Node sourceNode = nodeRecord.containsKey(CypherBuilder.sourceNodeName) ? nodeRecord.get(CypherBuilder.sourceNodeName).asNode():null;
                Node targetNode = nodeRecord.containsKey(CypherBuilder.targetNodeName) ? nodeRecord.get(CypherBuilder.targetNodeName).asNode():null;
                String relationType = resultRelationship.type();
                boolean isMatchedKind;
                // if the relationEntity is come from a DELETE relation operation,the relationType will be empty string,
                // make isMatchedKind to be true at this case
                if(this.targetRelationKindName == null || relationType.equals("")){
                    isMatchedKind = true;
                }else{
                    isMatchedKind = relationType.equals(targetRelationKindName)? true : false;
                }
                if(isMatchedKind){
                    long relationUID = resultRelationship.id();
                    String relationEntityUID = ""+relationUID;
                    String fromEntityUID = ""+resultRelationship.startNodeId();
                    String toEntityUID = ""+resultRelationship.endNodeId();
                    Neo4JRelationshipEntityImpl neo4jRelationshipEntityImpl =
                            new Neo4JRelationshipEntityImpl(relationType,relationEntityUID,fromEntityUID,toEntityUID);
                    if(sourceNode != null){
                        Iterable<String> sourceNodeLabels = sourceNode.labels();
                        List<String> sourceNodeLabelList = Lists.newArrayList(sourceNodeLabels);
                        String sourceNodeId = ""+sourceNode.id();
                        if(sourceNodeId.equals(fromEntityUID)){
                            neo4jRelationshipEntityImpl.setFromEntityTypeList(sourceNodeLabelList);
                        }else{
                            neo4jRelationshipEntityImpl.setToEntityTypeList(sourceNodeLabelList);
                        }
                    }
                    if(targetNode != null){
                        Iterable<String> targetNodeLabels = targetNode.labels();
                        List<String> targetNodeLabelList = Lists.newArrayList(targetNodeLabels);
                        String targetNodeId = ""+targetNode.id();
                        if(targetNodeId.equals(toEntityUID)){
                            neo4jRelationshipEntityImpl.setToEntityTypeList(targetNodeLabelList);
                        }else{
                            neo4jRelationshipEntityImpl.setFromEntityTypeList(targetNodeLabelList);
                        }
                    }
                    neo4jRelationshipEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                    return neo4jRelationshipEntityImpl;
                }else{
                    return null;
                }
            }
        }
        return null;
    }
}
