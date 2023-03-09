package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.term.RelationshipEntity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JRelationshipEntityImpl;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;

import java.util.ArrayList;
import java.util.List;

public class GetListRelationshipEntityTransformer implements DataTransformer<List<RelationshipEntity>>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String targetRelationKindName;
    private boolean isDistinctMode;

    public GetListRelationshipEntityTransformer(String targetRelationKindName, GraphOperationExecutor workingGraphOperationExecutor, boolean isDistinctMode){
        this.targetRelationKindName = targetRelationKindName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
        this.isDistinctMode = isDistinctMode;
    }

    @Override
    public List<RelationshipEntity> transformResult(Result result) {
        List<RelationshipEntity> relationshipEntityList = new ArrayList<>();
        List<String> alreadyExistRelationshipEntityUIDList = this.isDistinctMode ? new ArrayList<>(): null;
        if(result.hasNext()){
            while(result.hasNext()){
                Record nodeRecord = result.next();
                if(nodeRecord != null){
                    if(nodeRecord.containsKey(CypherBuilder.operationResultName) && !nodeRecord.get(CypherBuilder.operationResultName).isNull()){
                        Relationship resultRelationship = nodeRecord.get(CypherBuilder.operationResultName).asRelationship();
                        Node sourceNode = nodeRecord.containsKey(CypherBuilder.sourceNodeName) ? nodeRecord.get(CypherBuilder.sourceNodeName).asNode():null;
                        Node targetNode = nodeRecord.containsKey(CypherBuilder.targetNodeName) ? nodeRecord.get(CypherBuilder.targetNodeName).asNode():null;
                        String relationType = resultRelationship.type();
                        boolean isMatchedKind;
                        if(this.targetRelationKindName == null){
                            isMatchedKind = true;
                        }else{
                            isMatchedKind = relationType.equals(targetRelationKindName)? true : false;
                        }
                        if(isMatchedKind){
                            long relationUID = resultRelationship.id();
                            String relationEntityUID = ""+relationUID;
                            String fromEntityUID = ""+resultRelationship.startNodeId();
                            String toEntityUID = ""+resultRelationship.endNodeId();
                            if(alreadyExistRelationshipEntityUIDList != null){
                                if(!alreadyExistRelationshipEntityUIDList.contains(relationEntityUID)){
                                    Neo4JRelationshipEntityImpl neo4jRelationshipEntityImpl =
                                            new Neo4JRelationshipEntityImpl(relationType,relationEntityUID,fromEntityUID,toEntityUID);
                                    neo4jRelationshipEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
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
                                    relationshipEntityList.add(neo4jRelationshipEntityImpl);
                                    alreadyExistRelationshipEntityUIDList.add(relationEntityUID);
                                }
                            }else{
                                Neo4JRelationshipEntityImpl neo4jRelationshipEntityImpl =
                                        new Neo4JRelationshipEntityImpl(relationType,relationEntityUID,fromEntityUID,toEntityUID);
                                neo4jRelationshipEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
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
                                relationshipEntityList.add(neo4jRelationshipEntityImpl);
                            }
                        }
                    }
                }
            }
        }
        return relationshipEntityList;
    }
}
