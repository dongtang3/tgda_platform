package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.payload.RelationshipEntityValue;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Relationship;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetListRelationshipEntityValueTransformer implements DataTransformer<List<RelationshipEntityValue>>{

    private List<String> returnedAttributeList;
    private String targetRelationKindName;

    public GetListRelationshipEntityValueTransformer(String targetRelationKindName, List<String> returnedAttributeList){
        this.targetRelationKindName = targetRelationKindName;
        this.returnedAttributeList = returnedAttributeList;
    }

    @Override
    public List<RelationshipEntityValue> transformResult(Result result) {
        List<RelationshipEntityValue> relationshipEntityValueList = new ArrayList<>();
        if(result.hasNext()){
            while(result.hasNext()){
                Record nodeRecord = result.next();
                if(nodeRecord != null){
                    Relationship resultRelationship = nodeRecord.get(CypherBuilder.operationResultName).asRelationship();
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

                        Map<String,Object> entityAttributesValue = new HashMap<>();
                        if(returnedAttributeList != null && returnedAttributeList.size() > 0){
                            for(String currentAttributeName : returnedAttributeList){
                                Value targetValue = resultRelationship.get(currentAttributeName);
                                if(targetValue != null & !(targetValue instanceof org.neo4j.driver.internal.value.NullValue)){
                                    entityAttributesValue.put(currentAttributeName,targetValue);
                                }
                            }
                        }
                        RelationshipEntityValue relationshipEntityValue = new RelationshipEntityValue(relationEntityUID,fromEntityUID,toEntityUID,entityAttributesValue);
                        relationshipEntityValueList.add(relationshipEntityValue);
                    }
                }
            }
        }
        return relationshipEntityValueList;
    }
}
