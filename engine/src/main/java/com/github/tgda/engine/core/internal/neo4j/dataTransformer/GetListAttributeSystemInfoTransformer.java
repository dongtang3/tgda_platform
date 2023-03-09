package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.payload.AttributeSystemInfo;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;

import java.util.ArrayList;
import java.util.List;

public class GetListAttributeSystemInfoTransformer implements DataTransformer<List<AttributeSystemInfo>>{

    private GraphOperationExecutor workingGraphOperationExecutor;

    public GetListAttributeSystemInfoTransformer(GraphOperationExecutor workingGraphOperationExecutor){
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public List<AttributeSystemInfo> transformResult(Result result) {
        List<AttributeSystemInfo> attributeSystemInfoList = new ArrayList<>();
        if(result.hasNext()){
            while(result.hasNext()){
                Record nodeRecord = result.next();
                if(nodeRecord != null){
                    String attributeName = nodeRecord.get("property").asString();
                    String dataType = nodeRecord.get("type").asString();
                    boolean usedInIndex = false;
                    boolean uniqueAttribute = false;
                    boolean constraintAttribute = false;
                    if(!nodeRecord.get("isIndexed").isNull()){
                        usedInIndex = nodeRecord.get("isIndexed").asBoolean();
                    }
                    if(!nodeRecord.get("uniqueConstraint").isNull()){
                        uniqueAttribute = nodeRecord.get("uniqueConstraint").asBoolean();
                    }
                    if(!nodeRecord.get("existenceConstraint").isNull()){
                        constraintAttribute = nodeRecord.get("existenceConstraint").asBoolean();
                    }
                    AttributeSystemInfo attributeSystemInfo = new AttributeSystemInfo(attributeName,dataType,usedInIndex,
                            uniqueAttribute,constraintAttribute);
                    attributeSystemInfoList.add(attributeSystemInfo);
                }
            }
        }
        return attributeSystemInfoList;
    }
}
