package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.payload.TypeMetaInfo;
import com.github.tgda.engine.core.util.Constant;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class GetListKindMetaInfoTransformer implements DataTransformer<List<TypeMetaInfo>>{

    @Override
    public List<TypeMetaInfo> transformResult(Result result) {
        List<TypeMetaInfo> resultTypeMetaInfoList = new ArrayList<>();
        while(result.hasNext()){
            Record nodeRecord = result.next();
            String conceptionKindName = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._NameProperty).asString();
            String conceptionKindDesc = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._DescProperty).asString();
            ZonedDateTime createDate = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._createDateProperty).asZonedDateTime();
            ZonedDateTime lastModifyDate = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._lastModifyDateProperty).asZonedDateTime();
            String dataOrigin = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._dataOriginProperty).asString();
            long KindUID = nodeRecord.get("id("+CypherBuilder.operationResultName+")").asLong();
            String creatorId = nodeRecord.containsKey(CypherBuilder.operationResultName+"."+ Constant._creatorIdProperty) ?
                    nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._creatorIdProperty).asString():null;
            resultTypeMetaInfoList.add(new TypeMetaInfo(conceptionKindName,conceptionKindDesc,""+KindUID,createDate,lastModifyDate,creatorId,dataOrigin));
        }
        return resultTypeMetaInfoList;
    }
}
