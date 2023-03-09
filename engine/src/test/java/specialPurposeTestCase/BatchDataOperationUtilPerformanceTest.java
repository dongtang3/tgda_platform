package specialPurposeTestCase;

import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.RelationshipEntityValue;
import com.github.tgda.engine.core.term.Entity;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.util.BatchDataOperationUtil;
import com.github.tgda.engine.core.payload.EntitiesRetrieveResult;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.factory.EngineFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchDataOperationUtilPerformanceTest {

    public static void main(String[] args) throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        LocalDateTime localDateTime0 = LocalDateTime.now();
        batchAddNewEntitiesTest();
        //batchAttachNewRelations();
        LocalDateTime localDateTime1 = LocalDateTime.now();

        System.out.println("===============");
        System.out.println(localDateTime0);
        System.out.println(localDateTime1);
        System.out.println("===============");
    }

    public static void batchAddNewEntitiesTest() throws EngineServiceRuntimeException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        Type _operationType = coreRealm.getType("BatchInsertOpType");
        if(_operationType != null){
            _operationType.purgeAllEntities();
        }else{
            _operationType = coreRealm.createType("BatchInsertOpType","BatchInsertOpType"+"DESC");
        }

        List<EntityValue> entityValuesList = new ArrayList<>();
        for(int i =0 ;i< 1000300; i++){
            EntityValue currentEntityValue = new EntityValue();
            Map<String,Object> attributeMap = new HashMap<>();
            String parentElementId = "elementId"+(int)(Math.random()*5000);
            attributeMap.put("pAtter01","pAtter01Value"+i);
            attributeMap.put("pAtter02","pAtter02Value"+i);
            attributeMap.put("pAtter03","pAtter03Value"+i);
            attributeMap.put("pAtter04","pAtter04Value"+i);
            attributeMap.put("pAtter05","pAtter05Value"+i);
            attributeMap.put("pAtter06","pAtter06Value"+i);
            attributeMap.put("parentElementId",parentElementId);
            attributeMap.put("pAtter07","07Value");
            attributeMap.put("pAtter08","08Value");
            attributeMap.put("pAtter09","09Value");
            currentEntityValue.setEntityAttributesValue(attributeMap);
            entityValuesList.add(currentEntityValue);
        }
        Map<String,Object> batchAddResult = BatchDataOperationUtil.batchAddNewEntities("BatchInsertOpType", entityValuesList, BatchDataOperationUtil.CPUUsageRate.High);
        System.out.println(batchAddResult);
    }

    public static void batchAttachNewRelations() throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        Type _operationType = coreRealm.getType("BatchInsertOpType");

        QueryParameters _QueryParameters = new QueryParameters();
        _QueryParameters.setResultNumber(1000300);

        EntitiesRetrieveResult entitiesRetrieveResult = _operationType.getEntities(_QueryParameters);
        System.out.println(entitiesRetrieveResult.getOperationStatistics().getResultEntitiesCount());
        List<Entity> entityList = entitiesRetrieveResult.getConceptionEntities();
        List<List<Entity>> rsList = Lists.partition(entityList, entityList.size()/2);
        List<Entity> fromList = rsList.get(0);
        List<Entity> toList = rsList.get(1);
        List<RelationshipEntityValue> relationshipEntityValueList = new ArrayList<>();

        for(int i=0;i<fromList.size();i++){
            Map<String,Object> relationPropertiesMap = new HashMap<>();
            relationPropertiesMap.put("text01","text01"+i);
            relationPropertiesMap.put("int01",1000+i);
            RelationshipEntityValue relationshipEntityValue = new RelationshipEntityValue();
            relationshipEntityValue.setFromEntityUID(fromList.get(i).getEntityUID());
            relationshipEntityValue.setToEntityUID(toList.get(i).getEntityUID());
            relationshipEntityValue.setEntityAttributesValue(relationPropertiesMap);
            relationshipEntityValueList.add(relationshipEntityValue);
        }
        Map<String,Object> batchOperationResult = BatchDataOperationUtil.batchAttachNewRelations(relationshipEntityValueList,"batchAttachRelType01",18);
        System.out.println(batchOperationResult);
    }
}
