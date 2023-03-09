package specialPurposeTestCase;

import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.exception.EngineFunctionNotSupportedException;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.util.BatchDataOperationUtil;
import com.github.tgda.engine.core.payload.EntitiesRetrieveResult;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.RelationshipType;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.factory.EngineFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PerformanceAssessmentTestCase {

    private static List<String> elementIdList = new ArrayList<>();
    private static String BuildingElementType = "BuildingElementType";
    private static String ElementProperties = "ElementProperties";
    private static String HasProperty = "HasProperty";

    public static void main(String[] args) throws EngineServiceRuntimeException, EngineFunctionNotSupportedException, EngineServiceEntityExploreException {
        setTypeDefinitions();
        List<EntityValue> entityValuesList1 = new ArrayList<>();
        List<EntityValue> entityValuesList2 = new ArrayList<>();
        prepareConceptionKindDataMap(entityValuesList1, entityValuesList2);
        loadData(entityValuesList1, entityValuesList2);
        linkData();
        deleteData();
    }

    private static void setTypeDefinitions() throws EngineServiceRuntimeException, EngineFunctionNotSupportedException {
        Engine coreRealm = EngineFactory.getDefaultEngine();

        Type _BuildingElementType = coreRealm.getType(BuildingElementType);
        if(_BuildingElementType != null){
            _BuildingElementType.purgeAllEntities();
        }else{
            _BuildingElementType = coreRealm.createType(BuildingElementType,BuildingElementType+"DESC");
        }

        Type _ElementProperties = coreRealm.getType(ElementProperties);
        if(_ElementProperties != null){
            _ElementProperties.purgeAllEntities();
        }else{
            _BuildingElementType = coreRealm.createType(ElementProperties,ElementProperties+"DESC");
        }

        RelationshipType _HasProperty = coreRealm.getRelationshipType(HasProperty);
        if(_ElementProperties == null){
            _HasProperty = coreRealm.createRelationshipType(HasProperty,HasProperty+"DESC");
        }
    }

    private static void loadData(List<EntityValue> entityValuesList1,
                                List<EntityValue> entityValuesList2){
        BatchDataOperationUtil.batchAddNewEntities(BuildingElementType, entityValuesList1,5);
        Map<String,Object> batchLoadResultMap = BatchDataOperationUtil.batchAddNewEntities(ElementProperties, entityValuesList2,30);
        System.out.println(batchLoadResultMap);
    }

    private static void prepareConceptionKindDataMap(List<EntityValue> entityValuesList1,
                                             List<EntityValue> entityValuesList2){
        elementIdList.clear();
        for(int i =0 ;i< 5000; i++){
            EntityValue currentEntityValue1 = new EntityValue();
            Map<String,Object> attributeMap = new HashMap<>();
            attributeMap.put("atter01","atter01Value"+i);
            attributeMap.put("atter02","atter02Value"+i);
            attributeMap.put("atter03","atter03Value"+i);
            attributeMap.put("atter04","atter04Value"+i);
            attributeMap.put("atter05","atter05Value"+i);
            attributeMap.put("atter06","atter06Value"+i);
            attributeMap.put("elementId","elementId"+i);
            elementIdList.add("elementId"+i);
            currentEntityValue1.setEntityAttributesValue(attributeMap);
            entityValuesList1.add(currentEntityValue1);
        }
        for(int i =0 ;i< 140000; i++){
            EntityValue currentEntityValue2 = new EntityValue();
            Map<String,Object> attributeMap = new HashMap<>();
            String parentElementId = "elementId"+(int)(Math.random()*5000);
            attributeMap.put("pAtter01","pAtter01Value"+i);
            attributeMap.put("pAtter02","pAtter02Value"+i);
            attributeMap.put("pAtter03","pAtter03Value"+i);
            attributeMap.put("pAtter04","pAtter04Value"+i);
            attributeMap.put("pAtter05","pAtter05Value"+i);
            attributeMap.put("pAtter06","pAtter06Value"+i);
            attributeMap.put("parentElementId",parentElementId);
            currentEntityValue2.setEntityAttributesValue(attributeMap);
            entityValuesList2.add(currentEntityValue2);
        }
    }

    private static void linkData() throws EngineServiceEntityExploreException {
        /*
        CoreRealm coreRealm = RealmTermFactory.getDefaultCoreRealm();
        coreRealm.openGlobalSession();

        Map<String,String> _BuildingElementId_UIDMapping = new HashMap<>();
        QueryParameters exploreParameters = new QueryParameters();
        exploreParameters.setResultNumber(1000000);

        ConceptionKind _BuildingElementType = coreRealm.getConceptionKind(BuildingElementType);
        List<String> _BuildingElementTypeAttributeList = new ArrayList<>();
        _BuildingElementTypeAttributeList.add("elementId");
        ConceptionEntitiesAttributesRetrieveResult _ConceptionEntitiesAttributesRetrieveResult1 =
                _BuildingElementType.getSingleValueEntityAttributesByAttributeNames(_BuildingElementTypeAttributeList,exploreParameters);
        List<EntityValue> conceptionEntityValues1 = _ConceptionEntitiesAttributesRetrieveResult1.getEntityValues();

        for(EntityValue currentEntityValue:conceptionEntityValues1){
            String conceptionEntityUID = currentEntityValue.getEntityUID();
            String elementId = currentEntityValue.getEntityAttributesValue().get("elementId").toString();
            _BuildingElementId_UIDMapping.put(elementId,conceptionEntityUID);
        }

        List<RelationshipEntityValue> relationEntityValueList = new ArrayList<>();

        ConceptionKind _ElementProperties = coreRealm.getConceptionKind(ElementProperties);
        List<String> _ElementPropertiesTypeAttributeList = new ArrayList<>();
        _ElementPropertiesTypeAttributeList.add("parentElementId");
        ConceptionEntitiesAttributesRetrieveResult _ConceptionEntitiesAttributesRetrieveResult2 =
                _ElementProperties.getSingleValueEntityAttributesByAttributeNames(_ElementPropertiesTypeAttributeList,exploreParameters);

        List<EntityValue> conceptionEntityValues2 = _ConceptionEntitiesAttributesRetrieveResult2.getEntityValues();
        for(EntityValue currentEntityValue:conceptionEntityValues2){
            String conceptionEntityUID_Param = currentEntityValue.getEntityUID();
            String parentElementId = currentEntityValue.getEntityAttributesValue().get("parentElementId").toString();
            String conceptionEntityUID_building = _BuildingElementId_UIDMapping.get(parentElementId);
            if(conceptionEntityUID_building != null){
                RelationshipEntityValue relationEntityValue = new RelationshipEntityValue(null,conceptionEntityUID_building,conceptionEntityUID_Param,null);
                relationEntityValueList.add(relationEntityValue);
            }

        }
        coreRealm.closeGlobalSession();
        Map<String,Object> batchLoadResultMap = BatchDataOperationUtil.batchAttachNewRelations(relationEntityValueList,HasProperty,30);
        System.out.println(batchLoadResultMap);

        above logic is equal to below method invoke
        */
        Map<String,Object> batchLoadResultMap = BatchDataOperationUtil.batchAttachNewRelationsWithSinglePropertyValueMatch(
                BuildingElementType,null,"elementId",ElementProperties,
                null,"parentElementId",HasProperty, BatchDataOperationUtil.CPUUsageRate.High);
        System.out.println(batchLoadResultMap);
    }

    private static void deleteData() throws EngineServiceEntityExploreException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        coreRealm.openGlobalSession();
        Type _ElementProperties = coreRealm.getType(ElementProperties);
        QueryParameters exploreParameters = new QueryParameters();
        exploreParameters.setResultNumber(1000000);
        EntitiesRetrieveResult _EntitiesRetrieveResult = _ElementProperties.getEntities(exploreParameters);
        List<String> entityUIDList = new ArrayList<>();
        List<Entity> resultEntityList = _EntitiesRetrieveResult.getConceptionEntities();
        for(Entity currentEntity :resultEntityList){
            entityUIDList.add(currentEntity.getEntityUID());
        }
        coreRealm.closeGlobalSession();

        Map<String,Object> batchLoadResultMap = BatchDataOperationUtil.batchDeleteEntities(entityUIDList,30);
        System.out.println(batchLoadResultMap);
    }
}
