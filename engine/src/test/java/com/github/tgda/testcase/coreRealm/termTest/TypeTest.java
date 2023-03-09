package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.GreaterThanFilteringItem;
import com.github.tgda.engine.core.analysis.query.filteringItem.NullValueFilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.coreRealm.realmServiceCore.payload.*;
import com.github.tgda.coreRealm.realmServiceCore.term.*;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

public class TypeTest {

    private static String testRealmName = "UNIT_TEST_Realm";
    private static String testConceptionKindName = "TestConceptionKindA";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for ConceptionKindTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testConceptionKindFunction() throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        CoreRealm coreRealm = EngineFactory.getDefaultEngine();
        Assert.assertEquals(coreRealm.getStorageImplTech(), StorageImplTech.NEO4J);

        ConceptionKind _ConceptionKind01 = coreRealm.getConceptionKind(testConceptionKindName);
        if(_ConceptionKind01 != null){
            coreRealm.removeConceptionKind(testConceptionKindName,true);
        }
        _ConceptionKind01 = coreRealm.getConceptionKind(testConceptionKindName);
        if(_ConceptionKind01 == null){
            _ConceptionKind01 = coreRealm.createConceptionKind(testConceptionKindName,"TestConceptionKindADesc+中文描述");
            Assert.assertNotNull(_ConceptionKind01);
            Assert.assertEquals(_ConceptionKind01.getConceptionKindName(),testConceptionKindName);
            Assert.assertEquals(_ConceptionKind01.getConceptionKindDesc(),"TestConceptionKindADesc+中文描述");
        }

        ConceptionKind _ConceptionKind02 = coreRealm.getConceptionKind(testConceptionKindName+"_TestRelation");
        if(_ConceptionKind02 != null){
            coreRealm.removeConceptionKind(testConceptionKindName+"_TestRelation",true);
        }
        _ConceptionKind02 = coreRealm.getConceptionKind(testConceptionKindName+"_TestRelation");
        if(_ConceptionKind02 == null){
            _ConceptionKind02 = coreRealm.createConceptionKind(testConceptionKindName+"_TestRelation","TestConceptionKind_TestRelationADesc+中文描述");
            Assert.assertNotNull(_ConceptionKind02);
            Assert.assertEquals(_ConceptionKind02.getConceptionKindName(),testConceptionKindName+"_TestRelation");
            Assert.assertEquals(_ConceptionKind02.getConceptionKindDesc(),"TestConceptionKind_TestRelationADesc+中文描述");
        }

        EntitiesOperationResult purgeEntitiesOperationResult = _ConceptionKind01.purgeAllEntities();
        Assert.assertNotNull(purgeEntitiesOperationResult.getOperationStatistics());
        Assert.assertNotNull(purgeEntitiesOperationResult.getOperationStatistics().getStartTime());
        Assert.assertNotNull(purgeEntitiesOperationResult.getOperationStatistics().getFinishTime());
        Assert.assertNotNull(purgeEntitiesOperationResult.getOperationStatistics().getOperationSummary());
        Assert.assertEquals(purgeEntitiesOperationResult.getOperationStatistics().getFailItemsCount(),0);

        Long entitiesCount = _ConceptionKind01.countConceptionEntities();
        Assert.assertEquals(entitiesCount.longValue(),0);

        List<AttributesViewKind> containedAttributesViewKindsList = _ConceptionKind01.getContainsAttributesViewKinds();
        Assert.assertNotNull(containedAttributesViewKindsList);
        Assert.assertEquals(containedAttributesViewKindsList.size(),0);

        AttributesViewKind attributesViewKindToAdd01 = coreRealm.createAttributesViewKind("targetAttributesViewKindToAdd01","targetAttributesViewKindToAdd01Desc",null);

        boolean addAttributesViewKindResult = _ConceptionKind01.attachAttributesViewKind(attributesViewKindToAdd01.getAttributesViewKindUID());
        Assert.assertTrue(addAttributesViewKindResult);
        addAttributesViewKindResult = _ConceptionKind01.attachAttributesViewKind(attributesViewKindToAdd01.getAttributesViewKindUID());
        Assert.assertTrue(addAttributesViewKindResult);
        addAttributesViewKindResult = _ConceptionKind01.attachAttributesViewKind(null);
        Assert.assertFalse(addAttributesViewKindResult);

        boolean exceptionShouldBeCaught = false;
        try{
            _ConceptionKind01.attachAttributesViewKind("123456");
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        containedAttributesViewKindsList = _ConceptionKind01.getContainsAttributesViewKinds();
        Assert.assertNotNull(containedAttributesViewKindsList);
        Assert.assertEquals(containedAttributesViewKindsList.size(),1);

        Assert.assertEquals(containedAttributesViewKindsList.get(0).getAttributesViewKindName(),"targetAttributesViewKindToAdd01");
        Assert.assertEquals(containedAttributesViewKindsList.get(0).getAttributesViewKindDesc(),"targetAttributesViewKindToAdd01Desc");
        Assert.assertEquals(containedAttributesViewKindsList.get(0).getAttributesViewKindUID(),attributesViewKindToAdd01.getAttributesViewKindUID());
        Assert.assertEquals(containedAttributesViewKindsList.get(0).getAttributesViewKindDataForm(), AttributesViewKind.AttributesViewKindDataForm.SINGLE_VALUE);

        AttributesViewKind attributesViewKindToAdd02 = coreRealm.createAttributesViewKind("targetAttributesViewKindToAdd02",
                "targetAttributesViewKindToAdd02Desc",AttributesViewKind.AttributesViewKindDataForm.LIST_VALUE);
        addAttributesViewKindResult = _ConceptionKind01.attachAttributesViewKind(attributesViewKindToAdd02.getAttributesViewKindUID());
        Assert.assertTrue(addAttributesViewKindResult);

        containedAttributesViewKindsList = _ConceptionKind01.getContainsAttributesViewKinds();
        Assert.assertNotNull(containedAttributesViewKindsList);
        Assert.assertEquals(containedAttributesViewKindsList.size(),2);

        List<AttributesViewKind> targetAttributesViewKindList = _ConceptionKind01.getContainsAttributesViewKinds("targetAttributesViewKindToAdd02");
        Assert.assertNotNull(targetAttributesViewKindList);
        Assert.assertNotNull(targetAttributesViewKindList.get(0));
        Assert.assertEquals(targetAttributesViewKindList.get(0).getAttributesViewKindName(),"targetAttributesViewKindToAdd02");
        Assert.assertEquals(targetAttributesViewKindList.get(0).getAttributesViewKindDesc(),"targetAttributesViewKindToAdd02Desc");
        Assert.assertEquals(targetAttributesViewKindList.get(0).getAttributesViewKindUID(),attributesViewKindToAdd02.getAttributesViewKindUID());
        Assert.assertEquals(targetAttributesViewKindList.get(0).getAttributesViewKindDataForm(), AttributesViewKind.AttributesViewKindDataForm.LIST_VALUE);

        List<AttributeKind> attributeKindList = _ConceptionKind01.getContainsSingleValueAttributeKinds();
        Assert.assertNotNull(attributeKindList);
        Assert.assertEquals(attributeKindList.size(),0);

        AttributeKind attributeKind01 = coreRealm.createAttributeKind("attributeKind01","attributeKind01Desc", AttributeDataType.BOOLEAN);
        Assert.assertNotNull(attributeKind01);

        boolean attachAttributeKindRes = attributesViewKindToAdd02.attachAttributeKind(attributeKind01.getAttributeKindUID());
        Assert.assertTrue(attachAttributeKindRes);

        attributeKindList = _ConceptionKind01.getContainsSingleValueAttributeKinds();
        Assert.assertNotNull(attributeKindList);
        Assert.assertEquals(attributeKindList.size(),0);

        AttributeKind attributeKind02 = coreRealm.createAttributeKind("attributeKind02","attributeKind02Desc", AttributeDataType.TIMESTAMP);
        Assert.assertNotNull(attributeKind02);

        attachAttributeKindRes = attributesViewKindToAdd01.attachAttributeKind(attributeKind02.getAttributeKindUID());
        Assert.assertTrue(attachAttributeKindRes);

        attributeKindList = _ConceptionKind01.getContainsSingleValueAttributeKinds();
        Assert.assertNotNull(attributeKindList);
        Assert.assertEquals(attributeKindList.size(),1);
        Assert.assertNotNull(attributeKindList.get(0).getAttributeKindUID());
        Assert.assertEquals(attributeKindList.get(0).getAttributeKindName(),"attributeKind02");
        Assert.assertEquals(attributeKindList.get(0).getAttributeKindDesc(),"attributeKind02Desc");
        Assert.assertEquals(attributeKindList.get(0).getAttributeDataType(),AttributeDataType.TIMESTAMP);

        attributeKindList = _ConceptionKind01.getContainsSingleValueAttributeKinds("attributeKind02");
        Assert.assertNotNull(attributeKindList);
        Assert.assertEquals(attributeKindList.size(),1);
        Assert.assertNotNull(attributeKindList.get(0).getAttributeKindUID());
        Assert.assertEquals(attributeKindList.get(0).getAttributeKindName(),"attributeKind02");
        Assert.assertEquals(attributeKindList.get(0).getAttributeKindDesc(),"attributeKind02Desc");
        Assert.assertEquals(attributeKindList.get(0).getAttributeDataType(),AttributeDataType.TIMESTAMP);

        attributeKindList = _ConceptionKind01.getContainsSingleValueAttributeKinds("attributeKind02+NotExist");
        Assert.assertNotNull(attributeKindList);
        Assert.assertEquals(attributeKindList.size(),0);

        targetAttributesViewKindList = _ConceptionKind01.getContainsAttributesViewKinds("targetAttributesViewKindNotExist");
        Assert.assertEquals(targetAttributesViewKindList.size(),0);

        targetAttributesViewKindList = _ConceptionKind01.getContainsAttributesViewKinds("targetAttributesViewKindToAdd02");
        boolean removeViewKindResult = _ConceptionKind01.detachAttributesViewKind(targetAttributesViewKindList.get(0).getAttributesViewKindUID());
        Assert.assertTrue(removeViewKindResult);
        removeViewKindResult = _ConceptionKind01.detachAttributesViewKind(targetAttributesViewKindList.get(0).getAttributesViewKindUID());
        Assert.assertFalse(removeViewKindResult);

        targetAttributesViewKindList = _ConceptionKind01.getContainsAttributesViewKinds("targetAttributesViewKindToAdd02");
        Assert.assertEquals(targetAttributesViewKindList.size(),0);


        Map<String,Object> newEntityValue= new HashMap<>();
        newEntityValue.put("prop1",10000l);
        newEntityValue.put("prop2",190.22d);
        newEntityValue.put("prop3",50);
        newEntityValue.put("prop4","thi is s string");
        newEntityValue.put("prop5","我是中文string");
        newEntityValue.put("propTmp1", LocalDate.of(1667,1,1));
        newEntityValue.put("propTmp2", new LocalTime[]{LocalTime.of(13,3,3),
                LocalTime.of(14,4,4)});

        EntityValue conceptionEntityValue = new EntityValue(newEntityValue);

        Entity _Entity = _ConceptionKind01.newEntity(conceptionEntityValue,false);
        Assert.assertNotNull(_Entity);
        Assert.assertEquals(_Entity.getConceptionKindName(),testConceptionKindName);
        Assert.assertEquals(_Entity.getAllConceptionKindNames().size(),1);
        Assert.assertEquals(_Entity.getAllConceptionKindNames().get(0),testConceptionKindName);
        Assert.assertNotNull(_Entity.getEntityUID());

        String queryUIDValue = _Entity.getEntityUID();
        Entity _queryResultEntity = _ConceptionKind01.getEntityByUID(queryUIDValue);

        Assert.assertNotNull(_queryResultEntity);
        Assert.assertEquals(_queryResultEntity.getConceptionKindName(),testConceptionKindName);
        Assert.assertEquals(_queryResultEntity.getAllConceptionKindNames().size(),1);
        Assert.assertEquals(_queryResultEntity.getAllConceptionKindNames().get(0),testConceptionKindName);
        Assert.assertEquals(_queryResultEntity.getEntityUID(),queryUIDValue);

        List<String> attributeNameList = _queryResultEntity.getAttributeNames();

        Assert.assertNotNull(attributeNameList);
        Assert.assertEquals(attributeNameList.size(),7);
        Assert.assertTrue(attributeNameList.contains("prop1"));
        Assert.assertTrue(attributeNameList.contains("prop2"));
        Assert.assertTrue(attributeNameList.contains("prop3"));
        Assert.assertTrue(attributeNameList.contains("prop4"));
        Assert.assertTrue(attributeNameList.contains("prop5"));
        Assert.assertTrue(attributeNameList.contains("propTmp1"));
        Assert.assertTrue(attributeNameList.contains("propTmp2"));

        List<AttributeValue> attributeValueList = _queryResultEntity.getAttributes();
        Assert.assertNotNull(attributeValueList);
        Assert.assertEquals(attributeValueList.size(),7);

        entitiesCount = _ConceptionKind01.countConceptionEntities();
        Assert.assertEquals(entitiesCount.longValue(),1);

        Map<String,Object> newEntityValueMap= new HashMap<>();
        newEntityValueMap.put("prop1",Long.parseLong("12345"));
        newEntityValueMap.put("prop2",Double.parseDouble("12345.789"));
        newEntityValueMap.put("prop3",Integer.parseInt("1234"));
        newEntityValueMap.put("prop4","thi is s string");
        newEntityValueMap.put("prop5",Boolean.valueOf("true"));
        newEntityValueMap.put("prop6", new BigDecimal("5566778890.223344"));
        newEntityValueMap.put("prop7", Short.valueOf("24"));
        newEntityValueMap.put("prop8", Float.valueOf("1234.66"));
        newEntityValueMap.put("prop9", new Long[]{1000l,2000l,3000l});
        newEntityValueMap.put("prop10", new Double[]{1000.1d,2000.2d,3000.3d});
        newEntityValueMap.put("prop11", new Integer[]{100,200,300});
        newEntityValueMap.put("prop12", new String[]{"this is str1","这是字符串2"});
        newEntityValueMap.put("prop13", new Boolean[]{true,true,false,false,true});
        newEntityValueMap.put("prop14", new BigDecimal[]{new BigDecimal("1234567.890"),new BigDecimal("987654321.12345")});
        newEntityValueMap.put("prop15", new Short[]{1,2,3,4,5});
        newEntityValueMap.put("prop16", new Float[]{1000.1f,2000.2f,3000.3f});
        newEntityValueMap.put("prop17", new Date());
        newEntityValueMap.put("prop18", new Date[]{new Date(),new Date(),new Date(),new Date()});
        newEntityValueMap.put("prop19", Byte.valueOf("2"));
        newEntityValueMap.put("prop20", "this is a byte array value".getBytes());
        newEntityValueMap.put("prop21", new Byte[]{Byte.valueOf("1"),Byte.valueOf("3"),Byte.valueOf("5")});
        newEntityValueMap.put("prop22", LocalDate.of(1667,1,1));
        newEntityValueMap.put("prop23", new LocalTime[]{LocalTime.of(13,3,3),
                LocalTime.of(14,4,4)});

        List<EntityValue> conceptionEntityValueList = new ArrayList<>();
        EntityValue conceptionEntityValue1 = new EntityValue(newEntityValueMap);
        EntityValue conceptionEntityValue2 = new EntityValue(newEntityValueMap);
        EntityValue conceptionEntityValue3 = new EntityValue(newEntityValueMap);
        conceptionEntityValueList.add(conceptionEntityValue1);
        conceptionEntityValueList.add(conceptionEntityValue2);
        conceptionEntityValueList.add(conceptionEntityValue3);

        EntitiesOperationResult addEntitiesResult = _ConceptionKind01.newEntities(conceptionEntityValueList,false);
        Assert.assertNotNull(addEntitiesResult);
        Assert.assertNotNull(addEntitiesResult.getSuccessEntityUIDs());
        Assert.assertNotNull(addEntitiesResult.getOperationStatistics());
        Assert.assertEquals(addEntitiesResult.getSuccessEntityUIDs().size(),3);
        Assert.assertEquals(addEntitiesResult.getOperationStatistics().getSuccessItemsCount(),3);
        Assert.assertNotNull(addEntitiesResult.getOperationStatistics().getStartTime());
        Assert.assertNotNull(addEntitiesResult.getOperationStatistics().getFinishTime());
        Assert.assertNotNull(addEntitiesResult.getOperationStatistics().getOperationSummary());
        Assert.assertEquals(addEntitiesResult.getOperationStatistics().getFailItemsCount(),0);

        entitiesCount = _ConceptionKind01.countConceptionEntities();
        Assert.assertEquals(entitiesCount.longValue(),4);

        for(String currentEntityUID:addEntitiesResult.getSuccessEntityUIDs()){
            Entity currentEntity = _ConceptionKind01.getEntityByUID(currentEntityUID);
            Assert.assertNotNull(currentEntity);
            Assert.assertEquals(currentEntity.getAttributes().size(),23);
        }

        Entity currentEntity = _ConceptionKind01.getEntityByUID(addEntitiesResult.getSuccessEntityUIDs().get(0));

        Assert.assertEquals(currentEntity.getAttribute("prop1").getAttributeValue(),Long.parseLong("12345"));
        Assert.assertEquals(currentEntity.getAttribute("prop2").getAttributeValue(),Double.parseDouble("12345.789"));
        Assert.assertEquals(currentEntity.getAttribute("prop22").getAttributeValue(),LocalDate.of(1667,1,1));

        Map<String,Object> updateEntityValueMap= new HashMap<>();
        updateEntityValueMap.put("prop1",Long.parseLong("59000"));
        updateEntityValueMap.put("prop2",Double.parseDouble("10000000.1"));
        updateEntityValueMap.put("prop3",new Date());
        updateEntityValueMap.put("prop3_NotExist",new Date());
        updateEntityValueMap.put("prop21", new Byte[]{Byte.valueOf("88"),Byte.valueOf("77"),Byte.valueOf("66")});
        updateEntityValueMap.put("prop23", new LocalTime[]{LocalTime.of(0,0,0)});

        EntityValue conceptionEntityValueForUpdate = new EntityValue(updateEntityValueMap);
        conceptionEntityValueForUpdate.setEntityUID(addEntitiesResult.getSuccessEntityUIDs().get(0));

        Entity updatedEntityValue = _ConceptionKind01.updateEntity(conceptionEntityValueForUpdate);
        Assert.assertNotNull(updatedEntityValue);
        Assert.assertEquals(updatedEntityValue.getAttribute("prop1").getAttributeValue(),Long.parseLong("59000"));
        Assert.assertEquals(updatedEntityValue.getAttribute("prop2").getAttributeValue(),Double.parseDouble("10000000.1"));
        Assert.assertEquals(updatedEntityValue.getAttribute("prop3").getAttributeValue(),Long.parseLong("1234"));
        Assert.assertEquals(((LocalTime[])updatedEntityValue.getAttribute("prop23").getAttributeValue())[0],LocalTime.of(0,0,0));
        Assert.assertFalse(updatedEntityValue.hasAttribute("prop3_NotExist"));

        List<EntityValue> conceptionEntityValueForUpdateList = new ArrayList<>();
        EntityValue conceptionEntityValueForUpdate1 = new EntityValue(updateEntityValueMap);
        conceptionEntityValueForUpdate1.setEntityUID(addEntitiesResult.getSuccessEntityUIDs().get(0));
        conceptionEntityValueForUpdateList.add(conceptionEntityValueForUpdate1);
        EntityValue conceptionEntityValueForUpdate2 = new EntityValue(updateEntityValueMap);
        conceptionEntityValueForUpdate2.setEntityUID(addEntitiesResult.getSuccessEntityUIDs().get(1));
        conceptionEntityValueForUpdateList.add(conceptionEntityValueForUpdate2);
        EntityValue conceptionEntityValueForUpdate3 = new EntityValue(updateEntityValueMap);
        conceptionEntityValueForUpdate3.setEntityUID(addEntitiesResult.getSuccessEntityUIDs().get(2));
        conceptionEntityValueForUpdateList.add(conceptionEntityValueForUpdate3);
        EntityValue conceptionEntityValueForUpdate4 = new EntityValue(updateEntityValueMap);
        conceptionEntityValueForUpdate4.setEntityUID("123456789");
        conceptionEntityValueForUpdateList.add(conceptionEntityValueForUpdate4);

        EntitiesOperationResult entitiesOperationResult = _ConceptionKind01.updateEntities(conceptionEntityValueForUpdateList);
        Assert.assertNotNull(entitiesOperationResult);
        Assert.assertNotNull(entitiesOperationResult.getSuccessEntityUIDs());
        Assert.assertNotNull(entitiesOperationResult.getOperationStatistics());
        Assert.assertNotNull(entitiesOperationResult.getOperationStatistics().getOperationSummary());
        Assert.assertNotNull(entitiesOperationResult.getOperationStatistics().getStartTime());
        Assert.assertNotNull(entitiesOperationResult.getOperationStatistics().getFinishTime());
        Assert.assertEquals(entitiesOperationResult.getSuccessEntityUIDs().size(),3);
        Assert.assertTrue(entitiesOperationResult.getSuccessEntityUIDs().contains(addEntitiesResult.getSuccessEntityUIDs().get(0)));
        Assert.assertTrue(entitiesOperationResult.getSuccessEntityUIDs().contains(addEntitiesResult.getSuccessEntityUIDs().get(1)));
        Assert.assertTrue(entitiesOperationResult.getSuccessEntityUIDs().contains(addEntitiesResult.getSuccessEntityUIDs().get(2)));
        Assert.assertEquals(entitiesOperationResult.getOperationStatistics().getSuccessItemsCount(),3);
        Assert.assertEquals(entitiesOperationResult.getOperationStatistics().getFailItemsCount(),1);

        Entity conceptionEntityForDelete = _ConceptionKind01.getEntityByUID(addEntitiesResult.getSuccessEntityUIDs().get(0));
        Assert.assertNotNull(conceptionEntityForDelete);
        boolean deleteEntityResult = _ConceptionKind01.deleteEntity(addEntitiesResult.getSuccessEntityUIDs().get(0));
        Assert.assertTrue(deleteEntityResult);
        conceptionEntityForDelete = _ConceptionKind01.getEntityByUID(addEntitiesResult.getSuccessEntityUIDs().get(0));
        Assert.assertNull(conceptionEntityForDelete);

        exceptionShouldBeCaught = false;
        try{
            _ConceptionKind01.deleteEntity("123456");
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        exceptionShouldBeCaught = false;
        try{
            _ConceptionKind01.deleteEntity(addEntitiesResult.getSuccessEntityUIDs().get(0));
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        List<String> entityUIDsForDelete = new ArrayList<>();
        entityUIDsForDelete.add(addEntitiesResult.getSuccessEntityUIDs().get(0));
        entityUIDsForDelete.add(addEntitiesResult.getSuccessEntityUIDs().get(1));
        entityUIDsForDelete.add(addEntitiesResult.getSuccessEntityUIDs().get(2));
        entityUIDsForDelete.add("123456");

        entitiesOperationResult = _ConceptionKind01.deleteEntities(entityUIDsForDelete);
        Assert.assertNotNull(entitiesOperationResult);
        Assert.assertNotNull(entitiesOperationResult.getSuccessEntityUIDs());
        Assert.assertNotNull(entitiesOperationResult.getOperationStatistics());
        Assert.assertNotNull(entitiesOperationResult.getOperationStatistics().getOperationSummary());
        Assert.assertNotNull(entitiesOperationResult.getOperationStatistics().getStartTime());
        Assert.assertNotNull(entitiesOperationResult.getOperationStatistics().getFinishTime());
        Assert.assertEquals(entitiesOperationResult.getSuccessEntityUIDs().size(),2);
        Assert.assertTrue(entitiesOperationResult.getSuccessEntityUIDs().contains(addEntitiesResult.getSuccessEntityUIDs().get(1)));
        Assert.assertTrue(entitiesOperationResult.getSuccessEntityUIDs().contains(addEntitiesResult.getSuccessEntityUIDs().get(2)));
        Assert.assertEquals(entitiesOperationResult.getOperationStatistics().getSuccessItemsCount(),2);
        Assert.assertEquals(entitiesOperationResult.getOperationStatistics().getFailItemsCount(),2);

        Entity relationQueryTest01 = _ConceptionKind01.newEntity(conceptionEntityValueForUpdate,false);
        Entity relationQueryTest02 = _ConceptionKind01.newEntity(conceptionEntityValueForUpdate,false);
        for(int i=0;i<100;i++){
            Map<String,Object> newEntityForRelationTestValueMap= new HashMap<>();
            newEntityForRelationTestValueMap.put("prop1",1000);
            EntityValue newRelationTestEntityValue = new EntityValue(newEntityForRelationTestValueMap);
            Entity relationEntity = _ConceptionKind02.newEntity(newRelationTestEntityValue,false);
            relationQueryTest01.attachFromRelation(relationEntity.getEntityUID(),"queryTestRelation01",null,false);
        }
        for(int i=0;i<5;i++){
            Map<String,Object> newEntityForRelationTestValueMap= new HashMap<>();
            newEntityForRelationTestValueMap.put("prop1",5000);
            EntityValue newRelationTestEntityValue = new EntityValue(newEntityForRelationTestValueMap);
            Entity relationEntity = _ConceptionKind02.newEntity(newRelationTestEntityValue,false);
            relationQueryTest02.attachFromRelation(relationEntity.getEntityUID(),"queryTestRelation01",null,false);
        }
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setStartPage(1);
        queryParameters.setEndPage(100);
        queryParameters.setPageSize(10);
        ConceptionEntitiesRetrieveResult conceptionEntitiesRetrieveResult =_ConceptionKind01.getKindDirectRelatedEntities(null,"queryTestRelation01",Direction.FROM,null,queryParameters);
        Assert.assertEquals(conceptionEntitiesRetrieveResult.getConceptionEntities().size(),105);

        List<String> attributesList = new ArrayList<>();
        attributesList.add("prop1");
        attributesList.add("propNotExist");
        ConceptionEntitiesAttributesRetrieveResult conceptionEntitiesAttributesRetrieveResult =_ConceptionKind01.getAttributesOfKindDirectRelatedEntities(null,attributesList,"queryTestRelation01",Direction.FROM,null,queryParameters);
        Assert.assertNotNull(conceptionEntitiesAttributesRetrieveResult);
        List<EntityValue> conceptionEntityValuesList = conceptionEntitiesAttributesRetrieveResult.getEntityValues();

        for(EntityValue currentEntityValue:conceptionEntityValuesList){
            Assert.assertNotNull(currentEntityValue.getEntityAttributesValue());
            Assert.assertNotNull(currentEntityValue.getEntityUID());
            Assert.assertNotNull(currentEntityValue.getEntityAttributesValue().get("prop1"));
            Assert.assertNull(currentEntityValue.getEntityAttributesValue().get("propNotExist"));
        }

        conceptionEntitiesRetrieveResult =_ConceptionKind01.getKindDirectRelatedEntities(null,"queryTestRelation01",Direction.TO,null,queryParameters);
        Assert.assertEquals(conceptionEntitiesRetrieveResult.getConceptionEntities().size(),0);

        conceptionEntitiesRetrieveResult =_ConceptionKind01.getKindDirectRelatedEntities(null,"queryTestRelation01",Direction.TWO_WAY,null,queryParameters);
        Assert.assertEquals(conceptionEntitiesRetrieveResult.getConceptionEntities().size(),105);

        queryParameters.setDefaultFilteringItem(new GreaterThanFilteringItem("prop1",1000));
        conceptionEntitiesRetrieveResult =_ConceptionKind01.getKindDirectRelatedEntities(null,"queryTestRelation01",Direction.FROM,null,queryParameters);
        Assert.assertEquals(conceptionEntitiesRetrieveResult.getConceptionEntities().size(),5);

        List<String> startEntityUIDS = new ArrayList<>();
        startEntityUIDS.add(relationQueryTest02.getEntityUID());
        conceptionEntitiesRetrieveResult =_ConceptionKind01.getKindDirectRelatedEntities(startEntityUIDS,"queryTestRelation01",Direction.FROM,null,queryParameters);
        Assert.assertEquals(conceptionEntitiesRetrieveResult.getConceptionEntities().size(),5);

        startEntityUIDS.clear();
        startEntityUIDS.add(relationQueryTest01.getEntityUID());
        conceptionEntitiesRetrieveResult =_ConceptionKind01.getKindDirectRelatedEntities(startEntityUIDS,"queryTestRelation01",Direction.FROM,null,queryParameters);
        Assert.assertEquals(conceptionEntitiesRetrieveResult.getConceptionEntities().size(),0);

        Set<KindDataDistributionInfo> dsset = _ConceptionKind01.getKindDataDistributionStatistics(0.9);
        Assert.assertTrue(dsset != null);
        Assert.assertTrue(dsset.size()>0);

        Set<KindAttributeDistributionInfo> deset2 = _ConceptionKind01.getKindAttributesDistributionStatistics(0.9);
        Assert.assertTrue(deset2 != null);
        Assert.assertTrue(deset2.size()>0);

        Set<Entity> randomEntityList = _ConceptionKind01.getRandomEntities(2);
        Assert.assertNotNull(randomEntityList);
        Assert.assertEquals(randomEntityList.size(),2);
        for(Entity currentEntity:randomEntityList){
            Assert.assertEquals(currentEntity.getConceptionKindName(),testConceptionKindName);
            Assert.assertNotNull(currentEntity.getEntityUID());
        }

        AttributesParameters attributesParameters = new AttributesParameters();
        attributesParameters.setDefaultFilteringItem(new NullValueFilteringItem("mustNotExistAttr"));
        randomEntityList = _ConceptionKind01.getRandomEntities(attributesParameters,true,2);
        Assert.assertNotNull(randomEntityList);
        Assert.assertEquals(randomEntityList.size(),2);
        for(Entity currentEntity:randomEntityList){
            Assert.assertEquals(currentEntity.getConceptionKindName(),testConceptionKindName);
            Assert.assertNotNull(currentEntity.getEntityUID());
        }

        Assert.assertEquals(_ConceptionKind01.getConceptionKindDesc(),"TestConceptionKindADesc+中文描述");
        boolean updateDescResult = _ConceptionKind01.updateConceptionKindDesc("TestConceptionKindADesc+中文描述UPD");
        Assert.assertTrue(updateDescResult);
        Assert.assertEquals(_ConceptionKind01.getConceptionKindDesc(),"TestConceptionKindADesc+中文描述UPD");
        Assert.assertEquals(coreRealm.getConceptionKind(testConceptionKindName).getConceptionKindDesc(),"TestConceptionKindADesc+中文描述UPD");

        Assert.assertEquals(attributeKind02.getAttributeKindDesc(),"attributeKind02Desc");
        updateDescResult = attributeKind02.updateAttributeKindDesc("attributeKind02DescUPD");
        Assert.assertTrue(updateDescResult);
        Assert.assertEquals(attributeKind02.getAttributeKindDesc(),"attributeKind02DescUPD");
        Assert.assertEquals(coreRealm.getAttributeKind(attributeKind02.getAttributeKindUID()).getAttributeKindDesc(),"attributeKind02DescUPD");
    }
}
