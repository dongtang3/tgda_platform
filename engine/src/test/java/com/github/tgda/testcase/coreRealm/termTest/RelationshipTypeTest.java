package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.EqualFilteringItem;
import com.github.tgda.engine.core.analysis.query.filteringItem.GreaterThanEqualFilteringItem;
import com.github.tgda.engine.core.analysis.query.filteringItem.NullValueFilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.EntitiesOperationResult;
import com.github.tgda.engine.core.payload.RelationshipEntitiesRetrieveResult;
import com.github.tgda.coreRealm.realmServiceCore.term.*;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.*;

public class RelationshipTypeTest {

    private static String testRealmName = "UNIT_TEST_Realm";
    private static String testConceptionKindName = "TestConceptionKindForRelationA";
    private static String testConceptionKindName2 = "TestConceptionKindForRelationB";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for RelationKindTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testRelationKindFunction() throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        CoreRealm coreRealm = EngineFactory.getDefaultEngine();
        Assert.assertEquals(coreRealm.getStorageImplTech(), StorageImplTech.NEO4J);
        coreRealm.openGlobalSession();
        ConceptionKind _ConceptionKind01 = coreRealm.getConceptionKind(testConceptionKindName);
        ConceptionKind _ConceptionKind02 = coreRealm.getConceptionKind(testConceptionKindName2);
        if(_ConceptionKind01 != null){
            coreRealm.removeConceptionKind(testConceptionKindName,true);
        }
        if(_ConceptionKind02 != null){
            coreRealm.removeConceptionKind(testConceptionKindName2,true);
        }
        _ConceptionKind01 = coreRealm.getConceptionKind(testConceptionKindName);
        _ConceptionKind02 = coreRealm.getConceptionKind(testConceptionKindName2);
        if(_ConceptionKind01 == null){
            _ConceptionKind01 = coreRealm.createConceptionKind(testConceptionKindName,null);
        }
        if(_ConceptionKind02 == null){
            _ConceptionKind02 = coreRealm.createConceptionKind(testConceptionKindName2,null);
        }

        RelationKind _RelationKind01 = coreRealm.getRelationKind("RelationKind0001ForTest");
        if(_RelationKind01 != null){
            coreRealm.removeRelationKind("RelationKind0001ForTest",true);
        }
        _RelationKind01 = coreRealm.getRelationKind("RelationKind0001ForTest");
        if(_RelationKind01 == null){
            _RelationKind01 = coreRealm.createRelationKind("RelationKind0001ForTest",null);
        }

        for(int i =0;i<10;i++){
            Map<String,Object> newEntityValue= new HashMap<>();
            newEntityValue.put("prop1",Math.random());
            newEntityValue.put("prop2",Math.random()+100000);
            EntityValue entityValue = new EntityValue(newEntityValue);

            Entity _Entity_1 = _ConceptionKind01.newEntity(entityValue,false);
            Entity _Entity_2 = _ConceptionKind02.newEntity(entityValue,false);

            Map<String,Object> relationPropertiesValue= new HashMap<>();
            relationPropertiesValue.put("relProp1",Math.random()*1000);
            relationPropertiesValue.put("temProp1", LocalTime.now());
            relationPropertiesValue.put("temProp2", new LocalDate[]{LocalDate.now()});

            RelationshipEntity resultRelationshipEntity = _Entity_1.attachFromRelation(_Entity_2.getEntityUID(),"RelationKind0001ForTest",relationPropertiesValue,false);
            Assert.assertNotNull(resultRelationshipEntity);
            Assert.assertNotNull(resultRelationshipEntity.getAttribute("temProp1").getAttributeValue());
            Assert.assertTrue(resultRelationshipEntity.getAttribute("temProp1").getAttributeValue() instanceof LocalTime);
            Assert.assertEquals(resultRelationshipEntity.getAttribute("temProp1").getAttributeDataType(),AttributeDataType.TIME);
            Assert.assertNotNull(resultRelationshipEntity.getAttribute("temProp2").getAttributeValue());
            Assert.assertTrue(resultRelationshipEntity.getAttribute("temProp2").getAttributeValue() instanceof LocalDate[]);
            Assert.assertEquals(resultRelationshipEntity.getAttribute("temProp2").getAttributeDataType(),AttributeDataType.DATE_ARRAY);
        }

        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setDistinctMode(true);
        RelationshipEntitiesRetrieveResult _RelationEntitiesRetrieveResult = _RelationKind01.getRelationEntities(queryParameters);

        Assert.assertNotNull(_RelationEntitiesRetrieveResult);
        Assert.assertNotNull(_RelationEntitiesRetrieveResult.getOperationStatistics());

        Assert.assertNotNull(_RelationEntitiesRetrieveResult.getOperationStatistics().getQueryParameters());
        Assert.assertEquals(_RelationEntitiesRetrieveResult.getOperationStatistics().getResultEntitiesCount(),10l);

        Assert.assertNotNull(_RelationEntitiesRetrieveResult.getOperationStatistics().getStartTime());
        Assert.assertNotNull(_RelationEntitiesRetrieveResult.getOperationStatistics().getFinishTime());

        Assert.assertNotNull(_RelationEntitiesRetrieveResult.getRelationEntities());
        Assert.assertEquals(_RelationEntitiesRetrieveResult.getRelationEntities().size(),10);

        for(RelationshipEntity currentRelationshipEntity : _RelationEntitiesRetrieveResult.getRelationEntities()){
            Assert.assertEquals(currentRelationshipEntity.getRelationKindName(),"RelationKind0001ForTest");
            Assert.assertNotNull(currentRelationshipEntity.getFromEntityUID());
            Assert.assertNotNull(currentRelationshipEntity.getToEntityUID());
        }

        queryParameters.setResultNumber(7);
        _RelationEntitiesRetrieveResult = _RelationKind01.getRelationEntities(queryParameters);
        Assert.assertNotNull(_RelationEntitiesRetrieveResult);
        Assert.assertEquals(_RelationEntitiesRetrieveResult.getRelationEntities().size(),7);

        queryParameters.setResultNumber(20);
        _RelationEntitiesRetrieveResult = _RelationKind01.getRelationEntities(queryParameters);
        Assert.assertNotNull(_RelationEntitiesRetrieveResult);
        Assert.assertEquals(_RelationEntitiesRetrieveResult.getRelationEntities().size(),10);

        queryParameters.setResultNumber(500);
        queryParameters.setDefaultFilteringItem(new GreaterThanEqualFilteringItem("relProp1",500l));
        _RelationEntitiesRetrieveResult = _RelationKind01.getRelationEntities(queryParameters);
        Assert.assertNotNull(_RelationEntitiesRetrieveResult);
        Assert.assertTrue(_RelationEntitiesRetrieveResult.getRelationEntities().size()<10);

        AttributesParameters attributesParameters = new AttributesParameters();
        attributesParameters.setDefaultFilteringItem(new GreaterThanEqualFilteringItem("relProp1",500l));
        Long entityCount = _RelationKind01.countRelationEntities(attributesParameters,true);
        long res1 = (_RelationEntitiesRetrieveResult.getOperationStatistics().getResultEntitiesCount());
        Assert.assertEquals(res1,entityCount.longValue());

        Assert.assertEquals(_RelationKind01.countRelationEntities(),new Long(10));

        Set<RelationshipEntity> relationEntitySet = _RelationKind01.getRandomEntities(5);
        Assert.assertNotNull(relationEntitySet);
        Assert.assertEquals(relationEntitySet.size(),5);
        for(RelationshipEntity currentEntity:relationEntitySet){
            Assert.assertEquals(currentEntity.getRelationKindName(),"RelationKind0001ForTest");
            Assert.assertNotNull(currentEntity.getRelationshipEntityUID());
        }

        AttributesParameters attributesParameters2 = new AttributesParameters();
        attributesParameters2.setDefaultFilteringItem(new NullValueFilteringItem("mustNotExistAttr"));
        relationEntitySet = _RelationKind01.getRandomEntities(attributesParameters2,true,5);
        Assert.assertNotNull(relationEntitySet);
        Assert.assertEquals(relationEntitySet.size(),5);
        for(RelationshipEntity currentEntity:relationEntitySet){
            Assert.assertEquals(currentEntity.getRelationKindName(),"RelationKind0001ForTest");
            Assert.assertNotNull(currentEntity.getRelationshipEntityUID());
        }

        attributesParameters2 = new AttributesParameters();
        attributesParameters2.setDefaultFilteringItem(new EqualFilteringItem("mustNotExistAttr",1000));
        relationEntitySet = _RelationKind01.getRandomEntities(attributesParameters2,false,5);
        Assert.assertNotNull(relationEntitySet);
        Assert.assertEquals(relationEntitySet.size(),0);

        EntitiesOperationResult purgeAllOperationResult = _RelationKind01.purgeAllRelationEntities();
        Assert.assertNotNull(purgeAllOperationResult);
        Assert.assertNotNull(purgeAllOperationResult.getOperationStatistics());

        Assert.assertEquals(purgeAllOperationResult.getOperationStatistics().getSuccessItemsCount(),10);
        Assert.assertEquals(purgeAllOperationResult.getOperationStatistics().getFailItemsCount(),0);

        Assert.assertNotNull(purgeAllOperationResult.getOperationStatistics().getStartTime());
        Assert.assertNotNull(purgeAllOperationResult.getOperationStatistics().getFinishTime());
        Assert.assertNotNull(purgeAllOperationResult.getOperationStatistics().getOperationSummary());

        Assert.assertEquals(_RelationKind01.countRelationEntities(),new Long(0));

        long selfAttachedRemoveResult = _RelationKind01.purgeRelationsOfSelfAttachedConceptionEntities();
        Assert.assertEquals(selfAttachedRemoveResult,0);

        Map<String,Object> newEntityValue= new HashMap<>();
        EntityValue entityValue = new EntityValue(newEntityValue);
        Entity _Entity_3 = _ConceptionKind01.newEntity(entityValue,false);

        for(int i=0;i<10;i++) {
            _Entity_3.attachFromRelation(_Entity_3.getEntityUID(), "RelationKind0001ForTest", null, false);
        }
        selfAttachedRemoveResult = _RelationKind01.purgeRelationsOfSelfAttachedConceptionEntities();
        Assert.assertEquals(selfAttachedRemoveResult,1);

        List<String> relationEntityUIDList = new ArrayList<>();
        for(int i=0;i<10;i++) {
            RelationshipEntity currentRelationshipEntity = _Entity_3.attachFromRelation(_Entity_3.getEntityUID(), "RelationKind0001ForTest", null, true);
            relationEntityUIDList.add(currentRelationshipEntity.getRelationshipEntityUID());
        }

        boolean exceptionShouldThrown = false;
        try {
            _RelationKind01.deleteEntity("12345");
        }catch(EngineServiceException e){
            exceptionShouldThrown = true;
        }
        Assert.assertTrue(exceptionShouldThrown);

        RelationshipEntity targetRelationshipEntity = _RelationKind01.getEntityByUID(relationEntityUIDList.get(0));
        Assert.assertNotNull(targetRelationshipEntity);
        Assert.assertNotNull(targetRelationshipEntity.getFromEntityKinds());
        Assert.assertNotNull(targetRelationshipEntity.getToEntityKinds());
        Assert.assertEquals(targetRelationshipEntity.getFromEntityKinds().get(0),testConceptionKindName);
        Assert.assertEquals(targetRelationshipEntity.getToEntityKinds().get(0),testConceptionKindName);

        boolean  deleteSingleEntityResult = _RelationKind01.deleteEntity(relationEntityUIDList.get(0));
        Assert.assertTrue(deleteSingleEntityResult);

        List<String> uidsForMultiDelete = new ArrayList<>();
        uidsForMultiDelete.add(relationEntityUIDList.get(1));
        uidsForMultiDelete.add(relationEntityUIDList.get(2));
        uidsForMultiDelete.add("1234567890");
        EntitiesOperationResult entitiesOperationResult = _RelationKind01.deleteEntities(uidsForMultiDelete);
        Assert.assertEquals(entitiesOperationResult.getSuccessEntityUIDs().size(),2);
        Assert.assertEquals(entitiesOperationResult.getOperationStatistics().getFailItemsCount(),1);

        selfAttachedRemoveResult = _RelationKind01.purgeRelationsOfSelfAttachedConceptionEntities();
        Assert.assertEquals(selfAttachedRemoveResult,10-3);

        boolean updateDescResult = _RelationKind01.updateRelationKindDesc("TestRelationKindADesc+中文描述UPD");
        Assert.assertTrue(updateDescResult);
        Assert.assertEquals(_RelationKind01.getRelationKindDesc(),"TestRelationKindADesc+中文描述UPD");
        Assert.assertEquals(coreRealm.getRelationKind("RelationKind0001ForTest").getRelationKindDesc(),"TestRelationKindADesc+中文描述UPD");
        coreRealm.closeGlobalSession();
    }
}
