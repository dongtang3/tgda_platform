package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.operator.CrossKindDataOperator;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.EntitiesOperationResult;
import com.github.tgda.engine.core.payload.RelationshipEntityValue;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.RelationshipEntity;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CrossKindDataOperatorTest {

    private static String testRealmName = "UNIT_TEST_Realm";
    private static String testConceptionKindName1 = "CrossKindTestKind01";
    private static String testConceptionKindName2 = "CrossKindTestKind02";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for CrossKindDataOperatorTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testCrossKindDataOperatorFunction() throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        coreRealm.openGlobalSession();
        Assert.assertEquals(coreRealm.getStorageImplTech(), StorageImplTech.NEO4J);

        Type _Type01 = coreRealm.getType(testConceptionKindName1);
        if(_Type01 == null){
            _Type01 = coreRealm.createType(testConceptionKindName1, testConceptionKindName1 +"中文描述");
            Assert.assertNotNull(_Type01);
            Assert.assertEquals(_Type01.getConceptionKindName(), testConceptionKindName1);
            Assert.assertEquals(_Type01.getConceptionKindDesc(), testConceptionKindName1 +"中文描述");
        }

        EntitiesOperationResult purgeEntitiesOperationResult1 = _Type01.purgeAllEntities();

        Type _Type02 = coreRealm.getType(testConceptionKindName2);
        if(_Type02 == null){
            _Type02 = coreRealm.createType(testConceptionKindName2, testConceptionKindName2 +"中文描述");
            Assert.assertNotNull(_Type02);
            Assert.assertEquals(_Type02.getConceptionKindName(), testConceptionKindName2);
            Assert.assertEquals(_Type02.getConceptionKindDesc(), testConceptionKindName2 +"中文描述");
        }

        EntitiesOperationResult purgeEntitiesOperationResult2 = _Type02.purgeAllEntities();

        List<String> uidList = new ArrayList<>();
        for(int i=0;i<10;i++){
            Map<String,Object> newEntityValue= new HashMap<>();
            newEntityValue.put("prop1",i);
            newEntityValue.put("prop2","StringValueOf "+i);
            EntityValue entityValue = new EntityValue(newEntityValue);
            Entity _Entity = _Type01.newEntity(entityValue,false);
            uidList.add(_Entity.getEntityUID());
        }
        uidList.add("10000000000000");// means a not exist entity's UID

        CrossKindDataOperator targetCrossKindDataOperator = coreRealm.getCrossKindDataOperator();

        List<Entity> targetEntityList = targetCrossKindDataOperator.getConceptionEntitiesByUIDs(uidList);

        Assert.assertEquals(targetEntityList.size(),10);
        for(Entity currentEntity : targetEntityList){
            Assert.assertEquals(currentEntity.getConceptionKindName(), testConceptionKindName1);
            Assert.assertTrue(uidList.contains(currentEntity.getEntityUID()));
        }

        String relationAttachTargetEntityUID = uidList.get(3);

        List<String> conceptionEntityPairUIDList = new ArrayList<>();
        conceptionEntityPairUIDList.add(relationAttachTargetEntityUID);

        List<String> relationUidList = new ArrayList<>();
        for(int i=0;i<50;i++){
            Map<String,Object> newEntityValue= new HashMap<>();
            newEntityValue.put("propA",i);
            newEntityValue.put("propB","String-"+i);
            Map<String,Object> newRelationshipEntityValue= new HashMap<>();
            newRelationshipEntityValue.put("propR1",i);
            newRelationshipEntityValue.put("propR2","String-"+i);
            EntityValue entityValue = new EntityValue(newEntityValue);
            Entity _Entity = _Type02.newEntity(entityValue,false);
            conceptionEntityPairUIDList.add(_Entity.getEntityUID());
            RelationshipEntity resultRelationshipEntity = _Entity.attachFromRelation(relationAttachTargetEntityUID,"crossKindTestRelationKind",newRelationshipEntityValue,true);
            relationUidList.add(resultRelationshipEntity.getRelationshipEntityUID());
        }
        relationUidList.add("20000000000000");// means a not exist entity's UID

        List<RelationshipEntity> relationshipEntityList = targetCrossKindDataOperator.getRelationEntitiesByUIDs(relationUidList);

        Assert.assertEquals(relationshipEntityList.size(),50);
        for(RelationshipEntity currentRelationshipEntity : relationshipEntityList){
            Assert.assertEquals(currentRelationshipEntity.getRelationTypeName(), "crossKindTestRelationKind");
            Assert.assertTrue(relationUidList.contains(currentRelationshipEntity.getRelationshipEntityUID()));
        }

        List<RelationshipEntity> relationshipEntityList2 = targetCrossKindDataOperator.getRelationsOfEntityPair(conceptionEntityPairUIDList);
        Assert.assertEquals(relationshipEntityList2.size(),50);
        for(RelationshipEntity currentRelationshipEntity : relationshipEntityList2){
            Assert.assertEquals(currentRelationshipEntity.getRelationTypeName(),"crossKindTestRelationKind");
            Assert.assertEquals(relationAttachTargetEntityUID, currentRelationshipEntity.getToEntityUID());
            Assert.assertTrue(relationUidList.contains(currentRelationshipEntity.getRelationshipEntityUID()));
            Assert.assertTrue(conceptionEntityPairUIDList.contains(currentRelationshipEntity.getFromEntityUID()));
        }

        List<String> attributesNameList = new ArrayList<>();
        attributesNameList.add("prop1");
        attributesNameList.add("prop2");
        List<EntityValue> entityValueList = targetCrossKindDataOperator.getSingleValueEntityAttributesByUIDs(uidList,attributesNameList);

        Assert.assertEquals(entityValueList.size(),10);
        for(EntityValue currentEntityValue : entityValueList){
            Assert.assertTrue(uidList.contains(currentEntityValue.getEntityUID()));
            Assert.assertTrue(currentEntityValue.getEntityAttributesValue().containsKey(("prop1")));
            Assert.assertTrue(currentEntityValue.getEntityAttributesValue().containsKey(("prop2")));
        }

        attributesNameList.add("propR1");
        attributesNameList.add("propR2");
        List<RelationshipEntityValue> relationshipEntityValueList = targetCrossKindDataOperator.getSingleValueRelationshipEntityAttributesByUIDs(relationUidList,attributesNameList);
        Assert.assertEquals(relationshipEntityValueList.size(),50);
        for(RelationshipEntityValue currentRelationshipEntityValue : relationshipEntityValueList){
            Assert.assertTrue(!currentRelationshipEntityValue.getEntityAttributesValue().containsKey("prop1"));
            Assert.assertTrue(!currentRelationshipEntityValue.getEntityAttributesValue().containsKey("prop2"));
            Assert.assertTrue(currentRelationshipEntityValue.getEntityAttributesValue().containsKey(("propR1")));
            Assert.assertTrue(currentRelationshipEntityValue.getEntityAttributesValue().containsKey(("propR2")));
            Assert.assertTrue(relationUidList.contains(currentRelationshipEntityValue.getRelationshipEntityUID()));
            Assert.assertNotNull(currentRelationshipEntityValue.getFromEntityUID());
            Assert.assertNotNull(currentRelationshipEntityValue.getToEntityUID());
        }

        coreRealm.closeGlobalSession();
    }
}
