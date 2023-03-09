package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.exception.EngineFunctionNotSupportedException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.EntitiesOperationResult;
import com.github.tgda.engine.core.payload.RelationshipAttachLinkLogic;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.RelationshipAttach;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.*;

public class RelationshipAttachTest {

    private static String testRealmName = "UNIT_TEST_Realm";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for RelationAttachKindTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testRelationAttachKindFunction() throws EngineServiceRuntimeException, EngineFunctionNotSupportedException {
        Engine engine = EngineFactory.getDefaultEngine();
        Assert.assertEquals(engine.getStorageImplTech(), StorageImplTech.NEO4J);

        RelationshipAttach targetRelationshipAttach = engine.createRelationshipAttach("RelationAttachKindForUnitTest","RelationAttachKind_Desc",
                "RelationAttachKind_SourceKind","RelationAttachKind_TargetKind","RelationAttachKind_RelationKind",true);

        boolean updateResult = targetRelationshipAttach.updateRelationAttachKindDesc("RelationAttachKind_Desc2");
        Assert.assertTrue(updateResult);
        Assert.assertEquals(targetRelationshipAttach.getRelationAttachKindDesc(),"RelationAttachKind_Desc2");
        RelationshipAttach targetRelationshipAttach2 = engine.getRelationshipAttach(targetRelationshipAttach.getRelationAttachKindUID());
        Assert.assertEquals(targetRelationshipAttach2.getRelationAttachKindDesc(),"RelationAttachKind_Desc2");

        Assert.assertEquals(targetRelationshipAttach2.isRepeatableRelationKindAllow(),true);
        updateResult = targetRelationshipAttach.setAllowRepeatableRelationKind(false);
        Assert.assertFalse(updateResult);
        Assert.assertEquals(targetRelationshipAttach.isRepeatableRelationKindAllow(),false);
        targetRelationshipAttach2 = engine.getRelationshipAttach(targetRelationshipAttach.getRelationAttachKindUID());
        Assert.assertEquals(targetRelationshipAttach2.isRepeatableRelationKindAllow(),false);

        List<RelationshipAttachLinkLogic> attachLinkLogicList = targetRelationshipAttach2.getRelationAttachLinkLogic();
        Assert.assertNotNull(attachLinkLogicList);
        Assert.assertEquals(attachLinkLogicList.size(),0);

        RelationshipAttachLinkLogic relationshipAttachLinkLogic01 = new RelationshipAttachLinkLogic(RelationshipAttach.LinkLogicType.DEFAULT, RelationshipAttach.LinkLogicCondition.Equal,"knownPropertyName","unKnownPropertyName");
        RelationshipAttachLinkLogic resultRelationshipAttachLinkLogic = targetRelationshipAttach2.createRelationAttachLinkLogic(relationshipAttachLinkLogic01);
        Assert.assertNotNull(resultRelationshipAttachLinkLogic);
        Assert.assertNotNull(resultRelationshipAttachLinkLogic.getRelationAttachLinkLogicUID());

        attachLinkLogicList = targetRelationshipAttach2.getRelationAttachLinkLogic();
        Assert.assertNotNull(attachLinkLogicList);
        Assert.assertEquals(attachLinkLogicList.size(),1);

        Assert.assertEquals(attachLinkLogicList.get(0).getLinkLogicType(), RelationshipAttach.LinkLogicType.DEFAULT);
        Assert.assertEquals(attachLinkLogicList.get(0).getLinkLogicCondition(), RelationshipAttach.LinkLogicCondition.Equal);
        Assert.assertEquals(attachLinkLogicList.get(0).getSourceEntityLinkAttributeName(),"knownPropertyName");
        Assert.assertEquals(attachLinkLogicList.get(0).getTargetEntitiesLinkAttributeName(),"unKnownPropertyName");
        Assert.assertEquals(attachLinkLogicList.get(0).getRelationAttachLinkLogicUID(), resultRelationshipAttachLinkLogic.getRelationAttachLinkLogicUID());

        boolean exceptionShouldBeCaught = false;
        try{
            targetRelationshipAttach2.createRelationAttachLinkLogic(relationshipAttachLinkLogic01);
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        boolean removeResult = targetRelationshipAttach2.removeRelationAttachLinkLogic(resultRelationshipAttachLinkLogic.getRelationAttachLinkLogicUID());
        Assert.assertTrue(removeResult);

        attachLinkLogicList = targetRelationshipAttach2.getRelationAttachLinkLogic();
        Assert.assertNotNull(attachLinkLogicList);
        Assert.assertEquals(attachLinkLogicList.size(),0);

        exceptionShouldBeCaught = false;
        try{
            targetRelationshipAttach2.removeRelationAttachLinkLogic(resultRelationshipAttachLinkLogic.getRelationAttachLinkLogicUID()+"123");
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        relationshipAttachLinkLogic01 = new RelationshipAttachLinkLogic(RelationshipAttach.LinkLogicType.DEFAULT, RelationshipAttach.LinkLogicCondition.Equal,"knownPropertyName1","unKnownPropertyName1");
        resultRelationshipAttachLinkLogic = targetRelationshipAttach2.createRelationAttachLinkLogic(relationshipAttachLinkLogic01);
        Assert.assertNotNull(resultRelationshipAttachLinkLogic);
        Assert.assertNotNull(resultRelationshipAttachLinkLogic.getRelationAttachLinkLogicUID());

        relationshipAttachLinkLogic01 = new RelationshipAttachLinkLogic(RelationshipAttach.LinkLogicType.AND, RelationshipAttach.LinkLogicCondition.BeginWithSimilar,"knownPropertyName2","unKnownPropertyName2");
        resultRelationshipAttachLinkLogic = targetRelationshipAttach2.createRelationAttachLinkLogic(relationshipAttachLinkLogic01);
        Assert.assertNotNull(resultRelationshipAttachLinkLogic);
        Assert.assertNotNull(resultRelationshipAttachLinkLogic.getRelationAttachLinkLogicUID());

        relationshipAttachLinkLogic01 = new RelationshipAttachLinkLogic(RelationshipAttach.LinkLogicType.OR, RelationshipAttach.LinkLogicCondition.LessThan,"knownPropertyName3","unKnownPropertyName3");
        resultRelationshipAttachLinkLogic = targetRelationshipAttach2.createRelationAttachLinkLogic(relationshipAttachLinkLogic01);
        Assert.assertNotNull(resultRelationshipAttachLinkLogic);
        Assert.assertNotNull(resultRelationshipAttachLinkLogic.getRelationAttachLinkLogicUID());

        attachLinkLogicList = targetRelationshipAttach2.getRelationAttachLinkLogic();
        Assert.assertNotNull(attachLinkLogicList);
        Assert.assertEquals(attachLinkLogicList.size(),3);

        engine.removeRelationAttach(targetRelationshipAttach.getRelationAttachKindUID());

        engine.openGlobalSession();

        Type _Type01 = engine.getType("RelationAttachConceptionKind01");
        if(_Type01 != null){
            engine.removeType("RelationAttachConceptionKind01",true);
        }
        _Type01 = engine.getType("RelationAttachConceptionKind01");
        if(_Type01 == null){
            _Type01 = engine.createType("RelationAttachConceptionKind01","");
            Assert.assertNotNull(_Type01);
            Assert.assertEquals(_Type01.getConceptionKindName(),"RelationAttachConceptionKind01");
        }

        Type _Type02 = engine.getType("RelationAttachConceptionKind02");
        if(_Type02 != null){
            engine.removeType("RelationAttachConceptionKind02",true);
        }
        _Type02 = engine.getType("RelationAttachConceptionKind02");
        if(_Type02 == null){
            _Type02 = engine.createType("RelationAttachConceptionKind02","");
            Assert.assertNotNull(_Type02);
            Assert.assertEquals(_Type02.getConceptionKindName(),"RelationAttachConceptionKind02");
        }

        Type _Type03 = engine.getType("RelationAttachConceptionKind03");
        if(_Type03 != null){
            engine.removeType("RelationAttachConceptionKind03",true);
        }
        _Type03 = engine.getType("RelationAttachConceptionKind03");
        if(_Type03 == null){
            _Type03 = engine.createType("RelationAttachConceptionKind03","");
            Assert.assertNotNull(_Type03);
            Assert.assertEquals(_Type03.getConceptionKindName(),"RelationAttachConceptionKind03");
        }

        Map<String,Object> newEntityValueMap= new HashMap<>();
        newEntityValueMap.put("prop1",Long.parseLong("12345"));
        newEntityValueMap.put("prop2",Double.parseDouble("12345.789"));
        newEntityValueMap.put("prop3",Integer.parseInt("1234"));
        newEntityValueMap.put("prop4","thi is s string");
        newEntityValueMap.put("prop5",Boolean.valueOf("true"));

        for(int i=0;i<30;i++){
            newEntityValueMap.put("prop6","prop6Value"+i);
            EntityValue entityValue1 = new EntityValue(newEntityValueMap);
            _Type01.newEntity(entityValue1,false);
        }

        for(int i=0;i<50;i++){
            newEntityValueMap.put("prop7","prop7Value"+i);
            EntityValue entityValue1 = new EntityValue(newEntityValueMap);
            _Type02.newEntity(entityValue1,false);
        }

        RelationshipAttach targetRelationshipAttach3 = engine.createRelationshipAttach("RelationAttachKindForUnitTest3","RelationAttachKind_Desc3",
                "RelationAttachConceptionKind01","RelationAttachConceptionKind03","RAK_RelationKindA",true);
        RelationshipAttachLinkLogic relationshipAttachLinkLogicA = new RelationshipAttachLinkLogic(RelationshipAttach.LinkLogicType.DEFAULT, RelationshipAttach.LinkLogicCondition.Equal,"prop6","kprop1");
        targetRelationshipAttach3.createRelationAttachLinkLogic(relationshipAttachLinkLogicA);

        RelationshipAttach targetRelationshipAttach4 = engine.createRelationshipAttach("RelationAttachKindForUnitTest4","RelationAttachKind_Desc4",
                "RelationAttachConceptionKind03","RelationAttachConceptionKind02","RAK_RelationKindB",true);
        RelationshipAttachLinkLogic relationshipAttachLinkLogicB = new RelationshipAttachLinkLogic(RelationshipAttach.LinkLogicType.DEFAULT, RelationshipAttach.LinkLogicCondition.Equal,"kprop2","prop7");
        targetRelationshipAttach4.createRelationAttachLinkLogic(relationshipAttachLinkLogicB);

        String[] multiConceptionsArray = new String[]{"RelationAttachConceptionKind_Multi","RelationAttachConceptionKind03"};

        Map<String,Object> newEntityValueMap2= new HashMap<>();
        newEntityValueMap2.put("kprop1","prop6Value3");
        newEntityValueMap2.put("kprop2","prop7Value12");
        EntityValue entityValueC = new EntityValue(newEntityValueMap2);
        Entity resultEntity = _Type03.newEntity(entityValueC,true);
        Assert.assertEquals(resultEntity.countAllRelations().longValue(),2l);

        newEntityValueMap2 = new HashMap<>();
        newEntityValueMap2.put("kprop1","prop6Value3");
        newEntityValueMap2.put("kprop2","prop7Value12");
        EntityValue entityValueC_M = new EntityValue(newEntityValueMap2);
        Entity resultEntity_m = engine.newMultiEntity(multiConceptionsArray, entityValueC_M,true);
        Assert.assertEquals(resultEntity_m.countAllRelations().longValue(),2l);

        List<RelationshipAttach> relationshipAttachList = new ArrayList<>();
        relationshipAttachList.add(targetRelationshipAttach3);
        Map<String,Object> newEntityValueMap3= new HashMap<>();
        newEntityValueMap3.put("kprop1","prop6Value6");
        newEntityValueMap3.put("kprop2","prop7Value19");
        EntityValue entityValueD = new EntityValue(newEntityValueMap3);
        Entity resultEntity2 = _Type03.newEntity(entityValueD, relationshipAttachList, RelationshipAttach.EntityRelateRole.TARGET);
        Assert.assertEquals(resultEntity2.countAllRelations().longValue(),1l);

        relationshipAttachList = new ArrayList<>();
        relationshipAttachList.add(targetRelationshipAttach3);
        newEntityValueMap3= new HashMap<>();
        newEntityValueMap3.put("kprop1","prop6Value6");
        newEntityValueMap3.put("kprop2","prop7Value19");
        EntityValue entityValueD_m = new EntityValue(newEntityValueMap3);
        Entity resultEntity2_m = engine.newMultiEntity(multiConceptionsArray, entityValueD_m, relationshipAttachList, RelationshipAttach.EntityRelateRole.TARGET);
        Assert.assertEquals(resultEntity2_m.countAllRelations().longValue(),1l);

        List<EntityValue> entityValues = new ArrayList<>();
        Map<String,Object> newEntityValueMap4= new HashMap<>();
        newEntityValueMap4.put("kprop1","prop6Value15");
        newEntityValueMap4.put("kprop2","prop7Value21");
        EntityValue entityValueE = new EntityValue(newEntityValueMap4);
        entityValues.add(entityValueE);
        Map<String,Object> newEntityValueMap5= new HashMap<>();
        newEntityValueMap5.put("kprop1","prop6Value15");
        newEntityValueMap5.put("kprop2","prop7Value21");
        EntityValue entityValueF = new EntityValue(newEntityValueMap5);
        entityValues.add(entityValueF);

        _Type03.newEntities(entityValues,true);
        engine.newMultiConceptionEntities(multiConceptionsArray, entityValues,true);

        List<EntityValue> entityValues2 = new ArrayList<>();
        Map<String,Object> newEntityValueMap6= new HashMap<>();
        newEntityValueMap6.put("kprop1","prop6Value23");
        newEntityValueMap6.put("kprop2","prop7Value3");
        EntityValue entityValueG = new EntityValue(newEntityValueMap6);
        entityValues2.add(entityValueG);
        Map<String,Object> newEntityValueMap7= new HashMap<>();
        newEntityValueMap7.put("kprop1","prop6Value23");
        newEntityValueMap7.put("kprop2","prop7Value3");
        EntityValue entityValueH = new EntityValue(newEntityValueMap7);
        entityValues2.add(entityValueH);

        relationshipAttachList.clear();
        relationshipAttachList.add(targetRelationshipAttach4);
        _Type03.newEntities(entityValues2, relationshipAttachList, RelationshipAttach.EntityRelateRole.SOURCE);
        engine.newMultiConceptionEntities(multiConceptionsArray, entityValues2, relationshipAttachList, RelationshipAttach.EntityRelateRole.SOURCE);

        Type _Type04 = engine.getType("RelationAttachConceptionKind04");
        if(_Type04 != null){
            engine.removeType("RelationAttachConceptionKind04",true);
        }
        _Type04 = engine.getType("RelationAttachConceptionKind04");
        if(_Type04 == null){
            _Type04 = engine.createType("RelationAttachConceptionKind04","");
            Assert.assertNotNull(_Type04);
            Assert.assertEquals(_Type04.getConceptionKindName(),"RelationAttachConceptionKind04");
        }

        Type _Type05 = engine.getType("RelationAttachConceptionKind05");
        if(_Type05 != null){
            engine.removeType("RelationAttachConceptionKind05",true);
        }
        _Type05 = engine.getType("RelationAttachConceptionKind05");
        if(_Type05 == null){
            _Type05 = engine.createType("RelationAttachConceptionKind05","");
            Assert.assertNotNull(_Type05);
            Assert.assertEquals(_Type05.getConceptionKindName(),"RelationAttachConceptionKind05");
        }

        Map<String,Object> newEntityValueMap8= new HashMap<>();
        newEntityValueMap8.put("prop1",Long.parseLong("12345"));
        newEntityValueMap8.put("prop2",Double.parseDouble("12345.789"));
        newEntityValueMap8.put("prop3",Integer.parseInt("1234"));

        for(int i=0;i<50;i++){
            newEntityValueMap8.put("prop_CK4","CK4Value"+i);
            EntityValue entityValue1 = new EntityValue(newEntityValueMap8);
            _Type04.newEntity(entityValue1,false);
        }
        for(int i=0;i<2;i++){
            newEntityValueMap8.put("prop_CK4","COMMON_VALUE");
            EntityValue entityValue1 = new EntityValue(newEntityValueMap8);
            _Type04.newEntity(entityValue1,false);
        }

        newEntityValueMap8.remove("prop_CK4");

        for(int i=0;i<100;i++){
            newEntityValueMap8.put("prop_CK5","CK5Value"+i);
            EntityValue entityValue1 = new EntityValue(newEntityValueMap8);
            _Type05.newEntity(entityValue1,false);
        }

        for(int i=0;i<5;i++){
            newEntityValueMap8.put("prop_CK5","COMMON_VALUE");
            EntityValue entityValue1 = new EntityValue(newEntityValueMap8);
            _Type05.newEntity(entityValue1,false);
        }

        RelationshipAttach targetRelationshipAttach5 = engine.createRelationshipAttach("RelationAttachKindForUnitTest5","RelationAttachKind_Desc5",
                "RelationAttachConceptionKind04","RelationAttachConceptionKind05","RAK_RelationKindC01",true);
        RelationshipAttachLinkLogic relationshipAttachLinkLogicC = new RelationshipAttachLinkLogic(RelationshipAttach.LinkLogicType.DEFAULT, RelationshipAttach.LinkLogicCondition.Equal,"prop_CK4","prop_CK5");
        targetRelationshipAttach5.createRelationAttachLinkLogic(relationshipAttachLinkLogicC);

        Map<String,Object> relationDataMap = new HashMap<>();
        relationDataMap.put("relProp",1000);
        EntitiesOperationResult entitiesOperationResult = targetRelationshipAttach5.newUniversalRelationEntities(relationDataMap);
        Assert.assertEquals(entitiesOperationResult.getOperationStatistics().getSuccessItemsCount(),10l);

        engine.removeRelationAttach(targetRelationshipAttach3.getRelationAttachKindUID());
        engine.removeRelationAttach(targetRelationshipAttach4.getRelationAttachKindUID());
        engine.removeRelationAttach(targetRelationshipAttach5.getRelationAttachKindUID());
        engine.closeGlobalSession();
    }
}
