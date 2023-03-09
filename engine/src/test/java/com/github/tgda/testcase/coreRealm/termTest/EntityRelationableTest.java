package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.ResultEntitiesParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.EqualFilteringItem;
import com.github.tgda.engine.core.analysis.query.filteringItem.FilteringItem;
import com.github.tgda.engine.core.analysis.query.filteringItem.GreaterThanFilteringItem;
import com.github.tgda.engine.core.analysis.query.filteringItem.LessThanFilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.payload.EntitiesAttributesRetrieveResult;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.coreRealm.realmServiceCore.term.*;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEngineImpl;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.*;

public class EntityRelationableTest {

    private static String testRealmName = "UNIT_TEST_Realm";
    private static String testConceptionKindName = "TestConceptionKindForRelationableTest";
    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for EntityRelationableTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testEntityRelationableFunction() throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        CoreRealm coreRealm = EngineFactory.getDefaultEngine();
        Assert.assertEquals(coreRealm.getStorageImplTech(), StorageImplTech.NEO4J);

        ConceptionKind _ConceptionKind01 = coreRealm.getConceptionKind(testConceptionKindName);
        if(_ConceptionKind01 != null){
            coreRealm.removeConceptionKind(testConceptionKindName,true);
        }
        _ConceptionKind01 = coreRealm.getConceptionKind(testConceptionKindName);
        if(_ConceptionKind01 == null){
            _ConceptionKind01 = coreRealm.createConceptionKind(testConceptionKindName,"TestConceptionKindADesc+中文描述");
        }

        Map<String,Object> newEntityValue1= new HashMap<>();
        newEntityValue1.put("prop1","fromEntity");
        EntityValue entityValue1 = new EntityValue(newEntityValue1);
        Entity _Entity1 = _ConceptionKind01.newEntity(entityValue1,false);

        Map<String,Object> newEntityValue2= new HashMap<>();
        newEntityValue2.put("prop1","toEntity");
        EntityValue entityValue2 = new EntityValue(newEntityValue2);
        Entity _Entity2 = _ConceptionKind01.newEntity(entityValue2,false);

        Map<String,Object> newRelationValue= new HashMap<>();
        newRelationValue.put("prop1",10000l);
        newRelationValue.put("prop2",190.22d);
        newRelationValue.put("prop3",50);
        newRelationValue.put("prop4","thi is s string");
        newRelationValue.put("prop5","我是中文string");

        Assert.assertEquals(_Entity1.countAllRelations(),new Long(0));
        Assert.assertEquals(_Entity2.countAllRelations(),new Long(0));
        Assert.assertEquals(_Entity1.getAllRelations().size(),0);

        RelationshipEntity relationEntity01 = _Entity1.attachFromRelation(_Entity2.getEntityUID(),"testRelationType01",newRelationValue,true);

        Assert.assertEquals(_Entity1.countAllRelations(),new Long(1));
        Assert.assertEquals(_Entity2.countAllRelations(),new Long(1));
        Assert.assertEquals(_Entity1.getAllRelations().size(),1);

        Assert.assertEquals(_Entity1.getAllRelations().get(0).getRelationshipEntityUID(),relationEntity01.getRelationshipEntityUID());
        Assert.assertEquals(_Entity1.getAllRelations().get(0).getFromEntityUID(),_Entity1.getEntityUID());
        Assert.assertEquals(_Entity1.getAllRelations().get(0).getToEntityUID(),_Entity2.getEntityUID());

        RelationshipEntity relationEntity02 = _Entity1.attachToRelation(_Entity2.getEntityUID(),"testRelationType02",newRelationValue,true);

        Assert.assertEquals(_Entity1.countAllRelations(),new Long(2));
        Assert.assertEquals(_Entity2.countAllRelations(),new Long(2));
        Assert.assertEquals(_Entity1.getAllRelations().size(),2);

        List<RelationshipEntity> relationEntityList1 = _Entity1.getAllSpecifiedRelations(null, Direction.TWO_WAY);
        Assert.assertEquals(relationEntityList1.size(),2);
        relationEntityList1 = _Entity1.getAllSpecifiedRelations(null, Direction.FROM);
        Assert.assertEquals(relationEntityList1.size(),1);
        Assert.assertEquals(relationEntityList1.get(0).getRelationshipEntityUID(),relationEntity01.getRelationshipEntityUID());
        relationEntityList1 = _Entity1.getAllSpecifiedRelations(null, Direction.TO);
        Assert.assertEquals(relationEntityList1.size(),1);
        Assert.assertEquals(relationEntityList1.get(0).getRelationshipEntityUID(),relationEntity02.getRelationshipEntityUID());

        relationEntityList1 = _Entity1.getAllSpecifiedRelations("RelationKindNotExist", Direction.TWO_WAY);
        Assert.assertEquals(relationEntityList1.size(),0);
        Assert.assertEquals(_Entity1.countAllSpecifiedRelations("RelationKindNotExist", Direction.TWO_WAY),new Long(0));

        relationEntityList1 = _Entity1.getAllSpecifiedRelations("testRelationType01", Direction.TWO_WAY);
        Assert.assertEquals(relationEntityList1.size(),1);
        Assert.assertEquals(_Entity1.countAllSpecifiedRelations("testRelationType01", Direction.TWO_WAY),new Long(1));

        relationEntityList1 = _Entity1.getAllSpecifiedRelations("testRelationType01", Direction.FROM);
        Assert.assertEquals(relationEntityList1.size(),1);
        Assert.assertEquals(_Entity1.countAllSpecifiedRelations("testRelationType01", Direction.FROM),new Long(1));

        relationEntityList1 = _Entity1.getAllSpecifiedRelations("testRelationType01", Direction.TO);
        Assert.assertEquals(relationEntityList1.size(),0);
        Assert.assertEquals(_Entity1.countAllSpecifiedRelations("testRelationType01", Direction.TO),new Long(0));

        QueryParameters queryParameters1 = new QueryParameters();

        FilteringItem defaultFilteringItem = new EqualFilteringItem("prop1",10000l);
        queryParameters1.setDefaultFilteringItem(defaultFilteringItem);
        FilteringItem orFilteringItem = new EqualFilteringItem("prop3",50);
        queryParameters1.addFilteringItem(orFilteringItem, QueryParameters.FilteringLogic.OR);
        queryParameters1.addSortingAttribute("prop4", QueryParameters.SortingLogic.ASC);

        List<RelationshipEntity> relationEntityList2 = _Entity1.getSpecifiedRelations(queryParameters1,Direction.TWO_WAY);
        Assert.assertEquals(relationEntityList2.size(),4);
        Long countSpecifiedRelations = _Entity1.countSpecifiedRelations(queryParameters1,Direction.TWO_WAY);
        Assert.assertEquals(countSpecifiedRelations,new Long(4));

        queryParameters1.setDistinctMode(true);
        relationEntityList2 = _Entity1.getSpecifiedRelations(queryParameters1,Direction.TWO_WAY);
        Assert.assertEquals(relationEntityList2.size(),2);
        countSpecifiedRelations = _Entity1.countSpecifiedRelations(queryParameters1,Direction.TWO_WAY);
        Assert.assertEquals(countSpecifiedRelations,new Long(2));

        //use batch operation mode way 1
        GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
        CoreRealm coreRealm2 = EngineFactory.getDefaultEngine();
        ((Neo4JEngineImpl)coreRealm2).setGlobalGraphOperationExecutor(graphOperationExecutor);
        _ConceptionKind01 = coreRealm2.getConceptionKind(testConceptionKindName);
        try{
            Map<String,Object> newEntityValue3= new HashMap<>();
            newEntityValue3.put("prop1ForRelTest","Entity3");
            EntityValue entityValue3 = new EntityValue(newEntityValue3);
            Entity _Entity3 = _ConceptionKind01.newEntity(entityValue3,false);

            Map<String,Object> newEntityValue4= new HashMap<>();
            newEntityValue4.put("prop1ForRelTest","Entity4");
            EntityValue entityValue4 = new EntityValue(newEntityValue4);
            Entity _Entity4 = _ConceptionKind01.newEntity(entityValue4,false);

            for(int i = 0; i<100;i++){
                Map<String,Object> relationValue= new HashMap<>();
                relationValue.put("prop1",10000l+i);
                relationValue.put("prop2",190.22d+i);
                relationValue.put("prop3",50+i);
                relationValue.put("prop4","thi is s stringA"+i);
                _Entity3.attachFromRelation(_Entity4.getEntityUID(),"testRelationTypeA",relationValue,true);
            }

            for(int i = 0; i<100;i++){
                Map<String,Object> relationValue= new HashMap<>();
                relationValue.put("prop2",1000.22d+i);
                relationValue.put("prop3",600+i);
                relationValue.put("prop4","thi is s stringB"+i);
                _Entity3.attachToRelation(_Entity4.getEntityUID(),"testRelationTypeA",relationValue,true);
            }

            for(int i = 0; i<50;i++){
                Map<String,Object> relationValue= new HashMap<>();
                relationValue.put("prop1",10000l+i);
                relationValue.put("prop2",190.22d+i);
                relationValue.put("prop3",100000+i);
                relationValue.put("prop4","thi is s stringA"+i);
                _Entity3.attachFromRelation(_Entity4.getEntityUID(),"testRelationTypeB",relationValue,true);
            }

            for(int i = 0; i<50;i++){
                Map<String,Object> relationValue= new HashMap<>();
                relationValue.put("prop2",1000.22d+i);
                relationValue.put("prop3",50000+i);
                relationValue.put("prop4","thi is s stringB"+i);
                _Entity3.attachFromRelation(_Entity4.getEntityUID(),"testRelationTypeB",relationValue,true);
            }

            QueryParameters queryParameters2 = new QueryParameters();

            FilteringItem defaultFilteringItem2 = new EqualFilteringItem("prop1",10000l);
            queryParameters2.setDefaultFilteringItem(defaultFilteringItem2);
            List<RelationshipEntity> relationEntityList3 = _Entity3.getSpecifiedRelations(queryParameters2,Direction.TWO_WAY);
            Assert.assertEquals(relationEntityList3.size(),2);
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.TWO_WAY),new Long("2"));
            Assert.assertEquals(_Entity4.countSpecifiedRelations(queryParameters2,Direction.TWO_WAY),new Long("2"));

            FilteringItem defaultFilteringItem3 = new GreaterThanFilteringItem("prop3",50019);
            queryParameters2.setDefaultFilteringItem(defaultFilteringItem3);
            relationEntityList3 = _Entity3.getSpecifiedRelations(queryParameters2,Direction.FROM);
            Assert.assertEquals(relationEntityList3.size(),80);
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.FROM),new Long("80"));
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.TO),new Long("0"));
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.TWO_WAY),new Long("80"));

            FilteringItem andFilteringItem1 = new LessThanFilteringItem("prop2",1037);
            queryParameters2.addFilteringItem(andFilteringItem1, QueryParameters.FilteringLogic.AND);
            relationEntityList3 = _Entity3.getSpecifiedRelations(queryParameters2,Direction.FROM);
            Assert.assertEquals(relationEntityList3.size(),67);
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.FROM),new Long("67"));
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.TO),new Long("0"));
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.TWO_WAY),new Long("67"));

        }finally {
            graphOperationExecutor.close();
        }

        //use batch operation mode way 2
        CoreRealm coreRealm3 = EngineFactory.getDefaultEngine();
        coreRealm3.openGlobalSession();

            _ConceptionKind01 = coreRealm3.getConceptionKind(testConceptionKindName);
            Map<String,Object> newEntityValue3= new HashMap<>();
            newEntityValue3.put("prop1ForRelTest","Entity3");
            EntityValue entityValue3 = new EntityValue(newEntityValue3);
            Entity _Entity3 = _ConceptionKind01.newEntity(entityValue3,false);

            Map<String,Object> newEntityValue4= new HashMap<>();
            newEntityValue4.put("prop1ForRelTest","Entity4");
            EntityValue entityValue4 = new EntityValue(newEntityValue4);
            Entity _Entity4 = _ConceptionKind01.newEntity(entityValue4,false);

            for(int i = 0; i<100;i++){
                Map<String,Object> relationValue= new HashMap<>();
                relationValue.put("prop1",10000l+i);
                relationValue.put("prop2",190.22d+i);
                relationValue.put("prop3",50+i);
                relationValue.put("prop4","thi is s stringA"+i);
                _Entity3.attachFromRelation(_Entity4.getEntityUID(),"testRelationTypeA",relationValue,true);
            }

            for(int i = 0; i<100;i++){
                Map<String,Object> relationValue= new HashMap<>();
                relationValue.put("prop2",1000.22d+i);
                relationValue.put("prop3",600+i);
                relationValue.put("prop4","thi is s stringB"+i);
                _Entity3.attachToRelation(_Entity4.getEntityUID(),"testRelationTypeA",relationValue,true);
            }

            for(int i = 0; i<50;i++){
                Map<String,Object> relationValue= new HashMap<>();
                relationValue.put("prop1",10000l+i);
                relationValue.put("prop2",190.22d+i);
                relationValue.put("prop3",100000+i);
                relationValue.put("prop4","thi is s stringA"+i);
                _Entity3.attachFromRelation(_Entity4.getEntityUID(),"testRelationTypeB",relationValue,true);
            }

            for(int i = 0; i<50;i++){
                Map<String,Object> relationValue= new HashMap<>();
                relationValue.put("prop2",1000.22d+i);
                relationValue.put("prop3",50000+i);
                relationValue.put("prop4","thi is s stringB"+i);
                _Entity3.attachFromRelation(_Entity4.getEntityUID(),"testRelationTypeB",relationValue,true);
            }

            QueryParameters queryParameters2 = new QueryParameters();

            FilteringItem defaultFilteringItem2 = new EqualFilteringItem("prop1",10000l);
            queryParameters2.setDefaultFilteringItem(defaultFilteringItem2);
            List<RelationshipEntity> relationEntityList3 = _Entity3.getSpecifiedRelations(queryParameters2,Direction.TWO_WAY);
            Assert.assertEquals(relationEntityList3.size(),2);
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.TWO_WAY),new Long("2"));
            Assert.assertEquals(_Entity4.countSpecifiedRelations(queryParameters2,Direction.TWO_WAY),new Long("2"));

            FilteringItem defaultFilteringItem3 = new GreaterThanFilteringItem("prop3",50019);
            queryParameters2.setDefaultFilteringItem(defaultFilteringItem3);
            relationEntityList3 = _Entity3.getSpecifiedRelations(queryParameters2,Direction.FROM);
            Assert.assertEquals(relationEntityList3.size(),80);
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.FROM),new Long("80"));
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.TO),new Long("0"));
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.TWO_WAY),new Long("80"));

            FilteringItem andFilteringItem1 = new LessThanFilteringItem("prop2",1037);
            queryParameters2.addFilteringItem(andFilteringItem1, QueryParameters.FilteringLogic.AND);
            relationEntityList3 = _Entity3.getSpecifiedRelations(queryParameters2,Direction.FROM);
            Assert.assertEquals(relationEntityList3.size(),67);
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.FROM),new Long("67"));
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.TO),new Long("0"));
            Assert.assertEquals(_Entity3.countSpecifiedRelations(queryParameters2,Direction.TWO_WAY),new Long("67"));

        coreRealm3.closeGlobalSession();

        ConceptionKind _ConceptionKind0A = coreRealm.getConceptionKind(testConceptionKindName+"A");
        if(_ConceptionKind0A != null){
            coreRealm.removeConceptionKind(testConceptionKindName+"A",true);
        }
        _ConceptionKind0A = coreRealm.getConceptionKind(testConceptionKindName+"A");
        if(_ConceptionKind0A == null){
            _ConceptionKind0A = coreRealm.createConceptionKind(testConceptionKindName+"A","TestConceptionKindADesc+中文描述");
        }

        ConceptionKind _ConceptionKind0B = coreRealm.getConceptionKind(testConceptionKindName+"B");
        if(_ConceptionKind0B != null){
            coreRealm.removeConceptionKind(testConceptionKindName+"B",true);
        }
        _ConceptionKind0B = coreRealm.getConceptionKind(testConceptionKindName+"B");
        if(_ConceptionKind0B == null){
            _ConceptionKind0B = coreRealm.createConceptionKind(testConceptionKindName+"B","TestConceptionKindADesc+中文描述");
        }

        ConceptionKind _ConceptionKind0C = coreRealm.getConceptionKind(testConceptionKindName+"C");
        if(_ConceptionKind0C != null){
            coreRealm.removeConceptionKind(testConceptionKindName+"C",true);
        }
        _ConceptionKind0C = coreRealm.getConceptionKind(testConceptionKindName+"C");
        if(_ConceptionKind0C == null){
            _ConceptionKind0C = coreRealm.createConceptionKind(testConceptionKindName+"C","TestConceptionKindADesc+中文描述");
        }

        Map<String,Object> newEntityValueCommon= new HashMap<>();
        newEntityValueCommon.put("prop1",10000);
        newEntityValueCommon.put("prop2",20000);
        newEntityValueCommon.put("prop3",30000);
        EntityValue entityValueCommon1 = new EntityValue(newEntityValueCommon);
        Entity _EntityA = _ConceptionKind0A.newEntity(entityValueCommon1,false);

        Map<String,Object> newEntityValueCommon2= new HashMap<>();
        EntityValue entityValueCommon2 = new EntityValue(newEntityValueCommon2);
        Entity _EntityB1 = _ConceptionKind0B.newEntity(entityValueCommon2,false);
        _EntityB1.addAttribute("kindName","ConceptionKind0B");
        Entity _EntityB2 = _ConceptionKind0B.newEntity(entityValueCommon2,false);
        _EntityB2.addAttribute("kindName","ConceptionKind0B");

        Map<String,Object> newEntityValueCommon3= new HashMap<>();
        EntityValue entityValueCommon3 = new EntityValue(newEntityValueCommon3);
        Entity _EntityC1 = _ConceptionKind0C.newEntity(entityValueCommon3,false);
        _EntityC1.addAttribute("kindName","ConceptionKind0C");
        Entity _EntityC2 = _ConceptionKind0C.newEntity(entityValueCommon3,false);
        _EntityC2.addAttribute("kindName","ConceptionKind0C");
        Entity _EntityC3 = _ConceptionKind0C.newEntity(entityValueCommon3,false);
        _EntityC3.addAttribute("kindName","ConceptionKind0C");

        _EntityA.attachFromRelation(_EntityB1.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityB2.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityC1.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityC3.getEntityUID(),"testRelationTypeType2",null,true);
        _EntityB1.attachFromRelation(_EntityC2.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityB2.attachFromRelation(_EntityB1.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityB1.getEntityUID(),"testRelationTypeType1",null,false);

        List<Entity> resultListEntityList = _EntityA.getRelatedConceptionEntities(null,"testRelationTypeType1",Direction.TWO_WAY,2);

        Assert.assertEquals(resultListEntityList.size(),4);
        for(Entity currentEntity:resultListEntityList){
            Assert.assertNotNull(currentEntity.getEntityUID());
            Assert.assertNotNull(currentEntity.getConceptionKindName());
            if(currentEntity.getConceptionKindName().equals("TestConceptionKindForRelationableTestB")){
                Assert.assertTrue(
                currentEntity.getEntityUID().equals(_EntityB1.getEntityUID()) |
                        currentEntity.getEntityUID().equals(_EntityB2.getEntityUID())
                );
            }
            if(currentEntity.getConceptionKindName().equals("TestConceptionKindForRelationableTestC")){
                Assert.assertTrue(
                        currentEntity.getEntityUID().equals(_EntityC1.getEntityUID()) |
                                currentEntity.getEntityUID().equals(_EntityC2.getEntityUID())
                );
            }
        }

        Long countRelatedNodesNumber = _EntityA.countRelatedConceptionEntities(null,"testRelationTypeType1",Direction.TWO_WAY,2);
        Assert.assertEquals(countRelatedNodesNumber,new Long("4"));

        List<String> attributesList = new ArrayList<>();
        attributesList.add("kindName");
        attributesList.add("dataOrigin");
        attributesList.add("createDate");
        EntitiesAttributesRetrieveResult resultEntitiesAttributesRetrieveResult = _EntityA.getAttributesOfRelatedConceptionEntities(null,attributesList,"testRelationTypeType1",Direction.TWO_WAY,2);
        List<EntityValue> entityValueList = resultEntitiesAttributesRetrieveResult.getEntityValues();
        for(EntityValue currentEntityValue : entityValueList){
            Assert.assertNotNull(currentEntityValue.getEntityUID());
            Assert.assertEquals(currentEntityValue.getEntityAttributesValue().size(),3);
        }
        Assert.assertEquals(resultEntitiesAttributesRetrieveResult.getEntityValues().size(),4);

        AttributesParameters conceptionAttributesParameters =new AttributesParameters();
        conceptionAttributesParameters.setDefaultFilteringItem(new EqualFilteringItem("kindName","ConceptionKind0C"));
        Long countRelatedConceptionEntitiesRes = _EntityA.countRelatedConceptionEntities(null,"testRelationTypeType1",Direction.TWO_WAY,2,null,conceptionAttributesParameters,true);
        Assert.assertEquals(countRelatedConceptionEntitiesRes,new Long("2"));

        AttributesParameters relationAttributesParameters =new AttributesParameters();
        relationAttributesParameters.setDefaultFilteringItem(new EqualFilteringItem("dataOrigin","dataOrigin001"+"bad"));
        countRelatedConceptionEntitiesRes = _EntityA.countRelatedConceptionEntities(null,"testRelationTypeType1",Direction.TWO_WAY,2,relationAttributesParameters,conceptionAttributesParameters,true);
        Assert.assertEquals(countRelatedConceptionEntitiesRes,new Long("0"));
        relationAttributesParameters.setDefaultFilteringItem(new EqualFilteringItem("dataOrigin","dataOrigin001"));
        countRelatedConceptionEntitiesRes = _EntityA.countRelatedConceptionEntities(null,"testRelationTypeType1",Direction.TWO_WAY,2,relationAttributesParameters,conceptionAttributesParameters,true);
        Assert.assertEquals(countRelatedConceptionEntitiesRes,new Long("2"));

        ResultEntitiesParameters resultEntitiesParameters= new ResultEntitiesParameters();
        resultEntitiesParameters.setDistinctMode(true);
        resultEntitiesParameters.addSortingAttribute("kindName", QueryParameters.SortingLogic.DESC);
        resultEntitiesParameters.setResultNumber(10000);
        List<Entity> resultListEntityList2 = _EntityA.getRelatedConceptionEntities(null,"testRelationTypeType1",Direction.TWO_WAY,2,null,conceptionAttributesParameters,resultEntitiesParameters);

        Assert.assertEquals(resultListEntityList2.size(),2);
        for(Entity currentEntity:resultListEntityList){
            Assert.assertNotNull(currentEntity.getEntityUID());
            Assert.assertNotNull(currentEntity.getConceptionKindName());
            if(currentEntity.getConceptionKindName().equals("TestConceptionKindForRelationableTestC")) {
                Assert.assertTrue(
                        currentEntity.getEntityUID().equals(_EntityC1.getEntityUID()) |
                                currentEntity.getEntityUID().equals(_EntityC2.getEntityUID())
                );
            }
        }

        resultListEntityList2 = _EntityA.getRelatedConceptionEntities(null,"testRelationTypeType1",Direction.TWO_WAY,2,relationAttributesParameters,conceptionAttributesParameters,resultEntitiesParameters);
        Assert.assertEquals(resultListEntityList2.size(),2);

        List<String> attributesList2 = new ArrayList<>();
        attributesList2.add("dataOrigin");
        attributesList2.add("createDate");
        EntitiesAttributesRetrieveResult resultEntitiesAttributesRetrieveResult2 = _EntityA.getAttributesOfRelatedConceptionEntities(null,attributesList2,"testRelationTypeType1",Direction.TWO_WAY,2,relationAttributesParameters,conceptionAttributesParameters,resultEntitiesParameters);
        List<EntityValue> entityValueList2 = resultEntitiesAttributesRetrieveResult2.getEntityValues();
        for(EntityValue currentEntityValue : entityValueList2){
            Assert.assertNotNull(currentEntityValue.getEntityUID());
            Assert.assertEquals(currentEntityValue.getEntityAttributesValue().size(),2);
        }
        Assert.assertEquals(resultEntitiesAttributesRetrieveResult2.getEntityValues().size(),2);

        relationAttributesParameters.setDefaultFilteringItem(new EqualFilteringItem("dataOrigin","dataOrigin001"+"bad"));
        resultListEntityList2 = _EntityA.getRelatedConceptionEntities(null,"testRelationTypeType1",Direction.TWO_WAY,2,relationAttributesParameters,conceptionAttributesParameters,resultEntitiesParameters);
        Assert.assertEquals(resultListEntityList2.size(),0);

        resultEntitiesParameters.setStartPage(2);
        resultEntitiesParameters.setEndPage(4);
        resultEntitiesParameters.setPageSize(10);
        resultListEntityList2 = _EntityA.getRelatedConceptionEntities(null,"testRelationTypeType1",Direction.TWO_WAY,2,null,conceptionAttributesParameters,resultEntitiesParameters);
        Assert.assertEquals(resultListEntityList2.size(),0);

        RelationshipEntity resultRelationshipEntity = _EntityA.attachFromRelation(_EntityB2.getEntityUID(),"detachRelTestRelation",null,false);
        RelationshipEntity resultRelationshipEntity2 = _EntityB1.attachFromRelation(_EntityB2.getEntityUID(),"detachRelTestRelation",null,false);
        boolean detachResult = _EntityA.detachRelation(resultRelationshipEntity.getRelationshipEntityUID());
        Assert.assertTrue(detachResult);

        detachResult = _EntityA.detachRelation(resultRelationshipEntity2.getRelationshipEntityUID());
        Assert.assertFalse(detachResult);
        boolean exceptionShouldBeCaught = false;
        try{
            _EntityA.detachRelation(resultRelationshipEntity.getRelationshipEntityUID());
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        List<String> detachAllRelationsResult = _EntityA.detachAllRelations();
        Assert.assertNotNull(detachAllRelationsResult);
        Assert.assertEquals(detachAllRelationsResult.size(),4);
        Assert.assertEquals(_EntityA.getAllRelations().size(),0);

        _EntityA.attachFromRelation(_EntityB1.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityB2.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityC1.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityC3.getEntityUID(),"testRelationTypeType2",null,true);

        detachAllRelationsResult = _EntityA.detachAllSpecifiedRelations("testRelationTypeType1",Direction.TO);
        Assert.assertNotNull(detachAllRelationsResult);
        Assert.assertEquals(detachAllRelationsResult.size(),0);

        detachAllRelationsResult = _EntityA.detachAllSpecifiedRelations("testRelationTypeType1",Direction.FROM);
        Assert.assertNotNull(detachAllRelationsResult);
        Assert.assertEquals(detachAllRelationsResult.size(),3);

        _EntityA.attachFromRelation(_EntityB1.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityB2.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityC1.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityC3.getEntityUID(),"testRelationTypeType2",null,true);

        detachAllRelationsResult = _EntityA.detachAllSpecifiedRelations("testRelationTypeType1",Direction.TWO_WAY);
        Assert.assertNotNull(detachAllRelationsResult);
        Assert.assertEquals(detachAllRelationsResult.size(),3);

        detachAllRelationsResult = _EntityC3.detachAllSpecifiedRelations("testRelationTypeType2",Direction.TO);
        Assert.assertNotNull(detachAllRelationsResult);
        Assert.assertEquals(detachAllRelationsResult.size(),2);

        _EntityA.attachFromRelation(_EntityB1.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityB2.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityC1.getEntityUID(),"testRelationTypeType1",null,true);
        _EntityA.attachFromRelation(_EntityC3.getEntityUID(),"testRelationTypeType2",null,true);

        QueryParameters exploreParameters = new QueryParameters();
        exploreParameters.setEntityKind("testRelationTypeType1");
        exploreParameters.setResultNumber(2);
        detachAllRelationsResult = _EntityA.detachSpecifiedRelations(exploreParameters,Direction.TWO_WAY);
        Assert.assertNotNull(detachAllRelationsResult);
        Assert.assertEquals(detachAllRelationsResult.size(),2);

        _EntityA.detachAllRelations();
        List<String> conceptionEntityUIDList = new ArrayList<>();
        conceptionEntityUIDList.add(_EntityB1.getEntityUID());
        conceptionEntityUIDList.add(_EntityB2.getEntityUID());
        conceptionEntityUIDList.add(_EntityC1.getEntityUID());
        conceptionEntityUIDList.add(_EntityC3.getEntityUID());

        List<RelationshipEntity> resultList = _EntityA.attachFromRelation(conceptionEntityUIDList,"testAttachMultiFromRelation",null,true);
        Assert.assertEquals(resultList.size(),4);
        List<RelationshipEntity> checkResult = _EntityA.getAllSpecifiedRelations("testAttachMultiFromRelation",Direction.FROM);
        Assert.assertEquals(checkResult.size(),4);

        Assert.assertNotNull(checkResult.get(0).getFromEntityKinds());
        Assert.assertEquals(checkResult.get(0).getFromEntityKinds().get(0),testConceptionKindName+"A");
        Assert.assertNotNull(checkResult.get(0).getToEntityKinds());
        Assert.assertNotEquals(checkResult.get(0).getToEntityKinds().get(0),testConceptionKindName+"A");

        resultList = _EntityA.attachToRelation(conceptionEntityUIDList,"testAttachMultiToRelation",null,true);
        Assert.assertEquals(resultList.size(),4);
        checkResult = _EntityA.getAllSpecifiedRelations("testAttachMultiToRelation",Direction.TO);
        Assert.assertEquals(checkResult.size(),4);

        Map<String,Long> relationKindCountMap = _EntityA.countAttachedRelationKinds();
        Assert.assertNotNull(relationKindCountMap);
        Assert.assertEquals(relationKindCountMap.get("testAttachMultiFromRelation"),Long.valueOf(4));
        Assert.assertEquals(relationKindCountMap.get("testAttachMultiToRelation"),Long.valueOf(4));

        List<String> attachedConceptionKinds = _EntityA.listAttachedConceptionKinds();
        Assert.assertNotNull(attachedConceptionKinds);
        Assert.assertEquals(attachedConceptionKinds.size(),2);
        Assert.assertTrue(attachedConceptionKinds.contains("TestConceptionKindForRelationableTestB"));
        Assert.assertTrue(attachedConceptionKinds.contains("TestConceptionKindForRelationableTestC"));
        Map<Set<String>,Long> attachedConceptionKindsCount = _EntityA.countAttachedConceptionKinds();
        Assert.assertNotNull(attachedConceptionKindsCount);
        Assert.assertEquals(attachedConceptionKindsCount.size(),2);
    }
}
