package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.exception.EngineFunctionNotSupportedException;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.payload.TypeCorrelationInfo;
import com.github.tgda.engine.core.payload.EntityStatisticsInfo;
import com.github.tgda.engine.core.payload.TypeMetaInfo;

import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JTypeImpl;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JTimeFlowImpl;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.Constant;
import com.github.tgda.engine.core.util.config.PropertiesHandler;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;

public class CoreRealmTest {

    private static String testRealmName = "UNIT_TEST_Realm";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for CoreRealmTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testCoreRealmFunction() throws EngineServiceRuntimeException, EngineFunctionNotSupportedException, EngineServiceEntityExploreException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        Assert.assertEquals(coreRealm.getStorageImplTech(), StorageImplTech.NEO4J);
        Assert.assertEquals(coreRealm.getEngineName(), PropertiesHandler.getPropertyValue(PropertiesHandler.DEFAULT_REALM_NAME));

        Type _Type01 = coreRealm.getType("kind01");
        if(_Type01 != null){
            boolean removeResult = coreRealm.removeType("kind01",true);
            Assert.assertTrue(removeResult);
            _Type01 = coreRealm.getType("kind01");
        }
        Assert.assertNull(_Type01);
        _Type01 = coreRealm.createType("kind01","kind01Desc+中文描述");
        Assert.assertNotNull(_Type01);
        Assert.assertEquals(_Type01.getTypeName(),"kind01");
        Assert.assertEquals(_Type01.getTypeDesc(),"kind01Desc+中文描述");
        Assert.assertNotNull(((Neo4JTypeImpl)_Type01).getTypeUID());
        Assert.assertNull(((Neo4JTypeImpl)_Type01).getCoreRealmName());

        _Type01 = coreRealm.createType("kind01","kind01Desc+中文描述");
        Assert.assertNull(_Type01);

        Type _Type02 = coreRealm.getType("kind02");
        if(_Type02 != null){
            coreRealm.removeType("kind02",true);
        }
        _Type02 = coreRealm.createType("kind02","kind02Desc+中文描述");
        Assert.assertNotNull(_Type02);

        _Type01 = coreRealm.getType("kind01");
        Assert.assertNotNull(_Type01);
        Assert.assertEquals(_Type01.getTypeName(),"kind01");
        Assert.assertEquals(_Type01.getTypeDesc(),"kind01Desc+中文描述");
        Assert.assertNotNull(((Neo4JTypeImpl)_Type01).getTypeUID());
        Assert.assertNull(((Neo4JTypeImpl)_Type01).getCoreRealmName());

        AttributesView attributesViewKind01 = coreRealm.createAttributesView("attributesViewKind01","attributesViewKind01Desc",null);
        Assert.assertNotNull(attributesViewKind01);
        Assert.assertNotNull(attributesViewKind01.getAttributesViewUID());
        Assert.assertEquals(attributesViewKind01.getAttributesViewName(),"attributesViewKind01");
        Assert.assertEquals(attributesViewKind01.getAttributesViewDesc(),"attributesViewKind01Desc");
        Assert.assertEquals(attributesViewKind01.getAttributesViewDataForm(),AttributesView.AttributesViewDataForm.SINGLE_VALUE);
        Assert.assertFalse(attributesViewKind01.isCollectionAttributesView());

        String targetAttributesViewUID = attributesViewKind01.getAttributesViewUID();

        attributesViewKind01 = coreRealm.createAttributesView(null,"attributesViewKind01Desc",null);
        Assert.assertNull(attributesViewKind01);

        attributesViewKind01 = coreRealm.createAttributesView("attributesViewKind02",null,AttributesView.AttributesViewDataForm.LIST_VALUE);
        Assert.assertNotNull(attributesViewKind01);
        Assert.assertNotNull(attributesViewKind01.getAttributesViewUID());
        Assert.assertEquals(attributesViewKind01.getAttributesViewName(),"attributesViewKind02");
        Assert.assertEquals(attributesViewKind01.getAttributesViewDataForm(),AttributesView.AttributesViewDataForm.LIST_VALUE);
        Assert.assertTrue(attributesViewKind01.isCollectionAttributesView());

        AttributesView attributesViewKind02 = coreRealm.getAttributesView(targetAttributesViewUID);
        Assert.assertNotNull(attributesViewKind02);
        Assert.assertNotNull(attributesViewKind02.getAttributesViewUID());
        Assert.assertEquals(attributesViewKind02.getAttributesViewName(),"attributesViewKind01");
        Assert.assertEquals(attributesViewKind02.getAttributesViewDesc(),"attributesViewKind01Desc");
        Assert.assertEquals(attributesViewKind02.getAttributesViewDataForm(),AttributesView.AttributesViewDataForm.SINGLE_VALUE);
        Assert.assertFalse(attributesViewKind02.isCollectionAttributesView());

        attributesViewKind02 = coreRealm.getAttributesView("123456");
        Assert.assertNull(attributesViewKind02);

        boolean removeAttributesViewRes = coreRealm.removeAttributesView(targetAttributesViewUID);
        Assert.assertTrue(removeAttributesViewRes);

        attributesViewKind02 = coreRealm.getAttributesView(targetAttributesViewUID);
        Assert.assertNull(attributesViewKind02);

        removeAttributesViewRes = coreRealm.removeAttributesView(null);
        Assert.assertFalse(removeAttributesViewRes);

        boolean exceptionShouldBeCaught = false;
        try{
            coreRealm.removeAttributesView("123456");
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        AttributeKind attributeKind01 = coreRealm.createAttributeKind("attributeKind01","attributeKind01Desc", AttributeDataType.BOOLEAN);
        Assert.assertNotNull(attributeKind01);
        Assert.assertNotNull(attributeKind01.getAttributeKindUID());
        Assert.assertEquals(attributeKind01.getAttributeKindName(),"attributeKind01");
        Assert.assertEquals(attributeKind01.getAttributeKindDesc(),"attributeKind01Desc");
        Assert.assertEquals(attributeKind01.getAttributeDataType(),AttributeDataType.BOOLEAN);

        String targetAttributeKindUID = attributeKind01.getAttributeKindUID();
        AttributeKind attributeKind02 = coreRealm.getAttributeKind(targetAttributeKindUID);
        Assert.assertNotNull(attributeKind02);
        Assert.assertNotNull(attributeKind02.getAttributeKindUID());
        Assert.assertEquals(attributeKind02.getAttributeKindName(),"attributeKind01");
        Assert.assertEquals(attributeKind02.getAttributeKindDesc(),"attributeKind01Desc");
        Assert.assertEquals(attributeKind02.getAttributeDataType(),AttributeDataType.BOOLEAN);

        attributeKind02 = coreRealm.getAttributeKind(null);
        Assert.assertNull(attributeKind02);
        attributeKind02 = coreRealm.getAttributeKind("123456");
        Assert.assertNull(attributeKind02);

        boolean removeAttributeKindRes01 = coreRealm.removeAttributeKind(null);
        Assert.assertFalse(removeAttributeKindRes01);
        removeAttributeKindRes01 = coreRealm.removeAttributeKind(targetAttributeKindUID);
        Assert.assertTrue(removeAttributeKindRes01);
        attributeKind02 = coreRealm.getAttributeKind(targetAttributeKindUID);
        Assert.assertNull(attributeKind02);

        RelationKind relationKind01 = coreRealm.createRelationKind("relationKind01","relationKind01Desc");
        Assert.assertNotNull(relationKind01);
        Assert.assertEquals(relationKind01.getRelationKindName(),"relationKind01");
        Assert.assertEquals(relationKind01.getRelationKindDesc(),"relationKind01Desc");
        relationKind01 = coreRealm.createRelationKind("relationKind01","relationKind01Desc");
        Assert.assertNull(relationKind01);

        RelationKind targetRelationKind01 = coreRealm.getRelationKind("relationKind01");
        Assert.assertNotNull(targetRelationKind01);
        Assert.assertEquals(targetRelationKind01.getRelationKindName(),"relationKind01");
        Assert.assertEquals(targetRelationKind01.getRelationKindDesc(),"relationKind01Desc");

        targetRelationKind01 = coreRealm.getRelationKind("relationKind01+NotExist");
        Assert.assertNull(targetRelationKind01);

        boolean removeRelationTypeRes = coreRealm.removeRelationKind("relationKind01",true);
        Assert.assertTrue(removeRelationTypeRes);

        exceptionShouldBeCaught = false;
        try{
            coreRealm.removeRelationKind("relationKind01",true);
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        exceptionShouldBeCaught = false;
        try{
            coreRealm.createRelationKind("relationKind02","relationKind02Desc","parentRelationType");
        }catch(EngineFunctionNotSupportedException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        String classificationName01 = "classification001";
        Classification _Classification01 = coreRealm.getClassification(classificationName01);

        Assert.assertFalse(coreRealm.removeClassification(null));
        if(_Classification01 != null){
            boolean removeClassificationResult = coreRealm.removeClassification(classificationName01);
            Assert.assertTrue(removeClassificationResult);
            exceptionShouldBeCaught = false;
            try {
                coreRealm.removeClassification(classificationName01);
            }catch (EngineServiceRuntimeException e){
                exceptionShouldBeCaught = true;
            }
            Assert.assertTrue(exceptionShouldBeCaught);
        }

        _Classification01 = coreRealm.getClassification(classificationName01);
        Assert.assertNull(_Classification01);
        _Classification01 = coreRealm.createClassification(classificationName01,classificationName01+"Desc");
        Assert.assertNotNull(_Classification01);
        _Classification01 = coreRealm.getClassification(classificationName01);
        Assert.assertNotNull(_Classification01);

        String classificationName02 = "classification002";
        Classification _Classification02 = coreRealm.getClassification(classificationName02);
        if(_Classification02 != null){
            coreRealm.removeClassification(classificationName02);
        }

        _Classification02 = coreRealm.createClassification(classificationName02,classificationName02+"Desc",classificationName01);
        Assert.assertNotNull(_Classification02);

        _Classification02.addAttribute("attribute01","this is a string value");
        Assert.assertEquals(_Classification02.getAttribute("attribute01").getAttributeValue(),"this is a string value");

        String classificationName03 = "classification003";
        Classification _Classification03 = coreRealm.getClassification(classificationName03);
        if(_Classification03 != null){
            coreRealm.removeClassification(classificationName03);
        }

        String classificationName03_1 = "classification003_1";
        Classification _Classification03_1 = coreRealm.getClassification(classificationName03_1);
        if(_Classification03_1 != null){
            coreRealm.removeClassification(classificationName03_1);
        }

        String classificationName03_1_1 = "classification003_1_1";
        Classification _Classification03_1_1 = coreRealm.getClassification(classificationName03_1_1);
        if(_Classification03_1_1 != null){
            coreRealm.removeClassification(classificationName03_1_1);
        }

        coreRealm.createClassification(classificationName03,classificationName03+"Desc");
        coreRealm.createClassification(classificationName03_1,classificationName03_1+"Desc",classificationName03);
        coreRealm.createClassification(classificationName03_1_1,classificationName03_1_1+"Desc",classificationName03_1);

        Classification targetClassification = coreRealm.getClassification(classificationName03);
        Assert.assertNotNull(targetClassification);
        targetClassification = coreRealm.getClassification(classificationName03_1);
        Assert.assertNotNull(targetClassification);
        targetClassification = coreRealm.getClassification(classificationName03_1_1);
        Assert.assertNotNull(targetClassification);

        coreRealm.removeClassificationWithOffspring(classificationName03);

        targetClassification = coreRealm.getClassification(classificationName03);
        Assert.assertNull(targetClassification);
        targetClassification = coreRealm.getClassification(classificationName03_1);
        Assert.assertNull(targetClassification);
        targetClassification = coreRealm.getClassification(classificationName03_1_1);
        Assert.assertNull(targetClassification);

        List<AttributeKind> attributeKindList = coreRealm.getAttributeKinds(null,null,null);
        Assert.assertTrue(attributeKindList.size()>0);
        attributeKindList = coreRealm.getAttributeKinds("attributeKind01",null,null);
        Assert.assertTrue(attributeKindList.size()>0);
        attributeKindList = coreRealm.getAttributeKinds(null,"attributeKind01Desc",null);
        Assert.assertTrue(attributeKindList.size()>0);
        attributeKindList = coreRealm.getAttributeKinds("attributeKind01","attributeKind01Desc",null);
        Assert.assertTrue(attributeKindList.size()>0);
        attributeKindList = coreRealm.getAttributeKinds("attributeKind01","attributeKind01DescNOTEXIST",null);
        Assert.assertTrue(attributeKindList.size()==0);
        attributeKindList = coreRealm.getAttributeKinds("attributeKind01","attributeKind01Desc",AttributeDataType.BINARY);
        Assert.assertTrue(attributeKindList.size()==0);
        attributeKindList = coreRealm.getAttributeKinds("attributeKind01","attributeKind01Desc",AttributeDataType.BOOLEAN);
        Assert.assertTrue(attributeKindList.size()>0);

        Assert.assertEquals(attributeKindList.get(0).getAttributeKindName(),"attributeKind01");
        Assert.assertEquals(attributeKindList.get(0).getAttributeKindDesc(),"attributeKind01Desc");
        Assert.assertEquals(attributeKindList.get(0).getAttributeDataType(),AttributeDataType.BOOLEAN);

        coreRealm.createAttributesView("attributesViewKind03","attributesViewKind03Desc",AttributesView.AttributesViewDataForm.LIST_VALUE);

        List<AttributesView> attributesViewKindList = coreRealm.getAttributesViews(null,null,null);
        Assert.assertTrue(attributesViewKindList.size()>0);
        attributesViewKindList = coreRealm.getAttributesViews("attributesViewKind03",null,null);
        Assert.assertTrue(attributesViewKindList.size()>0);
        attributesViewKindList = coreRealm.getAttributesViews(null,"attributesViewKind03Desc",null);
        Assert.assertTrue(attributesViewKindList.size()>0);
        attributesViewKindList = coreRealm.getAttributesViews("attributesViewKind03","attributesViewKind03Desc",null);
        Assert.assertTrue(attributesViewKindList.size()>0);
        attributesViewKindList = coreRealm.getAttributesViews("attributesViewKind03","attributesViewKind03DescNOTEXIST",null);
        Assert.assertTrue(attributesViewKindList.size()==0);
        attributesViewKindList = coreRealm.getAttributesViews("attributesViewKind03","attributesViewKind03Desc",AttributesView.AttributesViewDataForm.SINGLE_VALUE);
        Assert.assertTrue(attributesViewKindList.size()==0);
        attributesViewKindList = coreRealm.getAttributesViews("attributesViewKind03","attributesViewKind03Desc",AttributesView.AttributesViewDataForm.LIST_VALUE);
        Assert.assertTrue(attributesViewKindList.size()>0);

        RelationAttachKind targetRelationAttachKind = coreRealm.createRelationAttachKind("RelationAttachKind_Name","RelationAttachKind_Desc",
                "RelationAttachKind_SourceKind","RelationAttachKind_TargetKind","RelationAttachKind_RelationKind",true);
        Assert.assertNotNull(targetRelationAttachKind);
        Assert.assertNotNull(targetRelationAttachKind.getRelationAttachKindUID());
        Assert.assertEquals(targetRelationAttachKind.getRelationAttachKindName(),"RelationAttachKind_Name");
        Assert.assertEquals(targetRelationAttachKind.getRelationAttachKindDesc(),"RelationAttachKind_Desc");
        Assert.assertEquals(targetRelationAttachKind.getSourceTypeName(),"RelationAttachKind_SourceKind");
        Assert.assertEquals(targetRelationAttachKind.getTargetTypeName(),"RelationAttachKind_TargetKind");
        Assert.assertEquals(targetRelationAttachKind.getRelationKindName(),"RelationAttachKind_RelationKind");
        Assert.assertEquals(targetRelationAttachKind.isRepeatableRelationKindAllow(),true);

        RelationAttachKind targetRelationAttachKind2 = coreRealm.getRelationAttachKind(targetRelationAttachKind.getRelationAttachKindUID());
        Assert.assertNotNull(targetRelationAttachKind2);
        Assert.assertNotNull(targetRelationAttachKind2.getRelationAttachKindUID());
        Assert.assertEquals(targetRelationAttachKind2.getRelationAttachKindName(),"RelationAttachKind_Name");
        Assert.assertEquals(targetRelationAttachKind2.getRelationAttachKindDesc(),"RelationAttachKind_Desc");
        Assert.assertEquals(targetRelationAttachKind2.getSourceTypeName(),"RelationAttachKind_SourceKind");
        Assert.assertEquals(targetRelationAttachKind2.getTargetTypeName(),"RelationAttachKind_TargetKind");
        Assert.assertEquals(targetRelationAttachKind2.getRelationKindName(),"RelationAttachKind_RelationKind");
        Assert.assertEquals(targetRelationAttachKind2.isRepeatableRelationKindAllow(),true);

        RelationAttachKind  targetRelationAttachKind3 = coreRealm.getRelationAttachKind(targetRelationAttachKind.getRelationAttachKindUID()+"1000");
        Assert.assertNull(targetRelationAttachKind3);

        targetRelationAttachKind = coreRealm.createRelationAttachKind("RelationAttachKind_Name","RelationAttachKind_Desc",
                "RelationAttachKind_SourceKind","RelationAttachKind_TargetKind","RelationAttachKind_RelationKind",true);
        Assert.assertNull(targetRelationAttachKind);

        targetRelationAttachKind = coreRealm.createRelationAttachKind("RelationAttachKind_Name","RelationAttachKind_Desc",
                null,"RelationAttachKind_TargetKind","RelationAttachKind_RelationKind",true);
        Assert.assertNull(targetRelationAttachKind);

        targetRelationAttachKind = coreRealm.createRelationAttachKind(null,"RelationAttachKind_Desc",
                null,"RelationAttachKind_TargetKind","RelationAttachKind_RelationKind",true);
        Assert.assertNull(targetRelationAttachKind);

        targetRelationAttachKind = coreRealm.createRelationAttachKind("RelationAttachKind_Name2","RelationAttachKind_Desc",
                "RelationAttachKind_SourceKind","RelationAttachKind_TargetKind","RelationAttachKind_RelationKind",false);
        Assert.assertNotNull(targetRelationAttachKind);
        Assert.assertNotNull(targetRelationAttachKind.getRelationAttachKindUID());
        Assert.assertEquals(targetRelationAttachKind.getRelationAttachKindName(),"RelationAttachKind_Name2");
        Assert.assertEquals(targetRelationAttachKind.getRelationAttachKindDesc(),"RelationAttachKind_Desc");
        Assert.assertEquals(targetRelationAttachKind.getSourceTypeName(),"RelationAttachKind_SourceKind");
        Assert.assertEquals(targetRelationAttachKind.getTargetTypeName(),"RelationAttachKind_TargetKind");
        Assert.assertEquals(targetRelationAttachKind.getRelationKindName(),"RelationAttachKind_RelationKind");
        Assert.assertEquals(targetRelationAttachKind.isRepeatableRelationKindAllow(),false);

        List<RelationAttachKind> relationAttachKindList = coreRealm.getRelationAttachKinds(null,null,null,null,null,Boolean.valueOf(true));
        Assert.assertNotNull(relationAttachKindList);
        Assert.assertEquals(relationAttachKindList.size(),1);
        relationAttachKindList = coreRealm.getRelationAttachKinds(null,null,null,null,null,Boolean.valueOf(false));
        Assert.assertNotNull(relationAttachKindList);
        Assert.assertEquals(relationAttachKindList.size(),1);

        relationAttachKindList = coreRealm.getRelationAttachKinds(null,null,null,null,null,null);
        Assert.assertNotNull(relationAttachKindList);
        Assert.assertEquals(relationAttachKindList.size(),2);

        relationAttachKindList = coreRealm.getRelationAttachKinds("NOTMatchedValue",null,null,null,null,null);
        Assert.assertNotNull(relationAttachKindList);
        Assert.assertEquals(relationAttachKindList.size(),0);

        boolean removeResult = coreRealm.removeRelationAttachKind(targetRelationAttachKind.getRelationAttachKindUID());
        Assert.assertTrue(removeResult);
        removeResult = coreRealm.removeRelationAttachKind(targetRelationAttachKind2.getRelationAttachKindUID());
        Assert.assertTrue(removeResult);

        exceptionShouldBeCaught = false;
        try{
            coreRealm.removeRelationAttachKind(targetRelationAttachKind2.getRelationAttachKindUID()+"12345");
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        TimeFlow defaultTimeFlow = coreRealm.getOrCreateTimeFlow();
        Assert.assertNotNull(defaultTimeFlow);
        Assert.assertNotNull(((Neo4JTimeFlowImpl)defaultTimeFlow).getTimeFlowUID());
        Assert.assertEquals(defaultTimeFlow.getTimeFlowName(), Constant._defaultTimeFlowName);

        TimeFlow defaultTimeFlow2 = coreRealm.getOrCreateTimeFlow("自定义时间流");
        Assert.assertNotNull(defaultTimeFlow2);
        Assert.assertNotNull(((Neo4JTimeFlowImpl)defaultTimeFlow2).getTimeFlowUID());
        Assert.assertEquals(defaultTimeFlow2.getTimeFlowName(), "自定义时间流");

        List<TimeFlow> timeFlowsList = coreRealm.getTimeFlows();
        Assert.assertTrue(timeFlowsList.size()>=2);

        boolean hasDefaultTimeFlow = false;
        boolean hasCustomTimeFlow = false;
        for(TimeFlow currentTimeFlow:timeFlowsList){
            if(currentTimeFlow.getTimeFlowName().equals(Constant._defaultTimeFlowName)){
                hasDefaultTimeFlow = true;
            }
            if(currentTimeFlow.getTimeFlowName().equals("自定义时间流")){
                hasCustomTimeFlow = true;
            }
        }
        Assert.assertTrue(hasDefaultTimeFlow);
        Assert.assertTrue(hasCustomTimeFlow);

        List<EntityStatisticsInfo> statisticsInfosList = coreRealm.getConceptionEntitiesStatistics();
        Assert.assertNotNull(statisticsInfosList);
        Assert.assertTrue(statisticsInfosList.size()>1);

        for(EntityStatisticsInfo currentEntityStatisticsInfo:statisticsInfosList){
            if(!currentEntityStatisticsInfo.isSystemKind()){
                Assert.assertNotNull(currentEntityStatisticsInfo.getEntityKindName());
                Assert.assertNotNull(currentEntityStatisticsInfo.getEntityKindType());
                Assert.assertNotNull(currentEntityStatisticsInfo.getEntityKindUID());
                Assert.assertNotNull(currentEntityStatisticsInfo.getEntityKindDesc());
            }
        }

        statisticsInfosList = coreRealm.getRelationEntitiesStatistics();
        Assert.assertNotNull(statisticsInfosList);
        Assert.assertTrue(statisticsInfosList.size()>0);
        for(EntityStatisticsInfo currentEntityStatisticsInfo:statisticsInfosList){
            if(!currentEntityStatisticsInfo.isSystemKind()){
                Assert.assertNotNull(currentEntityStatisticsInfo.getEntityKindName());
                Assert.assertNotNull(currentEntityStatisticsInfo.getEntityKindType());
                Assert.assertNotNull(currentEntityStatisticsInfo.getEntityKindUID());
                Assert.assertNotNull(currentEntityStatisticsInfo.getEntityKindDesc());
            }
        }

        List<TypeCorrelationInfo> correlationInfo = coreRealm.getTypesCorrelation();
        Assert.assertNotNull(correlationInfo);

        List<TypeMetaInfo> typeMetaInfoList = coreRealm.getTypesMetaInfo();
        Assert.assertNotNull(statisticsInfosList);
        Assert.assertTrue(statisticsInfosList.size()>0);
        typeMetaInfoList = coreRealm.getRelationKindsMetaInfo();
        Assert.assertNotNull(statisticsInfosList);
        Assert.assertTrue(statisticsInfosList.size()>0);
        typeMetaInfoList = coreRealm.getAttributeKindsMetaInfo();
        Assert.assertNotNull(statisticsInfosList);
        Assert.assertTrue(statisticsInfosList.size()>0);
        typeMetaInfoList = coreRealm.getAttributesViewsMetaInfo();
        Assert.assertNotNull(statisticsInfosList);
        Assert.assertTrue(statisticsInfosList.size()>0);
    }
}
