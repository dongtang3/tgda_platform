package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.GreaterThanFilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.feature.StatisticalAndEvaluable;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.GroupNumericalAttributesStatisticResult;
import com.github.tgda.engine.core.payload.RelationshipAttachInfo;
import com.github.tgda.engine.core.payload.NumericalAttributeStatisticCondition;
import com.github.tgda.coreRealm.realmServiceCore.term.*;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StatisticalAndEvaluableTest {

    private static String testRealmName = "UNIT_TEST_Realm";
    private static String testConceptionKindName = "TestConceptionKind01";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for StatisticalAndEvaluableTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testStatisticalAndEvaluableFunction() throws EngineServiceEntityExploreException {
        CoreRealm coreRealm = EngineFactory.getDefaultEngine();
        coreRealm.openGlobalSession();
        Assert.assertEquals(coreRealm.getStorageImplTech(), StorageImplTech.NEO4J);

        try {
            coreRealm.removeConceptionKind(testConceptionKindName+"ForSTAAndEva",true);
        } catch (EngineServiceRuntimeException e) {
            e.printStackTrace();
        }

        ConceptionKind _ConceptionKind01 = coreRealm.getConceptionKind(testConceptionKindName+"ForSTAAndEva");

        Classification classification0 = coreRealm.getClassification("Classification0");
        if(classification0 == null){
            coreRealm.createClassification("Classification0","Classification0Desc");
        }
        Classification classification1 = coreRealm.getClassification("Classification1");
        if(classification1 == null){
            coreRealm.createClassification("Classification1","Classification1Desc");
        }
        Classification classification2 = coreRealm.getClassification("Classification2");
        if(classification2 == null){
            coreRealm.createClassification("Classification2","Classification2Desc");
        }
        RelationshipAttachInfo relationshipAttachInfo = new RelationshipAttachInfo();
        relationshipAttachInfo.setRelationKind("RelationKind0001");
        relationshipAttachInfo.setDirection(Direction.FROM);

        if(_ConceptionKind01 == null){
            _ConceptionKind01 = coreRealm.createConceptionKind(testConceptionKindName+"ForSTAAndEva","testKind01Desc+中文描述");
            Assert.assertNotNull(_ConceptionKind01);
            Assert.assertEquals(_ConceptionKind01.getConceptionKindName(),testConceptionKindName+"ForSTAAndEva");
            Assert.assertEquals(_ConceptionKind01.getConceptionKindDesc(),"testKind01Desc+中文描述");

            for(int i =0;i<100;i++){
                Map<String,Object> newEntityValue= new HashMap<>();
                newEntityValue.put("prop01",(i+1)*100);
                newEntityValue.put("prop02",(i+1)*100+50);

                if(i<20){
                    newEntityValue.put("groupPropA","Group1");
                    newEntityValue.put("groupPropB","Group1-B");
                }else if(i<40){
                    newEntityValue.put("groupPropA","Group2");
                    newEntityValue.put("groupPropB","Group2-B");
                }else if(i<60){
                    newEntityValue.put("groupPropA","Group3");
                    newEntityValue.put("groupPropB","Group3-B");
                }else if(i<80){
                    newEntityValue.put("groupPropA","Group4");
                    newEntityValue.put("groupPropB","Group4-B");
                }else{
                    newEntityValue.put("groupPropA","Group5");
                    newEntityValue.put("groupPropB","Group5-B");
                }

                EntityValue entityValue = new EntityValue(newEntityValue);
                Entity currentEntity = _ConceptionKind01.newEntity(entityValue,false);
                try {
                    if(i<20){
                        currentEntity.attachClassification(relationshipAttachInfo,"Classification1");
                    }else if(i<40){
                        currentEntity.attachClassification(relationshipAttachInfo,"Classification0");
                    }else if(i<60){
                        currentEntity.attachClassification(relationshipAttachInfo,"Classification2");
                    }else if(i<80){
                        currentEntity.attachClassification(relationshipAttachInfo,"Classification1");
                    }else{
                        currentEntity.attachClassification(relationshipAttachInfo,"Classification0");
                    }
                } catch (EngineServiceRuntimeException e) {
                    e.printStackTrace();
                }
            }
        }

        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setDefaultFilteringItem(new GreaterThanFilteringItem("prop01",1000));

        List<NumericalAttributeStatisticCondition> statisticConditionList = new ArrayList<>();

        statisticConditionList.add(
          new NumericalAttributeStatisticCondition("prop01",StatisticalAndEvaluable.StatisticFunction.MIN)
        );
        statisticConditionList.add(
                new NumericalAttributeStatisticCondition("prop01",StatisticalAndEvaluable.StatisticFunction.AVG)
        );
        statisticConditionList.add(
                new NumericalAttributeStatisticCondition("prop01",StatisticalAndEvaluable.StatisticFunction.COUNT)
        );
        statisticConditionList.add(
                new NumericalAttributeStatisticCondition("prop02",StatisticalAndEvaluable.StatisticFunction.SUM)
        );
        statisticConditionList.add(
                new NumericalAttributeStatisticCondition("prop02",StatisticalAndEvaluable.StatisticFunction.STDEV)
        );
        statisticConditionList.add(
                new NumericalAttributeStatisticCondition("prop02",StatisticalAndEvaluable.StatisticFunction.MAX)
        );

        Map<String,Number> statisticResult = _ConceptionKind01.statisticNumericalAttributes(queryParameters,statisticConditionList);

        Assert.assertNotNull(statisticResult);
        Assert.assertNotNull(statisticResult.get("stDev(prop02)"));
        Assert.assertNotNull(statisticResult.get("avg(prop01)"));
        Assert.assertNotNull(statisticResult.get("count(prop01)"));
        Assert.assertNotNull(statisticResult.get("sum(prop02)"));
        Assert.assertNotNull(statisticResult.get("min(prop01)"));
        Assert.assertNotNull(statisticResult.get("max(prop02)"));

        Assert.assertEquals(statisticResult.get("count(prop01)"),Long.valueOf(90));
        Assert.assertEquals(statisticResult.get("sum(prop02)"),Long.valueOf(504000));
        Assert.assertEquals(statisticResult.get("min(prop01)"),Long.valueOf(1100));
        Assert.assertEquals(statisticResult.get("max(prop02)"),Long.valueOf(10050));

        List<GroupNumericalAttributesStatisticResult> groupStatisticResult = _ConceptionKind01.statisticNumericalAttributesByGroup("groupPropA",queryParameters,statisticConditionList);
        Assert.assertEquals(groupStatisticResult.size(),5);

        List<String> groupList = new ArrayList<>();
        groupList.add("Group1");groupList.add("Group2");groupList.add("Group3");groupList.add("Group4");groupList.add("Group5");
        for(GroupNumericalAttributesStatisticResult currentGroupNumericalAttributesStatisticResult:groupStatisticResult){
            String groupValue = currentGroupNumericalAttributesStatisticResult.getGroupAttributeValue().toString();
            Assert.assertTrue(groupList.contains(groupValue));
            Map<String, Number> groupResult = currentGroupNumericalAttributesStatisticResult.getNumericalAttributesStatisticValue();
            Assert.assertEquals(groupResult.size(),6);
            Assert.assertNotNull(groupResult.get("stDev(prop02)"));
            Assert.assertNotNull(groupResult.get("avg(prop01)"));
            Assert.assertNotNull(groupResult.get("count(prop01)"));
            Assert.assertNotNull(groupResult.get("sum(prop02)"));
            Assert.assertNotNull(groupResult.get("min(prop01)"));
            Assert.assertNotNull(groupResult.get("max(prop02)"));
        }

        coreRealm.closeGlobalSession();

        CoreRealm coreRealm2 = EngineFactory.getDefaultEngine();
        ConceptionKind _ConceptionKind2 = coreRealm2.getConceptionKind(testConceptionKindName+"ForSTAAndEva");
        Map<String,List<Entity>> staticClassificationResult = _ConceptionKind2.statisticRelatedClassifications(null,"RelationKind0001", Direction.TO);

        Assert.assertEquals(staticClassificationResult.keySet().size(),3);
        Assert.assertTrue(staticClassificationResult.containsKey("Classification0"));
        Assert.assertTrue(staticClassificationResult.containsKey("Classification1"));
        Assert.assertTrue(staticClassificationResult.containsKey("Classification2"));

        int entityNumber = staticClassificationResult.get("Classification0").size()+
                staticClassificationResult.get("Classification1").size()+
                staticClassificationResult.get("Classification2").size();
        Assert.assertEquals(entityNumber,100);

        Entity _Entity01 = staticClassificationResult.get("Classification0").get(0);
        Assert.assertEquals(_Entity01.getConceptionKindName(),testConceptionKindName+"ForSTAAndEva");
        List<Classification> resultClassificationList01 = _Entity01.getAttachedClassifications("RelationKind0001", Direction.FROM);
        Assert.assertEquals(resultClassificationList01.get(0).getClassificationName(),"Classification0");

        Entity _Entity02 = staticClassificationResult.get("Classification1").get(0);
        Assert.assertEquals(_Entity02.getConceptionKindName(),testConceptionKindName+"ForSTAAndEva");
        List<Classification> resultClassificationList02 = _Entity02.getAttachedClassifications("RelationKind0001", Direction.FROM);
        Assert.assertEquals(resultClassificationList02.get(0).getClassificationName(),"Classification1");

        Entity _Entity03 = staticClassificationResult.get("Classification2").get(0);
        Assert.assertEquals(_Entity03.getConceptionKindName(),testConceptionKindName+"ForSTAAndEva");
        List<Classification> resultClassificationList03 = _Entity03.getAttachedClassifications("RelationKind0001", Direction.FROM);
        Assert.assertEquals(resultClassificationList03.get(0).getClassificationName(),"Classification2");

        staticClassificationResult = _ConceptionKind2.statisticRelatedClassifications(null,"RelationKind0001", Direction.FROM);
        Assert.assertEquals(staticClassificationResult.keySet().size(),0);

        staticClassificationResult = _ConceptionKind2.statisticRelatedClassifications(null,"RelationKind0001", Direction.TWO_WAY);
        Assert.assertEquals(staticClassificationResult.keySet().size(),3);

        RelationKind _RelationKind0001 = coreRealm2.getRelationKind("RelationKind0001");

        if(_RelationKind0001 != null){
            try {
                coreRealm.removeRelationKind("RelationKind0001",true);
            } catch (EngineServiceRuntimeException e) {
                e.printStackTrace();
            }
        }
        _RelationKind0001 = coreRealm2.createRelationKind("RelationKind0001","DESC");

        List<GroupNumericalAttributesStatisticResult> groupNumericalAttributesStatisticResult2 = _RelationKind0001.statisticNumericalAttributesByGroup("groupPropA",queryParameters,statisticConditionList);
        Assert.assertEquals(groupNumericalAttributesStatisticResult2.size(),0);


        Map<String,Number> numberMap2 = _RelationKind0001.statisticNumericalAttributes(queryParameters,statisticConditionList);
        Assert.assertEquals(numberMap2.size(),6);

        boolean exceptionShouldThrown = false;
        try {
            _RelationKind0001.statisticRelatedClassifications(null, "RelationKind0001", Direction.TO);
        }catch (EngineServiceEntityExploreException e){
            exceptionShouldThrown = true;
        }
        Assert.assertTrue(exceptionShouldThrown);
    }
}
