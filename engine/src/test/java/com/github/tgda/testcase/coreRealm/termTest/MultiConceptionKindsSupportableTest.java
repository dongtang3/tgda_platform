package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.EntitiesOperationResult;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MultiConceptionKindsSupportableTest {

    private static String testRealmName = "UNIT_TEST_Realm";
    private static String testConceptionKindName = "TestConceptionKindMultiKindsSupportableTest";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for MultiKindsSupportableTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testMultiKindsSupportable() throws EngineServiceRuntimeException {
        Engine coreRealm = EngineFactory.getDefaultEngine();

        Type _Type01 = coreRealm.getType(testConceptionKindName);
        if(_Type01 != null){
            coreRealm.removeType(testConceptionKindName,true);
        }
        _Type01 = coreRealm.getType(testConceptionKindName);
        if(_Type01 == null){
            _Type01 = coreRealm.createType(testConceptionKindName,"TestConceptionKindADesc+中文描述");
        }

        Map<String,Object> newEntityValue1= new HashMap<>();
        newEntityValue1.put("prop1","fromEntity");
        EntityValue entityValue1 = new EntityValue(newEntityValue1);
        Entity _Entity1 = _Type01.newEntity(entityValue1,false);

        Map<String,Object> newEntityValue2= new HashMap<>();
        newEntityValue2.put("prop1","toEntity");
        EntityValue entityValue2 = new EntityValue(newEntityValue2);
        Entity _Entity2 = _Type01.newEntity(entityValue2,false);
        Assert.assertEquals(_Entity1.getAllConceptionKindNames().size(),1);

        Type _Type02 = coreRealm.getType("newKind001");
        if(_Type02 != null){
            coreRealm.removeType("newKind001",true);
        }
        _Type02 = coreRealm.getType("newKind001");
        if(_Type02 == null){
            _Type02 = coreRealm.createType("newKind001","TestConceptionKindADesc+中文描述");
        }

        Assert.assertEquals(_Type02.countConceptionEntities(),new Long("0"));
        Assert.assertEquals(_Type01.countConceptionEntities(),new Long("2"));

        String[] newKindNamesArray = new String[]{"newKind001","newKind002"};
        boolean joinResult = _Entity1.joinConceptionKinds(newKindNamesArray);
        Assert.assertTrue(joinResult);
        Assert.assertEquals(_Type02.countConceptionEntities(),new Long("1"));
        Assert.assertEquals(_Type01.countConceptionEntities(),new Long("2"));

        _Entity1 = _Type01.getEntityByUID(_Entity1.getEntityUID());
        Assert.assertEquals(_Entity1.getAllConceptionKindNames().size(),3);
        Assert.assertTrue(_Entity1.getAllConceptionKindNames().contains("newKind001"));
        Assert.assertTrue(_Entity1.getAllConceptionKindNames().contains("newKind002"));
        Assert.assertTrue(_Entity1.getAllConceptionKindNames().contains(testConceptionKindName));

        boolean retreatResult = _Entity1.retreatFromConceptionKind("newKind001");
        Assert.assertTrue(retreatResult);
        _Entity1 = _Type01.getEntityByUID(_Entity1.getEntityUID());
        Assert.assertEquals(_Entity1.getAllConceptionKindNames().size(),2);
        Assert.assertFalse(_Entity1.getAllConceptionKindNames().contains("newKind001"));
        Assert.assertTrue(_Entity1.getAllConceptionKindNames().contains("newKind002"));
        Assert.assertTrue(_Entity1.getAllConceptionKindNames().contains(testConceptionKindName));
        Assert.assertEquals(_Type02.countConceptionEntities(),new Long("0"));
        Assert.assertEquals(_Type01.countConceptionEntities(),new Long("2"));

        boolean exceptionShouldBeCaught = false;
        try{
            _Entity2.retreatFromConceptionKind("newKind001");
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        exceptionShouldBeCaught = false;
        try{
            _Entity1.retreatFromConceptionKind(null);
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        exceptionShouldBeCaught = false;
        try{
            _Entity1.joinConceptionKinds(null);
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        exceptionShouldBeCaught = false;
        try{
            _Entity1.joinConceptionKinds(new String[]{});
        }catch(EngineServiceRuntimeException e){
            exceptionShouldBeCaught = true;
        }
        Assert.assertTrue(exceptionShouldBeCaught);

        _Type01.purgeAllEntities();
        _Type02.purgeAllEntities();
        Assert.assertEquals(_Type02.countConceptionEntities(),new Long("0"));
        Assert.assertEquals(_Type01.countConceptionEntities(),new Long("0"));

        String[] newMultiConceptionTypeNamesArray = new String[]{_Type01.getConceptionKindName(), _Type02.getConceptionKindName()};

        Entity _Entity3 = coreRealm.newMultiEntity(newMultiConceptionTypeNamesArray, entityValue1,false);
        Assert.assertNotNull(_Entity3);
        Assert.assertEquals(_Entity3.getConceptionKindName(), _Type01.getConceptionKindName());
        Assert.assertEquals(_Entity3.getAllConceptionKindNames().size(),2);
        Assert.assertTrue(_Entity3.getAllConceptionKindNames().contains(_Type01.getConceptionKindName()));
        Assert.assertTrue(_Entity3.getAllConceptionKindNames().contains(_Type02.getConceptionKindName()));

        Assert.assertEquals(_Type02.countConceptionEntities(),new Long("1"));
        Assert.assertEquals(_Type01.countConceptionEntities(),new Long("1"));

        _Type01.purgeAllEntities();
        _Type02.purgeAllEntities();
        Assert.assertEquals(_Type02.countConceptionEntities(),new Long("0"));
        Assert.assertEquals(_Type01.countConceptionEntities(),new Long("0"));

        List<EntityValue> valueList = new ArrayList<>();
        valueList.add(entityValue1);
        valueList.add(entityValue2);

        EntitiesOperationResult entitiesOperationResult = coreRealm.newMultiConceptionEntities(newMultiConceptionTypeNamesArray,valueList,false);
        Assert.assertEquals(entitiesOperationResult.getSuccessEntityUIDs().size(),2);
        Assert.assertEquals(entitiesOperationResult.getOperationStatistics().getSuccessItemsCount(),2l);

        Assert.assertEquals(_Type02.countConceptionEntities(),new Long("2"));
        Assert.assertEquals(_Type01.countConceptionEntities(),new Long("2"));
    }
}
