package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.term.AttributesView;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.term.RelationshipType;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.Map;

public class MetaConfigItemFeatureSupportableTest {

    private static String testRealmName = "UNIT_TEST_Realm";
    private static String testAttributesViewKindName = "TestAttributesViewKindForMetaConfigItemFeature";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for MetaConfigItemFeatureSupportableTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testMetaConfigItemFeatureSupportableFunction() throws EngineServiceRuntimeException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        Assert.assertEquals(coreRealm.getStorageImplTech(), StorageImplTech.NEO4J);
        AttributesView targetAttributesView = coreRealm.createAttributesView(testAttributesViewKindName,"desc",null);

        boolean addConfigItemResult = targetAttributesView.addOrUpdateMetaConfigItem("configItem1",new Date());
        Assert.assertTrue(addConfigItemResult);

        Map<String,Object> metaConfigItemsMap = targetAttributesView.getMetaConfigItems();
        Assert.assertNotNull(metaConfigItemsMap);

        Assert.assertEquals(metaConfigItemsMap.size(),1);
        Assert.assertNotNull(metaConfigItemsMap.get("configItem1"));
        Assert.assertTrue(metaConfigItemsMap.get("configItem1") instanceof Date);

        addConfigItemResult = targetAttributesView.addOrUpdateMetaConfigItem("configItem2",Long.valueOf(10000));
        Assert.assertTrue(addConfigItemResult);

        metaConfigItemsMap = targetAttributesView.getMetaConfigItems();
        Assert.assertEquals(metaConfigItemsMap.size(),2);
        Assert.assertNotNull(metaConfigItemsMap.get("configItem2"));
        Assert.assertEquals(metaConfigItemsMap.get("configItem2"),Long.valueOf(10000));

        Object item2Result = targetAttributesView.getMetaConfigItem("configItem2");
        Assert.assertTrue(item2Result instanceof Long);
        Assert.assertEquals(item2Result,Long.valueOf(10000));

        addConfigItemResult = targetAttributesView.addOrUpdateMetaConfigItem("configItem2",Long.valueOf(50000));
        Assert.assertTrue(addConfigItemResult);

        item2Result = targetAttributesView.getMetaConfigItem("configItem2");
        Assert.assertTrue(item2Result instanceof Long);
        Assert.assertEquals(item2Result,Long.valueOf(50000));

        Object itemNotExistResult = targetAttributesView.getMetaConfigItem("configItemNotExist");
        Assert.assertNull(itemNotExistResult);

        boolean deleteItemResult = targetAttributesView.deleteMetaConfigItem("configItem2");
        Assert.assertTrue(deleteItemResult);
        itemNotExistResult = targetAttributesView.getMetaConfigItem("configItem2");
        Assert.assertNull(itemNotExistResult);

        metaConfigItemsMap = targetAttributesView.getMetaConfigItems();
        Assert.assertNotNull(metaConfigItemsMap);
        Assert.assertEquals(metaConfigItemsMap.size(),1);

        Type _Type01 = coreRealm.getType("TestConceptionKindC");
        if(_Type01 != null){
            coreRealm.removeType("TestConceptionKindC",true);
        }
        _Type01 = coreRealm.getType("TestConceptionKindC");
        if(_Type01 == null){
            _Type01 = coreRealm.createType("TestConceptionKindC","TestConceptionKindCDesc+中文描述");
            Assert.assertNotNull(_Type01);
        }
        metaConfigItemsMap = _Type01.getMetaConfigItems();
        Assert.assertNotNull(metaConfigItemsMap);
        Assert.assertEquals(metaConfigItemsMap.size(),0);
        boolean addConfigItemResult2 = _Type01.addOrUpdateMetaConfigItem("configItem1",new Date());
        Assert.assertTrue(addConfigItemResult2);
        metaConfigItemsMap = _Type01.getMetaConfigItems();
        Assert.assertNotNull(metaConfigItemsMap);
        Assert.assertEquals(metaConfigItemsMap.size(),1);

        RelationshipType targetRelationship01Type = coreRealm.getRelationshipType("relationKindForMetaConfigTest");
        if(targetRelationship01Type != null){
            coreRealm.removeRelationshipType("relationKindForMetaConfigTest",true);
        }
        targetRelationship01Type = coreRealm.getRelationshipType("relationKindForMetaConfigTest");
        if(targetRelationship01Type == null){
            targetRelationship01Type = coreRealm.createRelationshipType("relationKindForMetaConfigTest",null);
        }
        metaConfigItemsMap = targetRelationship01Type.getMetaConfigItems();
        Assert.assertNotNull(metaConfigItemsMap);
        Assert.assertEquals(metaConfigItemsMap.size(),0);
        boolean addConfigItemResult3 = targetRelationship01Type.addOrUpdateMetaConfigItem("configItem2",new Date());
        Assert.assertTrue(addConfigItemResult3);
        metaConfigItemsMap = targetRelationship01Type.getMetaConfigItems();
        Assert.assertNotNull(metaConfigItemsMap);
        Assert.assertEquals(metaConfigItemsMap.size(),1);
    }
}
