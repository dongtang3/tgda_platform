package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.term.RelationshipType;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Date;

public class MetaAttributeFeatureSupportableTest {

    private static String testRealmName = "UNIT_TEST_Realm";
    private static String testConceptionKindName = "TestConceptionKindForMetaAttributeFeature";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for MetaAttributeFeatureSupportableTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testMetaAttributeFeatureSupportableFunction() throws EngineServiceRuntimeException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        Assert.assertEquals(coreRealm.getStorageImplTech(), StorageImplTech.NEO4J);

        Type _Type01 = coreRealm.getType(testConceptionKindName);
        if(_Type01 != null){
            coreRealm.removeType(testConceptionKindName,true);
        }
        _Type01 = coreRealm.getType(testConceptionKindName);
        if(_Type01 == null){
            _Type01 = coreRealm.createType(testConceptionKindName,"TestMetaAttributeFeatureDesc+中文描述");
            Assert.assertNotNull(_Type01);
            Assert.assertEquals(_Type01.getConceptionKindName(),testConceptionKindName);
            Assert.assertEquals(_Type01.getConceptionKindDesc(),"TestMetaAttributeFeatureDesc+中文描述");
        }

        Assert.assertNotNull(_Type01.getDataOrigin());
        Assert.assertNull(_Type01.getCreatorId());

        Date createdDateTime = _Type01.getCreateDateTime();
        Assert.assertNotNull(createdDateTime);

        Date lastModifyDateTime = _Type01.getLastModifyDateTime();
        Assert.assertNotNull(lastModifyDateTime);

        boolean updateResult = _Type01.updateLastModifyDateTime();
        Assert.assertTrue(updateResult);
        Date lastModifyDateTime2 = _Type01.getLastModifyDateTime();
        Assert.assertNotNull(lastModifyDateTime2);
        Assert.assertTrue(lastModifyDateTime2.getTime() > lastModifyDateTime.getTime());

        updateResult = _Type01.updateCreatorId("creatorID001");
        Assert.assertTrue(updateResult);
        String newCreatorID = _Type01.getCreatorId();
        Assert.assertEquals(newCreatorID,"creatorID001");

        String dataOrigin = _Type01.getDataOrigin();
        updateResult = _Type01.updateDataOrigin(dataOrigin+"NewValue");
        Assert.assertTrue(updateResult);

        String newDataOrigin = _Type01.getDataOrigin();
        Assert.assertEquals(newDataOrigin,dataOrigin+"NewValue");

        Date newCreatDate = _Type01.getCreateDateTime();
        Assert.assertEquals(createdDateTime,newCreatDate);

        RelationshipType targetRelationship01Type = coreRealm.getRelationshipType("relationKindForMetaAttributeTest");
        if(targetRelationship01Type == null){
            targetRelationship01Type = coreRealm.createRelationshipType("relationKindForMetaAttributeTest",null);
        }
        Assert.assertNotNull(targetRelationship01Type.getCreateDateTime());
        Assert.assertNotNull(targetRelationship01Type.getLastModifyDateTime());
        Assert.assertNotNull(targetRelationship01Type.getDataOrigin());
    }
}
