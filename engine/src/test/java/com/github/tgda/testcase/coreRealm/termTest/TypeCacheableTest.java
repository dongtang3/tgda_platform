package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.feature.TypeCacheable;
import com.github.tgda.engine.core.payload.dataValueObject.AttributeVO;
import com.github.tgda.engine.core.payload.dataValueObject.AttributesViewVO;
import com.github.tgda.engine.core.payload.dataValueObject.TypeVO;
import com.github.tgda.engine.core.payload.dataValueObject.RelationshipTypeVO;
import com.github.tgda.coreRealm.realmServiceCore.term.*;
import com.github.tgda.engine.core.util.cache.ResourceCache;
import com.github.tgda.engine.core.util.cache.ResourceCacheHolder;
import com.github.tgda.engine.core.util.factory.EngineFactory;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class TypeCacheableTest {

    private static String testRealmName = "UNIT_TEST_Realm";
    private static String testConceptionKindName = "TestConceptionKind_03";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for KindCacheableTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testKindCacheableTestFunction() throws EngineServiceRuntimeException {
        CoreRealm coreRealm = EngineFactory.getDefaultEngine();
        ResourceCacheHolder resourceCacheHolder = ResourceCacheHolder.getInstance();

        AttributeKind attributeKind01 = coreRealm.createAttributeKind("attributeKind_01","attributeKind01Desc", AttributeDataType.BOOLEAN);
        Assert.assertNotNull(attributeKind01);

        ResourceCache<String, AttributeVO> cache = resourceCacheHolder.getCache(TypeCacheable.ATTRIBUTE_KIND_CACHE,String.class, AttributeVO.class);
        Assert.assertTrue(cache.containsCacheItem(attributeKind01.getAttributeKindUID()));

        AttributeVO targetAttributeKind = cache.getCacheItem(attributeKind01.getAttributeKindUID());
        Assert.assertEquals(targetAttributeKind.getAttributeKindUID(),attributeKind01.getAttributeKindUID());
        Assert.assertEquals(targetAttributeKind.getAttributeKindName(),"attributeKind_01");
        Assert.assertEquals(targetAttributeKind.getAttributeKindDesc(),"attributeKind01Desc");
        Assert.assertEquals(targetAttributeKind.getAttributeDataType(),AttributeDataType.BOOLEAN);

        coreRealm.removeAttributeKind(targetAttributeKind.getAttributeKindUID());
        Assert.assertFalse(cache.containsCacheItem(targetAttributeKind.getAttributeKindUID()));

        AttributesViewKind attributesViewKind01 = coreRealm.createAttributesViewKind("attributesViewKind_01","targetAttributesViewKindADesc",null);
        Assert.assertNotNull(attributesViewKind01);

        ResourceCache<String, AttributesViewVO> cache2 = resourceCacheHolder.getCache(TypeCacheable.ATTRIBUTES_VIEW_KIND_CACHE,String.class, AttributesViewVO.class);
        Assert.assertTrue(cache2.containsCacheItem(attributesViewKind01.getAttributesViewKindUID()));

        AttributesViewVO targetAttributesViewKind = cache2.getCacheItem(attributesViewKind01.getAttributesViewKindUID());
        Assert.assertEquals(targetAttributesViewKind.getAttributesViewKindUID(),attributesViewKind01.getAttributesViewKindUID());
        Assert.assertEquals(targetAttributesViewKind.getAttributesViewKindName(),"attributesViewKind_01");
        Assert.assertEquals(targetAttributesViewKind.getAttributesViewKindDesc(),"targetAttributesViewKindADesc");
        Assert.assertEquals(targetAttributesViewKind.getAttributesViewKindDataForm(), AttributesViewKind.AttributesViewKindDataForm.SINGLE_VALUE);

        coreRealm.removeAttributesViewKind(attributesViewKind01.getAttributesViewKindUID());
        Assert.assertFalse(cache2.containsCacheItem(targetAttributesViewKind.getAttributesViewKindUID()));

        ConceptionKind _ConceptionKind01 = coreRealm.getConceptionKind("conceptionKind_01");
        if(_ConceptionKind01 != null){
            coreRealm.removeConceptionKind("conceptionKind_01",true);
        }
        _ConceptionKind01 = coreRealm.getConceptionKind("conceptionKind_01");
        if(_ConceptionKind01 == null){
            _ConceptionKind01 = coreRealm.createConceptionKind("conceptionKind_01","TestConceptionKindADesc+中文描述");
            Assert.assertNotNull(_ConceptionKind01);
        }

        ResourceCache<String, TypeVO> cache3 = resourceCacheHolder.getCache(TypeCacheable.CONCEPTION_KIND_CACHE,String.class, TypeVO.class);
        Assert.assertTrue(cache3.containsCacheItem(_ConceptionKind01.getConceptionKindName()));
        TypeVO targetConceptionKind = cache3.getCacheItem(_ConceptionKind01.getConceptionKindName());
        Assert.assertEquals(targetConceptionKind.getConceptionKindDesc(),_ConceptionKind01.getConceptionKindDesc());

        coreRealm.removeConceptionKind("conceptionKind_01",true);
        Assert.assertFalse(cache3.containsCacheItem(_ConceptionKind01.getConceptionKindName()));

        RelationKind _RelationKind01 = coreRealm.getRelationKind("relationKind_01");
        if(_RelationKind01 != null){
            coreRealm.removeRelationKind("relationKind_01",true);
        }
        _RelationKind01 = coreRealm.getRelationKind("relationKind_01");
        if(_RelationKind01 == null) {
            _RelationKind01 = coreRealm.createRelationKind("relationKind_01", "relationKind_01Desc");
            Assert.assertNotNull(_RelationKind01);
        }

        ResourceCache<String, RelationshipTypeVO> cache4 = resourceCacheHolder.getCache(TypeCacheable.RELATION_KIND_CACHE,String.class, RelationshipTypeVO.class);
        Assert.assertTrue(cache4.containsCacheItem(_RelationKind01.getRelationKindName()));
        RelationshipTypeVO targetRelationshipTypeVO = cache4.getCacheItem(_RelationKind01.getRelationKindName());
        Assert.assertEquals(targetRelationshipTypeVO.getRelationKindDesc(),_RelationKind01.getRelationKindDesc());

        coreRealm.removeRelationKind("relationKind_01",true);
        Assert.assertFalse(cache4.containsCacheItem(_RelationKind01.getRelationKindName()));

        resourceCacheHolder.shutdownCacheHolder();
    }

}
