package com.github.tgda.engine.core.feature;

import com.github.tgda.engine.core.payload.dataValueObject.AttributeVO;
import com.github.tgda.engine.core.payload.dataValueObject.AttributesViewVO;
import com.github.tgda.engine.core.payload.dataValueObject.RelationshipTypeVO;
import com.github.tgda.engine.core.payload.dataValueObject.TypeVO;
import com.github.tgda.engine.core.term.*;
import com.github.tgda.engine.core.util.cache.ResourceCache;
import com.github.tgda.engine.core.util.cache.ResourceCacheHolder;

public interface TypeCacheable<K,V>{
    String ATTRIBUTE_KIND_CACHE = "ATTRIBUTE_KIND_CACHE";
    String ATTRIBUTES_VIEW_KIND_CACHE = "ATTRIBUTES_VIEW_KIND_CACHE";
    String CLASSIFICATION_KIND_CACHE = "CLASSIFICATION_KIND_CACHE";
    String CONCEPTION_KIND_CACHE = "CONCEPTION_KIND_CACHE";
    String RELATION_ATTACH_KIND_CACHE = "RELATION_ATTACH_KIND_CACHE";
    String RELATION_KIND_CACHE = "RELATION_KIND_CACHE";
    enum CacheOperationType {INSERT, UPDATE, DELETE}

    public default void executeAttributeCacheOperation(Attribute attribute, CacheOperationType cacheOperationType) {
        ResourceCacheHolder resourceCacheHolder = ResourceCacheHolder.getInstance();
        ResourceCache<String, AttributeVO> cache = resourceCacheHolder.getOrCreateCache(ATTRIBUTE_KIND_CACHE, String.class, AttributeVO.class);
        if(attribute != null){
            String cacheItemKey = attribute.getAttributeKindUID();
            AttributeVO attributeVO = new AttributeVO(attribute.getAttributeKindName(), attribute.getAttributeKindDesc(),
                    attribute.getAttributeDataType(), attribute.getAttributeKindUID());
            accessCacheData(cache,cacheOperationType,cacheItemKey, attributeVO);
        }
    }

    public default void executeAttributesViewCacheOperation(AttributesView attributesViewKind, CacheOperationType cacheOperationType) {
        ResourceCacheHolder resourceCacheHolder = ResourceCacheHolder.getInstance();
        ResourceCache<String, AttributesViewVO> cache = resourceCacheHolder.getOrCreateCache(ATTRIBUTES_VIEW_KIND_CACHE, String.class, AttributesViewVO.class);
        if(attributesViewKind != null){
            String cacheItemKey = attributesViewKind.getAttributesViewKindUID();
            AttributesViewVO attributesViewKindVO = new AttributesViewVO(attributesViewKind.getAttributesViewKindName(),attributesViewKind.getAttributesViewKindDesc(),
                    attributesViewKind.getAttributesViewKindDataForm(),attributesViewKind.getAttributesViewKindUID());
            accessCacheData(cache,cacheOperationType,cacheItemKey,attributesViewKindVO);
        }
    }

    public default void executeClassificationCacheOperation(Classification classification, CacheOperationType cacheOperationType) {

    }

    public default void executeTypeCacheOperation(Type conceptionKind, CacheOperationType cacheOperationType) {
        ResourceCacheHolder resourceCacheHolder = ResourceCacheHolder.getInstance();
        ResourceCache<String, TypeVO> cache = resourceCacheHolder.getOrCreateCache(CONCEPTION_KIND_CACHE, String.class, TypeVO.class);
        if(conceptionKind != null){
            String cacheItemKey = conceptionKind.getConceptionKindName();
            TypeVO conceptionKindVO = new TypeVO(conceptionKind.getConceptionKindName(),conceptionKind.getConceptionKindDesc());
            accessCacheData(cache,cacheOperationType,cacheItemKey,conceptionKindVO);
        }
    }

    public default void executeRelationshipAttachCacheOperation(RelationshipAttach relationAttachKind, CacheOperationType cacheOperationType) {

    }

    public default void executeRelationshipTyepCacheOperation(RelationshipType relationKind, CacheOperationType cacheOperationType) {
        ResourceCacheHolder resourceCacheHolder = ResourceCacheHolder.getInstance();
        ResourceCache<String, RelationshipTypeVO> cache = resourceCacheHolder.getOrCreateCache(RELATION_KIND_CACHE, String.class, RelationshipTypeVO.class);
        if(relationKind != null){
            String cacheItemKey = relationKind.getRelationKindName();
            RelationshipTypeVO relationKindVO = new RelationshipTypeVO(relationKind.getRelationKindName(),relationKind.getRelationKindDesc());
            accessCacheData(cache,cacheOperationType,cacheItemKey,relationKindVO);
        }
    }

    private void accessCacheData(ResourceCache cache,CacheOperationType cacheOperationType,String cacheItemKey,Object cacheData){
        switch(cacheOperationType){
            case INSERT:
                if(!cache.containsCacheItem(cacheItemKey)){
                    cache.addCacheItem(cacheItemKey,cacheData);
                }
                break;
            case DELETE:
                if(cache.containsCacheItem(cacheItemKey)){
                    cache.removeCacheItem(cacheItemKey);
                }
                break;
            case UPDATE:
                if(cache.containsCacheItem(cacheItemKey)){
                    cache.updateCacheItem(cacheItemKey,cacheData);
                }else{
                    cache.addCacheItem(cacheItemKey,cacheData);
                }
                break;
        }
    }
}
