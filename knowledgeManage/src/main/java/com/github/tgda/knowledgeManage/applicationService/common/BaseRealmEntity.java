package com.github.tgda.knowledgeManage.applicationService.common;

import com.github.tgda.engine.core.term.Engine;

import java.util.HashMap;
import java.util.Map;

public class BaseRealmEntity {

    private String entityUID;
    private Engine coreRealm;
    private Map<String, Object> attributeMap = new HashMap<>();

    public BaseRealmEntity() {}

    public BaseRealmEntity(String entityUID, Engine coreRealm) {
        this.setEntityUID(entityUID);
        this.setCoreRealm(coreRealm);
    }

    public void set(String propertyName,Object objectValue) {
        if(attributeMap == null) {
            attributeMap = new HashMap<>();
        }
        attributeMap.put(propertyName,objectValue);
    }

    public Object get(String property) {
        if(attributeMap!=null) {
            if (attributeMap.containsKey(property)) {
                return attributeMap.get(property);
            }
        }
        return null;

        /*
        try {
            Object object = getFact().getProperty(property).getPropertyValue();
            map.put(property, object);
            return object;
        } catch (Exception e) {
        //            e.printStackTrace();
        }

        map.put(property, null);
        return null;

         */
    }

    public void link(BaseRealmEntity destBaseRealmEntity, String relationTypeName,Map<String,Object> relationData) {
        System.out.println("do link operation");
        /*
        try {
            this.getFact().addToRelation(dastfact.getFact(),relationTypeName,false);
        } catch (CimDataEngineRuntimeException e) {
            e.printStackTrace();
        }
         */
    }

    public String getEntityUID() {
        return entityUID;
    }

    public void setEntityUID(String entityUID) {
        this.entityUID = entityUID;
    }

    public Engine getCoreRealm() {
        return coreRealm;
    }

    public void setCoreRealm(Engine coreRealm) {
        this.coreRealm = coreRealm;
    }
}
