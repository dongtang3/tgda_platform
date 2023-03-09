package com.github.tgda.compute.dataSliceTest;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.coreRealm.realmServiceCore.term.*;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import com.github.tgda.compute.applicationCapacity.compute.dataComputeUnit.dataService.DataSlicePropertyType;
import com.github.tgda.compute.applicationCapacity.compute.dataComputeUnit.dataService.result.DataSliceOperationResult;
import com.github.tgda.compute.applicationCapacity.compute.dataComputeUnit.util.CoreRealmOperationUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CoreRealmOperationUtilTest {

    public static void main(String[] args) throws EngineServiceRuntimeException {

        CoreRealm coreRealm = EngineFactory.getDefaultEngine();
        ConceptionKind targetConceptionKind = coreRealm.getConceptionKind("Fire911Call");
        List<AttributeKind> attributeKindList = targetConceptionKind.getContainsSingleValueAttributeKinds();
        if(attributeKindList.size() ==0){
            AttributesViewKind targetAttributesViewKind = coreRealm.createAttributesViewKind("defaultView","defaultView",null);
            targetConceptionKind.attachAttributesViewKind(targetAttributesViewKind.getAttributesViewKindUID());

            AttributeKind attributeKind01 = coreRealm.createAttributeKind("address","address", AttributeDataType.STRING);
            AttributeKind attributeKind02 = coreRealm.createAttributeKind("type","type", AttributeDataType.STRING);
            AttributeKind attributeKind03 = coreRealm.createAttributeKind("datetime","datetime", AttributeDataType.TIMESTAMP);
            AttributeKind attributeKind04 = coreRealm.createAttributeKind("latitude","latitude", AttributeDataType.DOUBLE);
            AttributeKind attributeKind05 = coreRealm.createAttributeKind("longitude","longitude", AttributeDataType.DOUBLE);
            AttributeKind attributeKind06 = coreRealm.createAttributeKind("location","location", AttributeDataType.STRING);
            AttributeKind attributeKind07 = coreRealm.createAttributeKind("incidentNumber","incidentNumber", AttributeDataType.STRING);

            targetAttributesViewKind.attachAttributeKind(attributeKind01.getAttributeKindUID());
            targetAttributesViewKind.attachAttributeKind(attributeKind02.getAttributeKindUID());
            targetAttributesViewKind.attachAttributeKind(attributeKind03.getAttributeKindUID());
            targetAttributesViewKind.attachAttributeKind(attributeKind04.getAttributeKindUID());
            targetAttributesViewKind.attachAttributeKind(attributeKind05.getAttributeKindUID());
            targetAttributesViewKind.attachAttributeKind(attributeKind06.getAttributeKindUID());
            targetAttributesViewKind.attachAttributeKind(attributeKind07.getAttributeKindUID());
        }

        //DataSliceOperationResult dataSliceOperationResult = CoreRealmOperationUtil.syncConceptionKindToDataSlice("Fire911Call",null,null);

        Map<String, DataSlicePropertyType> dataSlicePropertyMap = new HashMap<>();
        dataSlicePropertyMap.put("address",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("type",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("datetime",DataSlicePropertyType.DATE);
        dataSlicePropertyMap.put("latitude",DataSlicePropertyType.DOUBLE);
        dataSlicePropertyMap.put("longitude",DataSlicePropertyType.DOUBLE);
        dataSlicePropertyMap.put("location",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("incidentNumber",DataSlicePropertyType.STRING);

        DataSliceOperationResult dataSliceOperationResult = CoreRealmOperationUtil.syncConceptionKindToDataSlice("Fire911Call",null,null,dataSlicePropertyMap,null);

        System.out.println(dataSliceOperationResult.getStartTime());
        System.out.println(dataSliceOperationResult.getFinishTime());
        System.out.println(dataSliceOperationResult.getSuccessItemsCount());
        System.out.println(dataSliceOperationResult.getFailItemsCount());
        System.out.println(dataSliceOperationResult.getOperationSummary());
    }
}
