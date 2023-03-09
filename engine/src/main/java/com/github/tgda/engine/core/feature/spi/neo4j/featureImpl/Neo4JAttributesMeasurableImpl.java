package com.github.tgda.engine.core.feature.spi.neo4j.featureImpl;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.feature.AttributesMeasurable;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetBooleanFormatReturnValueTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetListFormatAggregatedReturnValueTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetMapFormatAggregatedReturnValueTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleAttributeValueTransformer;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.payload.AttributeValue;
import com.github.tgda.engine.core.term.AttributeDataType;
import com.github.tgda.engine.core.util.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

public class Neo4JAttributesMeasurableImpl implements AttributesMeasurable {

    private static Logger logger = LoggerFactory.getLogger(Neo4JAttributesMeasurableImpl.class);
    private String entityUID;
    private boolean isRelationshipEntity = false;

    public Neo4JAttributesMeasurableImpl(String entityUID){
        this.entityUID = entityUID;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    public Neo4JAttributesMeasurableImpl(String entityUID,boolean isRelationshipEntity){
        this.entityUID = entityUID;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
        this.isRelationshipEntity = isRelationshipEntity;
    }

    @Override
    public boolean removeAttribute(String attributeName) throws EngineServiceRuntimeException {
        if (this.entityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String queryCql;
                if(this.isRelationshipEntity){
                    queryCql = CypherBuilder.matchRelationWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),CypherBuilder.CypherFunctionType.EXISTS,attributeName);
                }else{
                    queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(this.entityUID), CypherBuilder.CypherFunctionType.EXISTS, attributeName);
                }
                GetBooleanFormatReturnValueTransformer getBooleanFormatReturnValueTransformer = new GetBooleanFormatReturnValueTransformer();
                Object resultRes = workingGraphOperationExecutor.executeRead(getBooleanFormatReturnValueTransformer,queryCql);
                boolean existCheckKResult = resultRes != null ? ((Boolean) resultRes).booleanValue() : false;
                if (!existCheckKResult) {
                    logger.error("Attribute {} of entity with UID {} does not exist.", attributeName, this.entityUID);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("Attribute " + attributeName + " of entity with UID " + this.entityUID + " does not exist.");
                    throw exception;
                }else{
                    List<String> targetAttributeNameList = new ArrayList<>();
                    targetAttributeNameList.add(attributeName);
                    String deleteCql = null;
                    if(this.isRelationshipEntity){
                        deleteCql = CypherBuilder.removeRelationPropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),targetAttributeNameList);
                    }else{
                        deleteCql = CypherBuilder.removeNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),targetAttributeNameList);
                    }
                    GetListFormatAggregatedReturnValueTransformer getListFormatAggregatedReturnValueTransformer = new GetListFormatAggregatedReturnValueTransformer("keys");
                    Object removeResultRes = workingGraphOperationExecutor.executeWrite(getListFormatAggregatedReturnValueTransformer,deleteCql);
                    CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.entityUID,isRelationshipEntity);
                    List<String> returnAttributeNameList = (List<String>)removeResultRes;
                    if(returnAttributeNameList.contains(attributeName)){
                        return false;
                    }else{
                        return true;
                    }
                }
            }finally{
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    @Override
    public List<AttributeValue> getAttributes() {
        if (this.entityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String queryCql = null;
                if(this.isRelationshipEntity){
                    queryCql = CypherBuilder.matchRelationWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),CypherBuilder.CypherFunctionType.PROPERTIES,null);
                }else{
                    queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),CypherBuilder.CypherFunctionType.PROPERTIES,null);
                }
                GetMapFormatAggregatedReturnValueTransformer getMapFormatAggregatedReturnValueTransformer = new GetMapFormatAggregatedReturnValueTransformer("properties");
                Object resultRes = workingGraphOperationExecutor.executeRead(getMapFormatAggregatedReturnValueTransformer,queryCql);
                Map attributeValuesMap = (Map)resultRes;
                List<AttributeValue> attributeValueList = CommonOperationUtil.getAttributeValueList(attributeValuesMap);
                return attributeValueList;
            }finally{
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public boolean hasAttribute(String attributeName) {
        if (this.entityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String queryCql;
                if(this.isRelationshipEntity){
                    queryCql = CypherBuilder.matchRelationWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),CypherBuilder.CypherFunctionType.EXISTS,attributeName);
                }else{
                    queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(this.entityUID), CypherBuilder.CypherFunctionType.EXISTS, attributeName);
                }
                GetBooleanFormatReturnValueTransformer getBooleanFormatReturnValueTransformer = new GetBooleanFormatReturnValueTransformer();
                Object resultRes = workingGraphOperationExecutor.executeRead(getBooleanFormatReturnValueTransformer,queryCql);
                return resultRes != null ? (Boolean)resultRes : false;
            }finally{
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    @Override
    public List<String> getAttributeNames() {
        if (this.entityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String queryCql;
                if(this.isRelationshipEntity){
                    queryCql = CypherBuilder.matchRelationWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),CypherBuilder.CypherFunctionType.KEYS,null);
                }else{
                    queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),CypherBuilder.CypherFunctionType.KEYS,null);
                }
                GetListFormatAggregatedReturnValueTransformer getListFormatAggregatedReturnValueTransformer = new GetListFormatAggregatedReturnValueTransformer("keys");
                Object resultRes = workingGraphOperationExecutor.executeRead(getListFormatAggregatedReturnValueTransformer,queryCql);
                List<String> returnAttributeNameList = (List<String>)resultRes;
                List<String> resultAttributeNameList = CommonOperationUtil.clearSystemBuiltinAttributeNames(returnAttributeNameList);
                return resultAttributeNameList;
            }finally{
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public AttributeValue getAttribute(String attributeName) {
        if (this.entityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String queryCql = null;
                if(this.isRelationshipEntity){
                    queryCql = CypherBuilder.matchRelationPropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),new String[]{attributeName});
                }else{
                    queryCql = CypherBuilder.matchNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),new String[]{attributeName});
                }
                GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(attributeName);
                Object resultRes = workingGraphOperationExecutor.executeRead(getSingleAttributeValueTransformer,queryCql);
                return resultRes != null?(AttributeValue)resultRes : null;
            }finally{
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public AttributeValue addAttribute(String attributeName, boolean attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,Boolean.valueOf(attributeValue));
    }

    @Override
    public AttributeValue addAttribute(String attributeName, int attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,Integer.valueOf(attributeValue));
    }

    @Override
    public AttributeValue addAttribute(String attributeName, short attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,Short.valueOf(attributeValue));
    }

    @Override
    public AttributeValue addAttribute(String attributeName, long attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,Long.valueOf(attributeValue));
    }

    @Override
    public AttributeValue addAttribute(String attributeName, float attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,Float.valueOf(attributeValue));
    }

    @Override
    public AttributeValue addAttribute(String attributeName, double attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,Double.valueOf(attributeValue));
    }

    @Override
    public AttributeValue addAttribute(String attributeName, Date attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, LocalDateTime attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, LocalDate attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, LocalTime attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, String attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, byte[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, byte attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,Byte.valueOf(attributeValue));
    }

    @Override
    public AttributeValue addAttribute(String attributeName, BigDecimal attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, Boolean[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, Integer[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, Short[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, Long[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, Float[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, Double[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, Date[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, LocalDateTime[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, LocalDate[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, LocalTime[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, String[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, Byte[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue addAttribute(String attributeName, BigDecimal[] attributeValue) throws EngineServiceRuntimeException {
        return setAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, boolean attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,Boolean.valueOf(attributeValue));
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, int attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,Integer.valueOf(attributeValue));
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, short attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,Short.valueOf(attributeValue));
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, long attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,Long.valueOf(attributeValue));
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, float attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,Float.valueOf(attributeValue));
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, double attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,Double.valueOf(attributeValue));
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, Date attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, LocalDateTime attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, LocalDate attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, LocalTime attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, String attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, byte[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, byte attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,Byte.valueOf(attributeValue));
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, BigDecimal attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, Boolean[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, Integer[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, Short[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, Long[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, Float[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, Double[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, Date[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, LocalDateTime[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, LocalDate[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, LocalTime[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, String[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, Byte[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public AttributeValue updateAttribute(String attributeName, BigDecimal[] attributeValue) throws EngineServiceRuntimeException {
        return checkAndUpdateAttribute(attributeName,attributeValue);
    }

    @Override
    public List<String> addAttributes(Map<String, Object> properties) {
        if (this.entityUID != null && properties != null && properties.size() > 0) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String queryCql;
                if(this.isRelationshipEntity){
                    queryCql = CypherBuilder.matchRelationWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(this.entityUID), CypherBuilder.CypherFunctionType.KEYS, null);
                }else{
                    queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(this.entityUID), CypherBuilder.CypherFunctionType.KEYS, null);
                }
                GetListFormatAggregatedReturnValueTransformer getListFormatAggregatedReturnValueTransformer = new GetListFormatAggregatedReturnValueTransformer("keys");
                Object resultRes = workingGraphOperationExecutor.executeRead(getListFormatAggregatedReturnValueTransformer, queryCql);
                List<String> returnAttributeNameList = (List<String>) resultRes;
                Set<String> newDataKeys = properties.keySet();
                List<String> realTargetAttributeKeys = new ArrayList<>();
                List<String> dupAttributeKeys = new ArrayList<>();
                for(String currentKey:newDataKeys){
                    if(!returnAttributeNameList.contains(currentKey)){
                        realTargetAttributeKeys.add(currentKey);
                    }else{
                        dupAttributeKeys.add(currentKey);
                    }
                }
                for(String currentDupKey:dupAttributeKeys){
                    properties.remove(currentDupKey);
                }

                String createCql = null;
                if(this.isRelationshipEntity){
                    createCql = CypherBuilder.setRelationPropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),properties);
                }else {
                    createCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(this.entityUID), properties);
                }

                GetMapFormatAggregatedReturnValueTransformer getMapFormatAggregatedReturnValueTransformer = new GetMapFormatAggregatedReturnValueTransformer();
                Object addAttributeResultRes = workingGraphOperationExecutor.executeWrite(getMapFormatAggregatedReturnValueTransformer,createCql);
                CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.entityUID,isRelationshipEntity);
                if(addAttributeResultRes!=null){
                    List<String> successNameList = new ArrayList<>();
                    Map<String,Object> newAddedAttributesMap = (Map<String,Object>)addAttributeResultRes;
                    for(String currentNewKeyToAdd:realTargetAttributeKeys){
                        String returnedName = CypherBuilder.operationResultName+"."+currentNewKeyToAdd;
                        if(newAddedAttributesMap.containsKey(returnedName)){
                            successNameList.add(currentNewKeyToAdd);
                        }
                    }
                    return successNameList;
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public List<String> updateAttributes(Map<String, Object> properties) {
        if (this.entityUID != null && properties != null && properties.size() > 0) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String queryCql;
                if(this.isRelationshipEntity){
                    queryCql = CypherBuilder.matchRelationWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),CypherBuilder.CypherFunctionType.PROPERTIES,null);
                }else{
                    queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),CypherBuilder.CypherFunctionType.PROPERTIES,null);
                }
                GetMapFormatAggregatedReturnValueTransformer getMapFormatAggregatedReturnValueTransformer = new GetMapFormatAggregatedReturnValueTransformer("properties");
                Object resultRes = workingGraphOperationExecutor.executeRead(getMapFormatAggregatedReturnValueTransformer,queryCql);

                Map<String,AttributeValue> currentAttributeValueMap = new HashMap<>();

                Map<String, Object> attributesValueMap = (Map<String, Object>)resultRes;
                if(attributesValueMap != null){
                    for(Object key : attributesValueMap.keySet()){
                        if(!key.equals(Constant._createDateProperty)&&
                                !key.equals(Constant._lastModifyDateProperty)&&
                                !key.equals(Constant._dataOriginProperty)){
                            Object attributeValueObject = attributesValueMap.get(key);
                            AttributeValue currentAttributeValue = CommonOperationUtil.getAttributeValue(key.toString(),attributeValueObject);
                            currentAttributeValueMap.put(key.toString(),currentAttributeValue);
                        }
                    }
                }

                Set<String> newDataKeys = properties.keySet();
                List<String> diffTargetAttributeKeys = new ArrayList<>();
                List<String> dupAttributeKeys = new ArrayList<>();
                for(String currentKey:newDataKeys){
                    if(!currentAttributeValueMap.containsKey(currentKey)){
                        diffTargetAttributeKeys.add(currentKey);
                    }else{
                        dupAttributeKeys.add(currentKey);
                    }
                }
                for(String currentDiffKey:diffTargetAttributeKeys){
                    properties.remove(currentDiffKey);
                }
                for(String currentDupKey:dupAttributeKeys){
                    if(!CommonOperationUtil.validateValueFormat(currentAttributeValueMap.get(currentDupKey).getAttributeDataType(),properties.get(currentDupKey))){
                        properties.remove(currentDupKey);
                    }
                }

                String createCql = null;
                if(this.isRelationshipEntity){
                    createCql = CypherBuilder.setRelationPropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),properties);
                }else {
                    createCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),properties);
                }
                getMapFormatAggregatedReturnValueTransformer = new GetMapFormatAggregatedReturnValueTransformer();
                Object addAttributeResultRes = workingGraphOperationExecutor.executeWrite(getMapFormatAggregatedReturnValueTransformer,createCql);
                CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.entityUID,isRelationshipEntity);
                if(addAttributeResultRes!=null){
                    List<String> successNameList = new ArrayList<>();
                    Map<String,Object> newAddedAttributesMap = (Map<String,Object>)addAttributeResultRes;
                    for(String currentNewKeyToAdd:dupAttributeKeys){
                        String returnedName = CypherBuilder.operationResultName+"."+currentNewKeyToAdd;
                        if(newAddedAttributesMap.containsKey(returnedName)){
                            successNameList.add(currentNewKeyToAdd);
                        }
                    }
                    return successNameList;
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public List<String> addNewOrUpdateAttributes(Map<String, Object> properties) {
        HashMap<String,Object> propertiesForAddMap = new HashMap<>();
        propertiesForAddMap.putAll(properties);
        List<String> successAttributeNamesList = new ArrayList<>();
        List<String> updSuccessList = updateAttributes(properties);
        List<String> addSuccessList = addAttributes(propertiesForAddMap);
        if(addSuccessList != null){
            successAttributeNamesList.addAll(addSuccessList);
        }
        if(updSuccessList != null){
            successAttributeNamesList.addAll(updSuccessList);
        }
        return successAttributeNamesList;
    }

    private AttributeValue setAttribute(String attributeName, Object attributeValue) throws EngineServiceRuntimeException {
        if (this.entityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String queryCql;
                if(this.isRelationshipEntity){
                    queryCql = CypherBuilder.matchRelationWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(this.entityUID), CypherBuilder.CypherFunctionType.EXISTS, attributeName);
                }else{
                    queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(this.entityUID), CypherBuilder.CypherFunctionType.EXISTS, attributeName);
                }
                GetBooleanFormatReturnValueTransformer getBooleanFormatReturnValueTransformer = new GetBooleanFormatReturnValueTransformer();
                Object checkExistResultRes = workingGraphOperationExecutor.executeRead(getBooleanFormatReturnValueTransformer,queryCql);
                boolean checkExistResult = checkExistResultRes != null? ((Boolean)checkExistResultRes).booleanValue() : false;
                if(checkExistResult){
                    logger.error("Attribute {} of entity with UID {} already exist.", attributeName, this.entityUID);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("Attribute " + attributeName + " of entity with UID " + this.entityUID + " already exist.");
                    throw exception;
                }else{
                    Map<String,Object> attributeDataMap = new HashMap<>();
                    attributeDataMap.put(attributeName,attributeValue);

                    String createCql = null;
                    if(this.isRelationshipEntity){
                        createCql = CypherBuilder.setRelationPropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),attributeDataMap);
                    }else {
                        createCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),attributeDataMap);
                    }

                    GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(attributeName);
                    Object resultRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeValueTransformer,createCql);
                    CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.entityUID,isRelationshipEntity);
                    return resultRes != null?(AttributeValue)resultRes : null;
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    private AttributeValue checkAndUpdateAttribute(String attributeName, Object attributeValue) throws EngineServiceRuntimeException {
        if (this.entityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String queryCql;
                if(this.isRelationshipEntity){
                    queryCql = CypherBuilder.matchRelationPropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(this.entityUID), new String[]{attributeName});
                }else{
                    queryCql = CypherBuilder.matchNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(this.entityUID), new String[]{attributeName});
                }
                GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(attributeName);
                Object resultRes = workingGraphOperationExecutor.executeRead(getSingleAttributeValueTransformer, queryCql);
                if (resultRes != null) {
                    AttributeValue originalAttributeValue = (AttributeValue) resultRes;
                    AttributeDataType originalAttributeDataType = originalAttributeValue.getAttributeDataType();
                    boolean isValidatedFormat = CommonOperationUtil.validateValueFormat(originalAttributeDataType, attributeValue);
                    if(!isValidatedFormat){
                        logger.error("Attribute  data type does not match {} of entity with UID {}.", attributeName, this.entityUID);
                        EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                        exception.setCauseMessage("Attribute data type does not match " + attributeName + " of entity with UID " + this.entityUID +".");
                        throw exception;
                    }else{
                        Map<String,Object> attributeDataMap = new HashMap<>();
                        attributeDataMap.put(attributeName,attributeValue);
                        String updateCql = null;
                        if(this.isRelationshipEntity){
                            updateCql = CypherBuilder.setRelationPropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),attributeDataMap);
                        }else{
                            updateCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.entityUID),attributeDataMap);
                        }
                        Object updateResultRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeValueTransformer,updateCql);
                        CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.entityUID,isRelationshipEntity);
                        return updateResultRes != null ? (AttributeValue) updateResultRes : null;
                    }
                }else {
                    logger.error("Attribute {} of entity with UID {} does not exist.", attributeName, this.entityUID);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("Attribute " + attributeName + " of entity with UID " + this.entityUID + " does not exist.");
                    throw exception;
                }
            } finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }
}
