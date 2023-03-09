package com.github.tgda.engine.core.feature.spi.neo4j.featureInf;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.FilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.feature.GeospatialScaleCalculable;
import com.github.tgda.engine.core.feature.GeospatialScaleFeatureSupportable;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.coreRealm.realmServiceCore.internal.neo4j.dataTransformer.*;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.util.Constant;
import com.github.tgda.engine.core.util.geospatial.GeospatialCalculateUtil;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public interface Neo4JGeospatialScaleCalculable extends GeospatialScaleCalculable,Neo4JKeyResourcesRetrievable {

    static Logger logger = LoggerFactory.getLogger(Neo4JGeospatialScaleCalculable.class);

    default public List<Entity> getSpatialPredicateMatchedConceptionEntities(String targetConceptionKind,
                                                                             AttributesParameters attributesParameters, SpatialPredicateType spatialPredicateType,
                                                                             SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                Map<String,String> entitiesSpatialContentDataMap = getEntitiesGeospatialScaleContentMap(workingGraphOperationExecutor,targetConceptionKind,attributesParameters,spatialScaleLevel);
                if(entitiesSpatialContentDataMap != null){
                    List<String> entityUIDList = new ArrayList<>();
                    entityUIDList.add(this.getEntityUID());
                    Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);
                    Set<String> matchedEntityUIDSet = GeospatialCalculateUtil.spatialPredicateFilterWKTsCalculate(
                            getGeospatialScaleContentMap.get(this.getEntityUID()),spatialPredicateType,entitiesSpatialContentDataMap);

                    return getConceptionEntitiesByUIDs(workingGraphOperationExecutor,matchedEntityUIDSet);
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public List<Entity> getSpatialBufferMatchedConceptionEntities(String targetConceptionKind,
                                                                          AttributesParameters attributesParameters, double bufferDistanceValue, SpatialPredicateType spatialPredicateType,
                                                                          SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                Map<String,String> entitiesSpatialContentDataMap = getEntitiesGeospatialScaleContentMap(workingGraphOperationExecutor,targetConceptionKind,attributesParameters,spatialScaleLevel);
                if(entitiesSpatialContentDataMap != null){
                    List<String> entityUIDList = new ArrayList<>();
                    entityUIDList.add(this.getEntityUID());
                    Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);
                    Set<String> matchedEntityUIDSet = GeospatialCalculateUtil.spatialBufferPredicateFilterWKTsCalculate(
                            getGeospatialScaleContentMap.get(this.getEntityUID()),bufferDistanceValue,spatialPredicateType,entitiesSpatialContentDataMap);
                    return getConceptionEntitiesByUIDs(workingGraphOperationExecutor,matchedEntityUIDSet);
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public boolean isSpatialPredicateMatchedWith(SpatialPredicateType spatialPredicateType,
            String targetEntityUID, SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null && targetEntityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                boolean isTargetEntityContentValidate=checkGeospatialScaleContentExist(workingGraphOperationExecutor,spatialScaleLevel,targetEntityUID);
                if(!isTargetEntityContentValidate){
                    return false;
                }else{
                    List<String> entityUIDList = new ArrayList<>();
                    entityUIDList.add(this.getEntityUID());
                    entityUIDList.add(targetEntityUID);

                    Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);
                    if(getGeospatialScaleContentMap.size() == 2){
                        return GeospatialCalculateUtil.spatialPredicateWKTCalculate(getGeospatialScaleContentMap.get(this.getEntityUID()),
                                spatialPredicateType,
                                getGeospatialScaleContentMap.get(targetEntityUID));
                    }
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    default public boolean isSpatialPredicateMatchedWith(SpatialPredicateType spatialPredicateType,
                                                         Set<String> targetEntityUIDsSet, SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null && targetEntityUIDsSet != null && targetEntityUIDsSet.size()>0) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                    List<String> entityUIDList = new ArrayList<>();
                    entityUIDList.add(this.getEntityUID());
                    entityUIDList.addAll(targetEntityUIDsSet);
                    Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);

                    String fromGeometryWKT = getGeospatialScaleContentMap.get(this.getEntityUID());
                    Set<String> targetGeometryWKTs = new HashSet<>();
                    for(String currentEntityUID:getGeospatialScaleContentMap.keySet()){
                        if(!currentEntityUID.equals(this.getEntityUID())){
                            targetGeometryWKTs.add(getGeospatialScaleContentMap.get(currentEntityUID));
                        }
                    }
                    return GeospatialCalculateUtil.spatialPredicateWKTCalculate(fromGeometryWKT,spatialPredicateType,
                                targetGeometryWKTs);
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    default public GeospatialScaleFeatureSupportable.WKTGeometryType getEntityGeometryType(SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                List<String> entityUIDList = new ArrayList<>();
                entityUIDList.add(this.getEntityUID());
                Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);
                if(getGeospatialScaleContentMap.size() == 1){
                    return GeospatialCalculateUtil.getGeometryWKTType(getGeospatialScaleContentMap.get(this.getEntityUID()));
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public double getEntitiesSpatialDistance(String targetEntityUID, SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null && targetEntityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                boolean isTargetEntityContentValidate=checkGeospatialScaleContentExist(workingGraphOperationExecutor,spatialScaleLevel,targetEntityUID);
                if(!isTargetEntityContentValidate){
                    return Double.NaN;
                }else{
                    List<String> entityUIDList = new ArrayList<>();
                    entityUIDList.add(this.getEntityUID());
                    entityUIDList.add(targetEntityUID);
                    Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);
                    if(getGeospatialScaleContentMap.size() == 2){
                        return GeospatialCalculateUtil.getGeometriesDistance(getGeospatialScaleContentMap.get(this.getEntityUID()),getGeospatialScaleContentMap.get(targetEntityUID));
                    }
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return Double.NaN;
    }

    default public boolean isSpatialDistanceWithinEntity(String targetEntityUID, double distanceValue, SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null && targetEntityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                validateSpatialScaleLevel(workingGraphOperationExecutor,targetEntityUID,spatialScaleLevel);
                List<String> entityUIDList = new ArrayList<>();
                entityUIDList.add(this.getEntityUID());
                entityUIDList.add(targetEntityUID);
                Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);
                if(getGeospatialScaleContentMap.size() == 2){
                    return GeospatialCalculateUtil.isGeometriesInDistance(getGeospatialScaleContentMap.get(this.getEntityUID()),getGeospatialScaleContentMap.get(targetEntityUID),distanceValue);
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    default public boolean isSpatialDistanceWithinEntities(Set<String> targetEntityUIDsSet, double distanceValue, SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null && targetEntityUIDsSet != null && targetEntityUIDsSet.size()>0) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                List<String> entityUIDList = new ArrayList<>();
                entityUIDList.add(this.getEntityUID());
                entityUIDList.addAll(targetEntityUIDsSet);
                Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);

                String fromGeometryWKT = getGeospatialScaleContentMap.get(this.getEntityUID());
                Set<String> targetGeometryWKTs = new HashSet<>();
                for(String currentEntityUID:getGeospatialScaleContentMap.keySet()){
                    if(!currentEntityUID.equals(this.getEntityUID())){
                        targetGeometryWKTs.add(getGeospatialScaleContentMap.get(currentEntityUID));
                    }
                }
                return GeospatialCalculateUtil.isGeometriesInDistance(fromGeometryWKT,targetGeometryWKTs,distanceValue);
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    default public String getEntitySpatialBufferWKTGeometryContent(double bufferDistanceValue,SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                List<String> entityUIDList = new ArrayList<>();
                entityUIDList.add(this.getEntityUID());
                Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);
                if(getGeospatialScaleContentMap.size() == 1){
                    return GeospatialCalculateUtil.getGeometryBufferWKTContent(getGeospatialScaleContentMap.get(this.getEntityUID()),bufferDistanceValue);
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public String getEntitySpatialEnvelopeWKTGeometryContent(SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                List<String> entityUIDList = new ArrayList<>();
                entityUIDList.add(this.getEntityUID());
                Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);
                if(getGeospatialScaleContentMap.size() == 1){
                    return GeospatialCalculateUtil.getGeometryEnvelopeWKTContent(getGeospatialScaleContentMap.get(this.getEntityUID()));
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public String getEntitySpatialCentroidPointWKTGeometryContent(SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                List<String> entityUIDList = new ArrayList<>();
                entityUIDList.add(this.getEntityUID());
                Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);
                if(getGeospatialScaleContentMap.size() == 1){
                    return GeospatialCalculateUtil.getGeometryCentroidPointWKTContent(getGeospatialScaleContentMap.get(this.getEntityUID()));
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public String getEntitySpatialInteriorPointWKTGeometryContent(SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                validateSpatialScaleLevel(workingGraphOperationExecutor,spatialScaleLevel);
                List<String> entityUIDList = new ArrayList<>();
                entityUIDList.add(this.getEntityUID());
                Map<String,String> getGeospatialScaleContentMap = getGeospatialScaleContent(workingGraphOperationExecutor,spatialScaleLevel,entityUIDList);
                if(getGeospatialScaleContentMap.size() == 1){
                    return GeospatialCalculateUtil.getGeometryInteriorPointWKTContent(getGeospatialScaleContentMap.get(this.getEntityUID()));
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    private void validateSpatialScaleLevel(GraphOperationExecutor workingGraphOperationExecutor,SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(!checkGeospatialScaleContentExist(workingGraphOperationExecutor,spatialScaleLevel,this.getEntityUID())){
            logger.error("Entity with UID {} doesn't have {} level SpatialScale.", this.getEntityUID(),spatialScaleLevel);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("Entity with UID "+this.getEntityUID()+" doesn't have "+spatialScaleLevel+" level SpatialScale.");
            throw exception;
        }
    }

    private void validateSpatialScaleLevel(GraphOperationExecutor workingGraphOperationExecutor,String entityUID,SpatialScaleLevel spatialScaleLevel) throws EngineServiceRuntimeException {
        if(!checkGeospatialScaleContentExist(workingGraphOperationExecutor,spatialScaleLevel,entityUID)){
            logger.error("Entity with UID {} doesn't have {} level SpatialScale.", entityUID,spatialScaleLevel);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("Entity with UID "+entityUID+" doesn't have "+spatialScaleLevel+" level SpatialScale.");
            throw exception;
        }
    }

    private boolean checkGeospatialScaleContentExist(GraphOperationExecutor workingGraphOperationExecutor,SpatialScaleLevel spatialScaleLevel,String entityUID){
        String spatialScalePropertyName = null;
        switch(spatialScaleLevel){
            case Local: spatialScalePropertyName = Constant._GeospatialLLGeometryContent;break;
            case Global: spatialScalePropertyName = Constant._GeospatialGLGeometryContent;break;
            case Country: spatialScalePropertyName = Constant._GeospatialCLGeometryContent;break;
        }
        if(spatialScalePropertyName != null){
            String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,
                    Long.parseLong(entityUID), CypherBuilder.CypherFunctionType.EXISTS, spatialScalePropertyName);
            GetBooleanFormatReturnValueTransformer getBooleanFormatReturnValueTransformer = new GetBooleanFormatReturnValueTransformer();
            Object resultRes = workingGraphOperationExecutor.executeRead(getBooleanFormatReturnValueTransformer,queryCql);
            return resultRes != null ? (Boolean)resultRes : false;
        }
        return false;
    }

    private Map<String,String> getGeospatialScaleContent(GraphOperationExecutor workingGraphOperationExecutor, SpatialScaleLevel spatialScaleLevel, List<String> entityUIDs){
        String spatialScalePropertyName = getGeospatialScaleContentAttributeName(spatialScaleLevel);
        List<String> attributeNames = new ArrayList<>();
        attributeNames.add(spatialScalePropertyName);

        try {
            String cypherProcedureString = CypherBuilder.matchAttributesWithNodeIDs(entityUIDs,attributeNames);

            GetListEntityValueTransformer getListEntityValueTransformer = new GetListEntityValueTransformer(attributeNames);
            getListEntityValueTransformer.setUseIDMatchLogic(true);
            Object resEntityRes = workingGraphOperationExecutor.executeRead(getListEntityValueTransformer,cypherProcedureString);
            if(resEntityRes != null){
                Map<String,String> geospatialScaleContentMap = new HashMap<>();
                List<EntityValue> resultEntitiesValues = (List<EntityValue>)resEntityRes;

                for(EntityValue currentEntityValue :resultEntitiesValues){
                    String entityUID = currentEntityValue.getEntityUID();
                    String geospatialScaleContent = currentEntityValue.getEntityAttributesValue().get(spatialScalePropertyName).toString();
                    geospatialScaleContentMap.put(entityUID,geospatialScaleContent);
                }
                return geospatialScaleContentMap;
            }
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getGeospatialScaleContentAttributeName(SpatialScaleLevel spatialScaleLevel){
        String spatialScalePropertyName = null;
        switch(spatialScaleLevel){
            case Local: spatialScalePropertyName = Constant._GeospatialLLGeometryContent;break;
            case Global: spatialScalePropertyName = Constant._GeospatialGLGeometryContent;break;
            case Country: spatialScalePropertyName = Constant._GeospatialCLGeometryContent;break;
        }
        return spatialScalePropertyName;
    }

    private Map<String,String> getEntitiesGeospatialScaleContentMap(GraphOperationExecutor workingGraphOperationExecutor,
                 String targetConceptionKind,AttributesParameters attributesParameters,SpatialScaleLevel spatialScaleLevel) throws EngineServiceEntityExploreException {
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setDistinctMode(true);
        queryParameters.setResultNumber(100000000);
        if (attributesParameters != null) {
            queryParameters.setDefaultFilteringItem(attributesParameters.getDefaultFilteringItem());
            if (attributesParameters.getAndFilteringItemsList() != null) {
                for (FilteringItem currentFilteringItem : attributesParameters.getAndFilteringItemsList()) {
                    queryParameters.addFilteringItem(currentFilteringItem, QueryParameters.FilteringLogic.AND);
                }
            }
            if (attributesParameters.getOrFilteringItemsList() != null) {
                for (FilteringItem currentFilteringItem : attributesParameters.getOrFilteringItemsList()) {
                    queryParameters.addFilteringItem(currentFilteringItem, QueryParameters.FilteringLogic.OR);
                }
            }
        }

        List<String> attributeNames = new ArrayList<>();
        String spatialScalePropertyName = getGeospatialScaleContentAttributeName(spatialScaleLevel);
        attributeNames.add(spatialScalePropertyName);

        String queryCql = CypherBuilder.matchAttributesWithQueryParameters(targetConceptionKind,queryParameters,attributeNames);

        Map<String,String> entitiesSpatialContentDataMap = new HashMap<>();
        DataTransformer spatialScalePropertyHandelTransformer = new DataTransformer(){
            @Override
            public Object transformResult(Result result) {
                while(result.hasNext()){
                    Record nodeRecord = result.next();
                    long nodeUID = nodeRecord.get("id("+CypherBuilder.operationResultName+")").asInt();
                    String conceptionEntityUID = ""+nodeUID;
                    String spatialScalePropertyValue = null;
                    if(nodeRecord.containsKey("operationResult."+spatialScalePropertyName)){
                        spatialScalePropertyValue = nodeRecord.get("operationResult."+spatialScalePropertyName).asString();
                    }
                    if(spatialScalePropertyValue != null){
                        entitiesSpatialContentDataMap.put(conceptionEntityUID,spatialScalePropertyValue);
                    }
                }
                return null;
            }
        };
        workingGraphOperationExecutor.executeRead(spatialScalePropertyHandelTransformer, queryCql);

        return entitiesSpatialContentDataMap;
    }

    private List<Entity> getConceptionEntitiesByUIDs(GraphOperationExecutor workingGraphOperationExecutor, Set<String> matchedEntityUIDSet){

        if(matchedEntityUIDSet!= null){
            String cypherProcedureString = "MATCH (targetNodes) WHERE id(targetNodes) IN " + matchedEntityUIDSet.toString()+"\n"+
                    "RETURN DISTINCT targetNodes as operationResult";
            logger.debug("Generated Cypher Statement: {}", cypherProcedureString);
            GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(null,
                    getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
            Object conceptionEntityList = workingGraphOperationExecutor.executeRead(getListEntityTransformer,cypherProcedureString);
            return conceptionEntityList != null ? (List<Entity>)conceptionEntityList : null;
        }
        return null;
    }
}
