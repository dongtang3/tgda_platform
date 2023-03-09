package com.github.tgda.engine.core.feature.spi.neo4j.featureInf;

import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.EqualFilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.payload.GeospatialScaleDataPair;
import com.github.tgda.engine.core.term.GeospatialScaleEvent;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.feature.GeospatialScaleFeatureSupportable;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JGeospatialScaleEntityImpl;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JGeospatialScaleEventImpl;
import com.github.tgda.engine.core.util.Constant;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public interface Neo4JGeospatialScaleFeatureSupportable extends GeospatialScaleFeatureSupportable,Neo4JKeyResourcesRetrievable{

    static Logger logger = LoggerFactory.getLogger(Neo4JGeospatialScaleFeatureSupportable.class);

    public default WKTGeometryType getGeometryType() {
        String returnDataValue = getAttributeValue(Constant._GeospatialGeometryType);
        if(returnDataValue != null){
            if(returnDataValue.equals(""+ WKTGeometryType.POINT)){
                return WKTGeometryType.POINT;
            }else if(returnDataValue.equals(""+ WKTGeometryType.LINESTRING)){
                return WKTGeometryType.LINESTRING;
            }else if(returnDataValue.equals(""+ WKTGeometryType.POLYGON)){
                return WKTGeometryType.POLYGON;
            }else if(returnDataValue.equals(""+ WKTGeometryType.MULTIPOINT)){
                return WKTGeometryType.MULTIPOINT;
            }else if(returnDataValue.equals(""+ WKTGeometryType.MULTILINESTRING)){
                return WKTGeometryType.MULTILINESTRING;
            }else if(returnDataValue.equals(""+ WKTGeometryType.MULTIPOLYGON)){
                return WKTGeometryType.MULTIPOLYGON;
            }else if(returnDataValue.equals(""+ WKTGeometryType.GEOMETRYCOLLECTION)){
                return WKTGeometryType.GEOMETRYCOLLECTION;
            }
        }
        return null;
    }

    public default boolean addOrUpdateGeometryType(WKTGeometryType wKTGeometryType){
        return addOrUpdateAttributeValue(Constant._GeospatialGeometryType,""+wKTGeometryType);
    }

    public default String getGlobalCRSAID(){
        return getAttributeValue(Constant._GeospatialGlobalCRSAID);
    }

    public default boolean addOrUpdateGlobalCRSAID(String crsAID){
        return addOrUpdateAttributeValue(Constant._GeospatialGlobalCRSAID,crsAID);
    }

    public default String getCountryCRSAID(){
        return getAttributeValue(Constant._GeospatialCountryCRSAID);
    }

    public default boolean addOrUpdateCountryCRSAID(String crsAID){
        return addOrUpdateAttributeValue(Constant._GeospatialCountryCRSAID,crsAID);
    }

    public default String getLocalCRSAID(){
        return getAttributeValue(Constant._GeospatialLocalCRSAID);
    }

    public default boolean addOrUpdateLocalCRSAID(String crsAID){
        return addOrUpdateAttributeValue(Constant._GeospatialLocalCRSAID,crsAID);
    }

    public default String getGLGeometryContent(){
        return getAttributeValue(Constant._GeospatialGLGeometryContent);
    }

    public default boolean addOrUpdateGLGeometryContent(String wKTContent){
        return addOrUpdateAttributeValue(Constant._GeospatialGLGeometryContent,wKTContent);
    }

    public default String getCLGeometryContent(){
        return getAttributeValue(Constant._GeospatialCLGeometryContent);
    }

    public default boolean addOrUpdateCLGeometryContent(String wKTContent){
        return addOrUpdateAttributeValue(Constant._GeospatialCLGeometryContent,wKTContent);
    }

    public default String getLLGeometryContent(){
        return getAttributeValue(Constant._GeospatialLLGeometryContent);
    }

    public default boolean addOrUpdateLLGeometryContent(String wKTContent){
        return addOrUpdateAttributeValue(Constant._GeospatialLLGeometryContent,wKTContent);
    }

    public default GeospatialScaleEvent attachGeospatialScaleEvent(String geospatialCode, String eventComment,
                                                                   Map<String, Object> eventData) throws EngineServiceRuntimeException {
        return attachGeospatialScaleEventInnerLogic(Constant._defaultGeospatialName,geospatialCode,eventComment,eventData);
    }

    public default GeospatialScaleEvent attachGeospatialScaleEvent(String geospatialRegionName, String geospatialCode,
                                                                   String eventComment, Map<String, Object> eventData) throws EngineServiceRuntimeException {
        return attachGeospatialScaleEventInnerLogic(geospatialRegionName,geospatialCode,eventComment,eventData);
    }

    public default boolean detachGeospatialScaleEvent(String geospatialScaleEventUID) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(geospatialScaleEventUID), null, null);
                GetSingleEntityTransformer getSingleEntityTransformer =
                        new GetSingleEntityTransformer(Constant.GeospatialScaleEventClass, getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                Object resEntityRes = workingGraphOperationExecutor.executeRead(getSingleEntityTransformer, queryCql);
                if(resEntityRes == null){
                    logger.error("GeospatialScaleEvent does not contains entity with UID {}.", geospatialScaleEventUID);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("GeospatialScaleEvent does not contains entity with UID " + geospatialScaleEventUID + ".");
                    throw exception;
                }else{
                    Neo4JGeospatialScaleEventImpl neo4JGeospatialScaleEventImpl = new Neo4JGeospatialScaleEventImpl(null,null,null,null,geospatialScaleEventUID);
                    neo4JGeospatialScaleEventImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                    if(neo4JGeospatialScaleEventImpl.getAttachEntity().getEntityUID().equals(this.getEntityUID())){
                        String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.valueOf(geospatialScaleEventUID),null,null);
                        Object deletedEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer, deleteCql);
                        if(deletedEntityRes == null){
                            throw new EngineServiceRuntimeException();
                        }else{
                            return true;
                        }
                    }else{
                        logger.error("GeospatialScaleEvent with entity UID {} doesn't attached to current Entity with UID {}.", geospatialScaleEventUID,this.getEntityUID());
                        EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                        exception.setCauseMessage("GeospatialScaleEvent with entity UID " + geospatialScaleEventUID + " doesn't attached to current Entity with UID "+ this.getEntityUID()+ ".");
                        throw exception;
                    }
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    public default List<GeospatialScaleEvent> getAttachedGeospatialScaleEvents(){
        if(this.getEntityUID() != null) {
            String queryCql = "MATCH(currentEntity)-[:`" + Constant.GeospatialScale_AttachToRelationClass + "`]->(geospatialScaleEvents:TGDA_GeospatialScaleEvent) WHERE id(currentEntity) = " + this.getEntityUID() + " \n" +
                    "RETURN geospatialScaleEvents as operationResult";
            logger.debug("Generated Cypher Statement: {}", queryCql);

            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                GetListGeospatialScaleEventTransformer getListGeospatialScaleEventTransformer = new GetListGeospatialScaleEventTransformer(null,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                Object queryRes = workingGraphOperationExecutor.executeRead(getListGeospatialScaleEventTransformer,queryCql);
                if(queryRes != null){
                    List<GeospatialScaleEvent> res = (List<GeospatialScaleEvent>)queryRes;
                    return res;
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return new ArrayList<>();
    }

    public default List<GeospatialScaleEntity> getAttachedGeospatialScaleEntities(){
        if(this.getEntityUID() != null) {
            String queryCql = "MATCH(currentEntity)-[:`" + Constant.GeospatialScale_AttachToRelationClass + "`]->(geospatialScaleEvents:TGDA_GeospatialScaleEvent)<-[:`"+ Constant.GeospatialScale_GeospatialReferToRelationClass+"`]-(geospatialScaleEntities:`TGDA_GeospatialScaleEntity`) WHERE id(currentEntity) = " + this.getEntityUID() + " \n" +
                    "RETURN geospatialScaleEntities as operationResult";
            logger.debug("Generated Cypher Statement: {}", queryCql);

            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                GetListGeospatialScaleEntityTransformer getListGeospatialScaleEntityTransformer =
                        new GetListGeospatialScaleEntityTransformer(null,null,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                Object queryRes = workingGraphOperationExecutor.executeRead(getListGeospatialScaleEntityTransformer,queryCql);
                if(queryRes != null){
                    return (List<GeospatialScaleEntity>)queryRes;
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return new ArrayList<>();
    }

    public default List<GeospatialScaleDataPair> getAttachedGeospatialScaleDataPairs(){
        List<GeospatialScaleDataPair> geospatialScaleDataPairList = new ArrayList<>();
        if(this.getEntityUID() != null) {
            String queryCql = "MATCH(currentEntity)-[:`" + Constant.GeospatialScale_AttachToRelationClass + "`]->(geospatialScaleEvents:TGDA_GeospatialScaleEvent)<-[:`"+ Constant.GeospatialScale_GeospatialReferToRelationClass+"`]-(geospatialScaleEntities:`TGDA_GeospatialScaleEntity`) WHERE id(currentEntity) = " + this.getEntityUID() + " \n" +
                    "RETURN geospatialScaleEntities ,geospatialScaleEvents";
            logger.debug("Generated Cypher Statement: {}", queryCql);

            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                DataTransformer<Object> _DataTransformer = new DataTransformer<Object>() {
                    @Override
                    public Object transformResult(Result result) {
                        while(result.hasNext()) {
                            Record record = result.next();

                            Neo4JGeospatialScaleEntityImpl neo4JGeospatialScaleEntityImpl = null;
                            Neo4JGeospatialScaleEventImpl neo4JGeospatialScaleEventImpl = null;

                            Node geospatialScaleEntityNode = record.get("geospatialScaleEntities").asNode();
                            List<String> allLabelNames = Lists.newArrayList(geospatialScaleEntityNode.labels());
                            boolean isMatchedEntity = true;
                            if (allLabelNames.size() > 0) {
                                isMatchedEntity = allLabelNames.contains(Constant.GeospatialScaleEntityClass);
                            }
                            if (isMatchedEntity) {
                                long nodeUID = geospatialScaleEntityNode.id();
                                String conceptionEntityUID = ""+nodeUID;
                                String targetGeospatialCode = geospatialScaleEntityNode.get(Constant.GeospatialCodeProperty).asString();
                                String targetGeospatialScaleGradeString = geospatialScaleEntityNode.get(Constant.GeospatialScaleGradeProperty).asString();
                                String currentGeospatialName = geospatialScaleEntityNode.get(Constant.GeospatialProperty).asString();
                                String _ChineseName = null;
                                String _EnglishName = null;
                                if(geospatialScaleEntityNode.containsKey(Constant.GeospatialChineseNameProperty)){
                                    _ChineseName = geospatialScaleEntityNode.get(Constant.GeospatialChineseNameProperty).asString();
                                }
                                if(geospatialScaleEntityNode.containsKey(Constant.GeospatialEnglishNameProperty)){
                                    _EnglishName = geospatialScaleEntityNode.get(Constant.GeospatialEnglishNameProperty).asString();
                                }
                                Geospatial.GeospatialScaleGrade geospatialScaleGrade = getGeospatialScaleGrade(targetGeospatialScaleGradeString);
                                neo4JGeospatialScaleEntityImpl =
                                        new Neo4JGeospatialScaleEntityImpl(null,currentGeospatialName,conceptionEntityUID,geospatialScaleGrade,targetGeospatialCode,_ChineseName,_EnglishName);
                                neo4JGeospatialScaleEntityImpl.setGlobalGraphOperationExecutor(getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                            }

                            Node geospatialScaleEventNode = record.get("geospatialScaleEvents").asNode();
                            List<String> allConceptionKindNames = Lists.newArrayList(geospatialScaleEventNode.labels());
                            boolean isMatchedConceptionKind = false;
                            if(allConceptionKindNames.size()>0){
                                isMatchedConceptionKind = allConceptionKindNames.contains(Constant.GeospatialScaleEventClass);
                            }
                            if(isMatchedConceptionKind) {
                                long nodeUID = geospatialScaleEventNode.id();
                                String geospatialScaleEventUID = "" + nodeUID;
                                String eventComment = geospatialScaleEventNode.get(Constant._GeospatialScaleEventComment).asString();
                                String geospatialScaleGrade = geospatialScaleEventNode.get(Constant._GeospatialScaleEventScaleGrade).asString();
                                String referLocation = geospatialScaleEventNode.get(Constant._GeospatialScaleEventReferLocation).asString();
                                String geospatialRegion = geospatialScaleEventNode.get(Constant._GeospatialScaleEventGeospatial).asString();

                                neo4JGeospatialScaleEventImpl = new Neo4JGeospatialScaleEventImpl(geospatialRegion, eventComment, referLocation, getGeospatialScaleGrade(geospatialScaleGrade.trim()), geospatialScaleEventUID);
                                neo4JGeospatialScaleEventImpl.setGlobalGraphOperationExecutor(getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                            }
                            if(neo4JGeospatialScaleEntityImpl != null && neo4JGeospatialScaleEventImpl != null){
                                geospatialScaleDataPairList.add(
                                        new GeospatialScaleDataPair(neo4JGeospatialScaleEventImpl,neo4JGeospatialScaleEntityImpl)
                                );
                            }
                        }
                        return null;
                    }
                };
                workingGraphOperationExecutor.executeRead(_DataTransformer,queryCql);
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return geospatialScaleDataPairList;
    }

    private GeospatialScaleEvent attachGeospatialScaleEventInnerLogic(String geospatialRegionName,String geospatialCode,
                                                                      String eventComment, Map<String, Object> eventData) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                QueryParameters queryParameters = new QueryParameters();
                queryParameters.setResultNumber(1);
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName));
                queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialCodeProperty,geospatialCode), QueryParameters.FilteringLogic.AND);
                queryParameters.setDistinctMode(true);
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleEntityClass,queryParameters,null);

                GetSingleEntityTransformer getSingleEntityTransformer =
                        new GetSingleEntityTransformer(Constant.GeospatialScaleEntityClass, workingGraphOperationExecutor);
                Object targetGeospatialScaleEntityRes = workingGraphOperationExecutor.executeRead(getSingleEntityTransformer, queryCql);
                if(targetGeospatialScaleEntityRes == null){
                    logger.error("GeospatialScaleEntity with geospatialCode {} doesn't exist.", geospatialCode);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("GeospatialScaleEntity with geospatialCode "+geospatialCode+" doesn't exist.");
                    throw exception;
                }
                Entity targetGeospatialScaleEntity = (Entity)targetGeospatialScaleEntityRes;
                String eventGeospatialScaleGrade = targetGeospatialScaleEntity.getAttribute(Constant.GeospatialScaleGradeProperty).
                        getAttributeValue().toString();

                Map<String, Object> propertiesMap = eventData != null ? eventData : new HashMap<>();
                CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
                propertiesMap.put(Constant._GeospatialScaleEventReferLocation,geospatialCode);
                propertiesMap.put(Constant._GeospatialScaleEventComment,eventComment);
                propertiesMap.put(Constant._GeospatialScaleEventScaleGrade,eventGeospatialScaleGrade);
                propertiesMap.put(Constant._GeospatialScaleEventGeospatial,geospatialRegionName);
                String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.GeospatialScaleEventClass}, propertiesMap);
                logger.debug("Generated Cypher Statement: {}", createCql);
                getSingleEntityTransformer =
                        new GetSingleEntityTransformer(Constant.GeospatialScaleEventClass, workingGraphOperationExecutor);
                Object newEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer, createCql);
                if(newEntityRes != null) {
                    Entity geospatialScaleEventEntity = (Entity) newEntityRes;
                    geospatialScaleEventEntity.attachToRelation(this.getEntityUID(), Constant.GeospatialScale_AttachToRelationClass, null, true);
                    RelationshipEntity linkToGeospatialScaleEntityRelation = targetGeospatialScaleEntity.attachFromRelation(geospatialScaleEventEntity.getEntityUID(), Constant.GeospatialScale_GeospatialReferToRelationClass, null, true);
                    if(linkToGeospatialScaleEntityRelation != null){
                        Neo4JGeospatialScaleEventImpl neo4JGeospatialScaleEventImpl = new Neo4JGeospatialScaleEventImpl(geospatialRegionName,eventComment,geospatialCode,getGeospatialScaleGrade(eventGeospatialScaleGrade.trim()),geospatialScaleEventEntity.getEntityUID());
                        neo4JGeospatialScaleEventImpl.setGlobalGraphOperationExecutor(getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                        return neo4JGeospatialScaleEventImpl;
                    }
                }
            }catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    private Geospatial.GeospatialScaleGrade getGeospatialScaleGrade(String geospatialScaleGradeValue){
        if(geospatialScaleGradeValue.equals("CONTINENT")){
            return Geospatial.GeospatialScaleGrade.CONTINENT;
        }else if(geospatialScaleGradeValue.equals("COUNTRY_REGION")){
            return Geospatial.GeospatialScaleGrade.COUNTRY_REGION;
        }else if(geospatialScaleGradeValue.equals("PROVINCE")){
            return Geospatial.GeospatialScaleGrade.PROVINCE;
        }else if(geospatialScaleGradeValue.equals("PREFECTURE")){
            return Geospatial.GeospatialScaleGrade.PREFECTURE;
        }else if(geospatialScaleGradeValue.equals("COUNTY")){
            return Geospatial.GeospatialScaleGrade.COUNTY;
        }else if(geospatialScaleGradeValue.equals("TOWNSHIP")){
            return Geospatial.GeospatialScaleGrade.TOWNSHIP;
        }else if(geospatialScaleGradeValue.equals("VILLAGE")){
            return Geospatial.GeospatialScaleGrade.VILLAGE;
        }
        return null;
    }

    private String getAttributeValue(String attributeName){
        if (getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                String queryCql = CypherBuilder.matchNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(getEntityUID()),new String[]{attributeName});
                DataTransformer dataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        if(result.hasNext()){
                            Record returnRecord = result.next();
                            Map<String,Object> returnValueMap = returnRecord.asMap();
                            String attributeNameFullName= CypherBuilder.operationResultName+"."+ attributeName;
                            Object attributeValueObject = returnValueMap.get(attributeNameFullName);
                            if(attributeValueObject!= null){
                                return attributeValueObject;
                            }
                        }
                        return null;
                    }
                };
                Object resultRes = workingGraphOperationExecutor.executeRead(dataTransformer,queryCql);
                return resultRes !=null ? resultRes.toString() : null;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    private boolean addOrUpdateAttributeValue(String attributeName,Object attributeValue) {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                Map<String,Object> attributeDataMap = new HashMap<>();
                attributeDataMap.put(attributeName,attributeValue);
                String updateCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(getEntityUID()),attributeDataMap);
                DataTransformer updateItemDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        if(result.hasNext()){
                            Record returnRecord = result.next();
                            Map<String,Object> returnValueMap = returnRecord.asMap();
                            String attributeNameFullName= CypherBuilder.operationResultName+"."+ attributeName;
                            Object attributeValueObject = returnValueMap.get(attributeNameFullName);
                            if(attributeValueObject!= null){
                                return attributeValueObject;
                            }
                        }
                        return null;
                    }
                };
                Object resultRes = workingGraphOperationExecutor.executeWrite(updateItemDataTransformer,updateCql);
                return resultRes != null ? true: false;
            } finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }
}
