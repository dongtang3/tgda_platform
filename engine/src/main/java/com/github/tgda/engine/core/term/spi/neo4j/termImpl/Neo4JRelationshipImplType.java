package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.FilteringItem;
import com.github.tgda.engine.core.exception.EngineFunctionNotSupportedException;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.coreRealm.realmServiceCore.internal.neo4j.dataTransformer.*;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.coreRealm.realmServiceCore.payload.*;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonEntitiesOperationResultImpl;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonRelationEntitiesAttributesRetrieveResultImpl;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonRelationshipEntitiesRetrieveResultImpl;
import com.github.tgda.engine.core.structure.InheritanceTree;
import com.github.tgda.engine.core.term.Direction;
import com.github.tgda.engine.core.term.RelationshipEntity;
import com.github.tgda.engine.core.term.RelationshipType;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JRelationshipType;
import com.github.tgda.engine.core.util.Constant;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Neo4JRelationshipImplType implements Neo4JRelationshipType {

    private static Logger logger = LoggerFactory.getLogger(Neo4JRelationshipImplType.class);
    private String coreRealmName;
    private String relationKindName;
    private String relationKindDesc;
    private String relationKindUID;

    public Neo4JRelationshipImplType(String coreRealmName, String relationKindName, String relationKindDesc, String relationKindUID){
        this.coreRealmName = coreRealmName;
        this.relationKindName = relationKindName;
        this.relationKindDesc = relationKindDesc;
        this.relationKindUID = relationKindUID;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    public String getRelationKindUID(){
        return this.relationKindUID;
    }

    @Override
    public String getRelationKindName() {
        return this.relationKindName;
    }

    @Override
    public String getRelationKindDesc() {
        return this.relationKindDesc;
    }

    @Override
    public boolean updateRelationKindDesc(String kindDesc) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            Map<String,Object> attributeDataMap = new HashMap<>();
            attributeDataMap.put(Constant._DescProperty, kindDesc);
            String updateCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.relationKindUID),attributeDataMap);
            GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._DescProperty);
            Object updateResultRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeValueTransformer,updateCql);
            CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.relationKindUID,false);
            AttributeValue resultAttributeValue =  updateResultRes != null ? (AttributeValue) updateResultRes : null;
            if(resultAttributeValue != null && resultAttributeValue.getAttributeValue().toString().equals(kindDesc)){
                this.relationKindDesc = kindDesc;
                return true;
            }else{
                return false;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public RelationshipType getParentRelationKind() throws EngineFunctionNotSupportedException {
        EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
        exception.setCauseMessage("Neo4J storage implements doesn't support this function");
        throw exception;
    }

    @Override
    public List<RelationshipType> getChildRelationKinds() throws EngineFunctionNotSupportedException {
        EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
        exception.setCauseMessage("Neo4J storage implements doesn't support this function");
        throw exception;
    }

    @Override
    public InheritanceTree<RelationshipType> getOffspringRelationKinds() throws EngineFunctionNotSupportedException {
        EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
        exception.setCauseMessage("Neo4J storage implements doesn't support this function");
        throw exception;
    }

    @Override
    public Long countRelationEntities() throws EngineServiceRuntimeException {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            String queryCql = CypherBuilder.matchRelationWithSinglePropertyValueAndFunction(getRelationKindName(), CypherBuilder.CypherFunctionType.COUNT, null, null);
            GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer("count");
            Object countRelationEntitiesRes = workingGraphOperationExecutor.executeWrite(getLongFormatAggregatedReturnValueTransformer, queryCql);
            if (countRelationEntitiesRes == null) {
                throw new EngineServiceRuntimeException();
            } else {
                return (Long) countRelationEntitiesRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Long countRelationEntitiesWithOffspring() throws EngineFunctionNotSupportedException {
        EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
        exception.setCauseMessage("Neo4J storage implements doesn't support this function");
        throw exception;
    }

    @Override
    public Long countRelationEntities(AttributesParameters attributesParameters, boolean isDistinctMode) throws EngineServiceEntityExploreException, EngineServiceRuntimeException {
        if (attributesParameters != null) {
            QueryParameters queryParameters = new QueryParameters();
            queryParameters.setDistinctMode(isDistinctMode);
            queryParameters.setResultNumber(100000000);
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
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                queryParameters.setEntityKind(this.relationKindName);
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,
                        null,null,false,queryParameters, CypherBuilder.CypherFunctionType.COUNT);

                GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer =
                        queryParameters.isDistinctMode() ?
                        new GetLongFormatAggregatedReturnValueTransformer("count","DISTINCT"):
                                new GetLongFormatAggregatedReturnValueTransformer("count");
                Object queryRes = workingGraphOperationExecutor.executeRead(getLongFormatAggregatedReturnValueTransformer,queryCql);
                if (queryRes != null) {
                    return (Long) queryRes;
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
            return null;
        }else{
            return countRelationEntities();
        }
    }

    @Override
    public RelationEntitiesRetrieveResult getRelationEntities(QueryParameters queryParameters) throws EngineServiceEntityExploreException {
        if (queryParameters != null) {
            CommonRelationshipEntitiesRetrieveResultImpl commonRelationEntitiesRetrieveResultImpl = new CommonRelationshipEntitiesRetrieveResultImpl();
            commonRelationEntitiesRetrieveResultImpl.getOperationStatistics().setQueryParameters(queryParameters);
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                queryParameters.setEntityKind(this.relationKindName);
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,
                        null,null,false,queryParameters,null);
                GetListRelationshipEntityTransformer getListRelationshipEntityTransformer =
                        new GetListRelationshipEntityTransformer(this.relationKindName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor(),queryParameters.isDistinctMode());
                Object queryRes = workingGraphOperationExecutor.executeRead(getListRelationshipEntityTransformer,queryCql);
                if(queryRes != null){
                    List<RelationshipEntity> resultEntityList = (List<RelationshipEntity>)queryRes;
                    commonRelationEntitiesRetrieveResultImpl.addRelationEntities(resultEntityList);
                    commonRelationEntitiesRetrieveResultImpl.getOperationStatistics().setResultEntitiesCount(resultEntityList.size());
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
            commonRelationEntitiesRetrieveResultImpl.finishEntitiesRetrieving();
            return commonRelationEntitiesRetrieveResultImpl;
        }
        return null;
    }

    @Override
    public EntitiesOperationResult purgeAllRelationEntities() throws EngineServiceRuntimeException {
        CommonEntitiesOperationResultImpl commonEntitiesOperationResultImpl = new CommonEntitiesOperationResultImpl();
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String deleteCql = CypherBuilder.deleteRelationTypeWithSinglePropertyValueAndFunction(this.relationKindName,
                    CypherBuilder.CypherFunctionType.COUNT,null,null);
            GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer =
                    new GetLongFormatAggregatedReturnValueTransformer("count","DISTINCT");
            Object deleteResultObject = workingGraphOperationExecutor.executeWrite(getLongFormatAggregatedReturnValueTransformer,deleteCql);
            if(deleteResultObject == null){
                throw new EngineServiceRuntimeException();
            }else{
                commonEntitiesOperationResultImpl.getOperationStatistics().setSuccessItemsCount((Long)deleteResultObject);
                commonEntitiesOperationResultImpl.getOperationStatistics().
                        setOperationSummary("purgeAllRelationEntities operation for relationKind "+this.relationKindName+" success.");
            }
            commonEntitiesOperationResultImpl.finishEntitiesOperation();
            return commonEntitiesOperationResultImpl;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public RelationEntitiesAttributesRetrieveResult getEntityAttributesByAttributeNames(List<String> attributeNames, QueryParameters queryParameters) throws EngineServiceEntityExploreException {
        if(attributeNames != null && attributeNames.size()>0){
            CommonRelationEntitiesAttributesRetrieveResultImpl commonRelationEntitiesAttributesRetrieveResultImpl =
                    new CommonRelationEntitiesAttributesRetrieveResultImpl();
            commonRelationEntitiesAttributesRetrieveResultImpl.getOperationStatistics().setQueryParameters(queryParameters);
            if(queryParameters == null){
                queryParameters = new QueryParameters();
            }

            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                queryParameters.setEntityKind(this.relationKindName);
                queryParameters.setDistinctMode(true);
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,
                        null,null,false,queryParameters,null);
                GetListRelationshipEntityValueTransformer getListRelationshipEntityValueTransformer =
                        new GetListRelationshipEntityValueTransformer(this.relationKindName,attributeNames);
                Object queryRes = workingGraphOperationExecutor.executeRead(getListRelationshipEntityValueTransformer,queryCql);
                if(queryRes != null){
                    List<RelationshipEntityValue> resultEntitiesValues = (List<RelationshipEntityValue>)queryRes;
                    commonRelationEntitiesAttributesRetrieveResultImpl.addRelationEntitiesAttributes(resultEntitiesValues);
                    commonRelationEntitiesAttributesRetrieveResultImpl.getOperationStatistics().setResultEntitiesCount(resultEntitiesValues.size());
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
            commonRelationEntitiesAttributesRetrieveResultImpl.finishEntitiesRetrieving();
            return commonRelationEntitiesAttributesRetrieveResultImpl;
        }
        return null;
    }

    @Override
    public RelationshipEntity getEntityByUID(String relationEntityUID) {
        if (relationEntityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchRelationWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(relationEntityUID), null, null);
                GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (this.relationKindName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object resEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer, queryCql);
                return resEntityRes != null ? (RelationshipEntity) resEntityRes : null;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public RelationDegreeDistributionInfo computeRelationDegreeDistribution(Direction direction) {
        String relationKindNameAndDirection = this.relationKindName;
        if(direction != null){
            switch(direction){
                case FROM: relationKindNameAndDirection = this.relationKindName+">";
                    break;
                case TO: relationKindNameAndDirection = "<"+this.relationKindName;
                    break;
                case TWO_WAY: relationKindNameAndDirection = this.relationKindName;
            }
        }

        String cypherProcedureString = "CALL apoc.stats.degrees(\""+relationKindNameAndDirection+"\");";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        DataTransformer<RelationDegreeDistributionInfo> entityRelationDegreeDataTransformer = new DataTransformer() {
            @Override
            public RelationDegreeDistributionInfo transformResult(Result result) {
                if(result.hasNext()){
                    Record nodeRecord = result.next();
                    String type = ""+nodeRecord.get("type").asString();
                    long total = nodeRecord.get("total").asLong();
                    long p50 = nodeRecord.get("p50").asLong();
                    long p75 = nodeRecord.get("p75").asLong();
                    long p90 = nodeRecord.get("p90").asLong();
                    long p95 = nodeRecord.get("p95").asLong();
                    long p99 = nodeRecord.get("p99").asLong();
                    long p999 = nodeRecord.get("p999").asLong();
                    long max = nodeRecord.get("max").asLong();
                    long min = nodeRecord.get("min").asLong();
                    float mean = nodeRecord.get("mean").asNumber().floatValue();

                    RelationDegreeDistributionInfo relationDegreeDistributionInfo = new RelationDegreeDistributionInfo(
                            type, direction,total,p50,p75,p90,p95,p99,p999,max,min,mean
                    );
                    return relationDegreeDistributionInfo;
                }
                return null;
            }
        };

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            Object resEntityRes = workingGraphOperationExecutor.executeRead(entityRelationDegreeDataTransformer, cypherProcedureString);
            return resEntityRes != null ? (RelationDegreeDistributionInfo) resEntityRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Set<RelationshipEntity> getRandomEntities(int entitiesCount) throws EngineServiceEntityExploreException {
        if(entitiesCount < 1){
            logger.error("entitiesCount must equal or great then 1.");
            EngineServiceEntityExploreException exception = new EngineServiceEntityExploreException();
            exception.setCauseMessage("entitiesCount must equal or great then 1.");
            throw exception;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = "MATCH p=()-[r:"+this.relationKindName+"]->() RETURN apoc.coll.randomItems(COLLECT(r),"+entitiesCount+") AS "+CypherBuilder.operationResultName;
            logger.debug("Generated Cypher Statement: {}", queryCql);
            RandomItemsRelationshipEntitySetDataTransformer randomItemsRelationshipEntitySetDataTransformer = new RandomItemsRelationshipEntitySetDataTransformer(workingGraphOperationExecutor);
            Object queryRes = workingGraphOperationExecutor.executeRead(randomItemsRelationshipEntitySetDataTransformer,queryCql);
            if(queryRes != null){
                Set<RelationshipEntity> resultRelationshipEntityList = (Set<RelationshipEntity>)queryRes;
                return resultRelationshipEntityList;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public Set<RelationshipEntity> getRandomEntities(AttributesParameters attributesParameters, boolean isDistinctMode, int entitiesCount) throws EngineServiceEntityExploreException, EngineServiceRuntimeException {
        if(entitiesCount < 1){
            logger.error("entitiesCount must equal or great then 1.");
            EngineServiceEntityExploreException exception = new EngineServiceEntityExploreException();
            exception.setCauseMessage("entitiesCount must equal or great then 1.");
            throw exception;
        }
        if (attributesParameters != null) {
            QueryParameters queryParameters = new QueryParameters();
            queryParameters.setDistinctMode(isDistinctMode);
            queryParameters.setResultNumber(100000000);
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
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                queryParameters.setEntityKind(this.relationKindName);
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,
                        null,null,true,queryParameters, CypherBuilder.CypherFunctionType.COUNT);
                String replaceContent = isDistinctMode ? "RETURN count(DISTINCT "+CypherBuilder.operationResultName+") SKIP 0 LIMIT 100000000" :
                        "RETURN count("+CypherBuilder.operationResultName+") SKIP 0 LIMIT 100000000";
                String newContent = isDistinctMode ? "RETURN apoc.coll.randomItems(COLLECT("+CypherBuilder.operationResultName+"),"+entitiesCount+",false) AS " +CypherBuilder.operationResultName:
                        "RETURN apoc.coll.randomItems(COLLECT("+CypherBuilder.operationResultName+"),"+entitiesCount+",true) AS " +CypherBuilder.operationResultName;
                queryCql = queryCql.replace(replaceContent,newContent);
                logger.debug("Generated Cypher Statement: {}", queryCql);
                RandomItemsRelationshipEntitySetDataTransformer randomItemsRelationshipEntitySetDataTransformer = new RandomItemsRelationshipEntitySetDataTransformer(workingGraphOperationExecutor);
                Object queryRes = workingGraphOperationExecutor.executeRead(randomItemsRelationshipEntitySetDataTransformer,queryCql);
                if(queryRes != null){
                    Set<RelationshipEntity> resultRelationshipEntityList = (Set<RelationshipEntity>)queryRes;
                    return resultRelationshipEntityList;
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
            return null;
        }else{
            return getRandomEntities(entitiesCount);
        }
    }

    @Override
    public long setKindScopeAttributes(Map<String, Object> attributes) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.setRelationKindProperties(this.relationKindName,attributes);
            GetLongFormatAggregatedReturnValueTransformer GetLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer("count");
            Object queryRes = workingGraphOperationExecutor.executeWrite(GetLongFormatAggregatedReturnValueTransformer,queryCql);
            if(queryRes != null) {
                Long operationResult =(Long)queryRes;
                return operationResult;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return 0;
    }

    @Override
    public long purgeRelationsOfSelfAttachedConceptionEntities() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = "MATCH p=(n1)-[r:"+this.relationKindName+"]->(n2) WHERE id(n1) = id(n2) delete r return count(r) AS " + CypherBuilder.operationResultName;
            logger.debug("Generated Cypher Statement: {}", queryCql);
            GetLongFormatAggregatedReturnValueTransformer GetLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer();
            Object queryRes = workingGraphOperationExecutor.executeWrite(GetLongFormatAggregatedReturnValueTransformer,queryCql);
            if(queryRes != null) {
                Long operationResult =(Long)queryRes;
                return operationResult;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return 0;
    }

    @Override
    public boolean deleteEntity(String relationEntityUID) throws EngineServiceRuntimeException {
        if(relationEntityUID != null){
            RelationshipEntity targetRelationshipEntity = this.getEntityByUID(relationEntityUID);
            if(targetRelationshipEntity != null){
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
                try{
                    String deleteCql = CypherBuilder.deleteRelationWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.valueOf(relationEntityUID),null,null);
                    GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer =
                            new GetSingleRelationshipEntityTransformer(this.relationKindName, this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                    Object deletedEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, deleteCql);
                    if(deletedEntityRes == null){
                        throw new EngineServiceRuntimeException();
                    }else{
                        return true;
                    }
                }finally {
                    this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
                }
            }else{
                logger.error("RelationKind {} does not contains entity with UID {}.", this.relationKindName, relationEntityUID);
                EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                exception.setCauseMessage("RelationKind " + this.relationKindName + " does not contains entity with UID " + relationEntityUID + ".");
                throw exception;
            }
        }
        return false;
    }

    @Override
    public EntitiesOperationResult deleteEntities(List<String> relationEntityUIDs) throws EngineServiceRuntimeException {
        if(relationEntityUIDs != null && relationEntityUIDs.size()>0){
            CommonEntitiesOperationResultImpl commonEntitiesOperationResultImpl = new CommonEntitiesOperationResultImpl();
            boolean countFail = false;
            for(String currentEntityUID:relationEntityUIDs) {
                RelationshipEntity targetRelationshipEntity = this.getEntityByUID(currentEntityUID);
                if(targetRelationshipEntity != null){
                    try {
                        boolean deleteCurrentEntityResult = deleteEntity(currentEntityUID);
                        if(deleteCurrentEntityResult){
                            commonEntitiesOperationResultImpl.getSuccessEntityUIDs().add(currentEntityUID);
                            commonEntitiesOperationResultImpl.getOperationStatistics().increaseSuccessCount();
                        }else{
                            commonEntitiesOperationResultImpl.getOperationStatistics().getFailItemsCount();
                        }
                    } catch (EngineServiceRuntimeException e) {
                        e.printStackTrace();
                        commonEntitiesOperationResultImpl.getOperationStatistics().getFailItemsCount();
                        logger.error("Exception occurred during delete entity with UID {} of RelationKind {}.", currentEntityUID , this.relationKindName);
                    }
                }else{
                    commonEntitiesOperationResultImpl.getOperationStatistics().increaseFailCount();
                    countFail = true;
                }
            }
            if(countFail){
                commonEntitiesOperationResultImpl.getOperationStatistics().
                        setOperationSummary("deleteEntities operation for relationKind "+this.relationKindName+" partial success.");
            }else{
                commonEntitiesOperationResultImpl.getOperationStatistics().
                        setOperationSummary("deleteEntities operation for relationKind "+this.relationKindName+" success.");
            }
            commonEntitiesOperationResultImpl.finishEntitiesOperation();
            return commonEntitiesOperationResultImpl;
        }
        return null;
    }

    private class RandomItemsRelationshipEntitySetDataTransformer implements DataTransformer<Set<RelationshipEntity>>{
        GraphOperationExecutor workingGraphOperationExecutor;
        public RandomItemsRelationshipEntitySetDataTransformer(GraphOperationExecutor workingGraphOperationExecutor){
            this.workingGraphOperationExecutor = workingGraphOperationExecutor;
        }
        @Override
        public Set<RelationshipEntity> transformResult(Result result) {
            Set<RelationshipEntity> relationshipEntitySet = new HashSet<>();
            if(result.hasNext()){
                List<Value> resultList = result.next().values();
                if(resultList.size() > 0){
                    List<Object> nodeObjList = resultList.get(0).asList();
                    for(Object currentNodeObj : nodeObjList){
                        org.neo4j.driver.types.Relationship resultRelationship = (org.neo4j.driver.types.Relationship)currentNodeObj;
                        boolean isMatchedRelationKind = relationKindName.equals(resultRelationship.type());
                        if(isMatchedRelationKind){
                            long relationUID = resultRelationship.id();
                            String relationEntityUID = ""+relationUID;
                            String fromEntityUID = ""+resultRelationship.startNodeId();
                            String toEntityUID = ""+resultRelationship.endNodeId();
                            Neo4JRelationshipEntityImpl neo4jRelationshipEntityImpl =
                                    new Neo4JRelationshipEntityImpl(relationKindName,relationEntityUID,fromEntityUID,toEntityUID);
                            neo4jRelationshipEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                            relationshipEntitySet.add(neo4jRelationshipEntityImpl);
                        }
                    }
                }
            }
            return relationshipEntitySet;
        }
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }

    @Override
    public String getEntityUID() {
        return relationKindUID;
    }

    @Override
    public GraphOperationExecutorHelper getGraphOperationExecutorHelper() {
        return graphOperationExecutorHelper;
    }
}
