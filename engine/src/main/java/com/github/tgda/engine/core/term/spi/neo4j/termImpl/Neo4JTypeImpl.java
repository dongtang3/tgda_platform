package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.FilteringItem;
import com.github.tgda.engine.core.exception.EngineFunctionNotSupportedException;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.*;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.payload.*;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonEntitiesAttributesRetrieveResultImpl;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonEntitiesRetrieveResultImpl;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonEntitiesOperationResultImpl;
import com.github.tgda.engine.core.structure.InheritanceTree;
import com.github.tgda.engine.core.term.*;
import com.github.tgda.engine.core.util.Constant;
import com.google.common.collect.Lists;
import com.github.tgda.coreRealm.realmServiceCore.internal.neo4j.dataTransformer.*;
import com.github.tgda.coreRealm.realmServiceCore.payload.*;
import com.github.tgda.coreRealm.realmServiceCore.term.*;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JType;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Value;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.*;

public class Neo4JTypeImpl implements Neo4JType {

    private static Logger logger = LoggerFactory.getLogger(Neo4JTypeImpl.class);
    private String coreRealmName;
    private String conceptionKindName;
    private String conceptionKindDesc;
    private String conceptionKindUID;
    private static Map<String, Object> singleValueAttributesViewKindTypeFilter = new HashMap<>();
    static {
        singleValueAttributesViewKindTypeFilter.put(Constant._viewKindDataForm,""+ AttributesView.AttributesViewKindDataForm.SINGLE_VALUE);
    }

    public Neo4JTypeImpl(String coreRealmName, String conceptionKindName, String conceptionKindDesc, String conceptionKindUID){
        this.coreRealmName = coreRealmName;
        this.conceptionKindName = conceptionKindName;
        this.conceptionKindDesc = conceptionKindDesc;
        this.conceptionKindUID = conceptionKindUID;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    public String getTypeUID() {
        return this.conceptionKindUID;
    }

    public String getCoreRealmName() {
        return this.coreRealmName;
    }

    @Override
    public String getTypeName() {
        return this.conceptionKindName;
    }

    @Override
    public String getTypeDesc() {
        return this.conceptionKindDesc;
    }

    @Override
    public boolean updateTypeDesc(String kindDesc) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            Map<String,Object> attributeDataMap = new HashMap<>();
            attributeDataMap.put(Constant._DescProperty, kindDesc);
            String updateCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.conceptionKindUID),attributeDataMap);
            GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._DescProperty);
            Object updateResultRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeValueTransformer,updateCql);
            CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.conceptionKindUID,false);
            AttributeValue resultAttributeValue =  updateResultRes != null ? (AttributeValue) updateResultRes : null;
            if(resultAttributeValue != null && resultAttributeValue.getAttributeValue().toString().equals(kindDesc)){
                this.conceptionKindDesc = kindDesc;
                return true;
            }else{
                return false;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Long countConceptionEntities() throws EngineServiceRuntimeException {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            String queryCql = CypherBuilder.matchLabelWithSinglePropertyValueAndFunction(getTypeName(), CypherBuilder.CypherFunctionType.COUNT, null, null);
            GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer("count");
            Object countConceptionEntitiesRes = workingGraphOperationExecutor.executeRead(getLongFormatAggregatedReturnValueTransformer, queryCql);
            if (countConceptionEntitiesRes == null) {
                throw new EngineServiceRuntimeException();
            } else {
                return (Long) countConceptionEntitiesRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Long countConceptionEntitiesWithOffspring() throws EngineFunctionNotSupportedException {
        EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
        exception.setCauseMessage("Neo4J storage implements doesn't support this function");
        throw exception;
    }

    @Override
    public List<Type> getChildTypes() throws EngineFunctionNotSupportedException {
        EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
        exception.setCauseMessage("Neo4J storage implements doesn't support this function");
        throw exception;
    }

    @Override
    public Type getParentType() throws EngineFunctionNotSupportedException {
        EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
        exception.setCauseMessage("Neo4J storage implements doesn't support this function");
        throw exception;
    }

    @Override
    public InheritanceTree<Type> getOffspringTypes() throws EngineFunctionNotSupportedException {
        EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
        exception.setCauseMessage("Neo4J storage implements doesn't support this function");
        throw exception;
    }

    @Override
    public Entity newEntity(EntityValue conceptionEntityValue, boolean addPerDefinedRelation) {
        if (conceptionEntityValue != null) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                Map<String, Object> propertiesMap = conceptionEntityValue.getEntityAttributesValue() != null ?
                        conceptionEntityValue.getEntityAttributesValue() : new HashMap<>();
                CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
                String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{this.conceptionKindName}, propertiesMap);
                GetSingleEntityTransformer getSingleEntityTransformer =
                        new GetSingleEntityTransformer(this.conceptionKindName, this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object newEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer, createCql);

                Entity resultEntity = newEntityRes != null ? (Entity) newEntityRes : null;
                if(addPerDefinedRelation && resultEntity != null){
                    List<String> uidList = new ArrayList<>();
                    uidList.add(resultEntity.getEntityUID());
                    CommonOperationUtil.attachEntities(this.conceptionKindName,uidList,workingGraphOperationExecutor);
                }
                return resultEntity;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public Entity newEntity(EntityValue conceptionEntityValue, List<RelationshipAttach> relationAttachKindList, RelationshipAttach.EntityRelateRole entityRelateRole) {
        Entity resultEntity = newEntity(conceptionEntityValue,false);
        if(relationAttachKindList != null){
            for(RelationshipAttach currentRelationshipAttach : relationAttachKindList){
                currentRelationshipAttach.newRelationEntities(resultEntity.getEntityUID(),entityRelateRole,null);
            }
        }
        return resultEntity;
    }

    @Override
    public EntitiesOperationResult newEntities(List<EntityValue> conceptionEntityValues, boolean addPerDefinedRelation) {
        if(conceptionEntityValues !=null && conceptionEntityValues.size()>0){
            CommonEntitiesOperationResultImpl commonEntitiesOperationResultImpl = new CommonEntitiesOperationResultImpl();

            ZonedDateTime currentDateTime = ZonedDateTime.now();
            List<Map<String,Object>> attributesValueMap = new ArrayList<>();
            for(EntityValue currentEntityValue:conceptionEntityValues){
                Map<String,Object> currentDateAttributesMap = currentEntityValue.getEntityAttributesValue();
                CommonOperationUtil.generateEntityMetaAttributes(currentDateAttributesMap,currentDateTime);
                attributesValueMap.add(currentDateAttributesMap);
            }

            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String createCql = CypherBuilder.createMultiLabeledNodesWithProperties(new String[]{this.conceptionKindName}, attributesValueMap);
                GetMapFormatAggregatedReturnValueTransformer getMapFormatAggregatedReturnValueTransformer =
                        new GetMapFormatAggregatedReturnValueTransformer();
                Object newEntityRes = workingGraphOperationExecutor.executeWrite(getMapFormatAggregatedReturnValueTransformer, createCql);
                if(newEntityRes!=null){
                    Map<String, Node> resultNodesMap = (Map<String, Node>)newEntityRes;
                    Iterator<Map.Entry<String,Node>> iter = resultNodesMap.entrySet().iterator();
                    while(iter.hasNext()){
                        Map.Entry<String,Node> entry = iter.next();
                        Node value = entry.getValue();
                        commonEntitiesOperationResultImpl.getSuccessEntityUIDs().add(""+value.id());
                    }
                    commonEntitiesOperationResultImpl.getOperationStatistics().setSuccessItemsCount(resultNodesMap.size());
                    commonEntitiesOperationResultImpl.getOperationStatistics().
                            setOperationSummary("newEntities operation for conceptionKind "+this.conceptionKindName+" success.");
                }
                commonEntitiesOperationResultImpl.finishEntitiesOperation();
                if(addPerDefinedRelation && commonEntitiesOperationResultImpl.getSuccessEntityUIDs() != null){
                    CommonOperationUtil.attachEntities(this.conceptionKindName,commonEntitiesOperationResultImpl.getSuccessEntityUIDs(),workingGraphOperationExecutor);
                }
                return commonEntitiesOperationResultImpl;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public EntitiesOperationResult newEntities(List<EntityValue> conceptionEntityValues, List<RelationshipAttach> relationAttachKindList, RelationshipAttach.EntityRelateRole entityRelateRole) {
        EntitiesOperationResult entitiesOperationResult =  newEntities(conceptionEntityValues,false);
        if(relationAttachKindList != null){
            for(RelationshipAttach currentRelationshipAttach : relationAttachKindList){
                currentRelationshipAttach.newRelationEntities(entitiesOperationResult.getSuccessEntityUIDs(),entityRelateRole,null);
            }
        }
        return entitiesOperationResult;
    }

    @Override
    public Entity updateEntity(EntityValue conceptionEntityValueForUpdate) throws EngineServiceRuntimeException {
        if(conceptionEntityValueForUpdate != null && conceptionEntityValueForUpdate.getEntityUID() != null){
            Entity targetEntity = this.getEntityByUID(conceptionEntityValueForUpdate.getEntityUID());
            if(targetEntity != null){
                Map<String,Object> newValueMap = conceptionEntityValueForUpdate.getEntityAttributesValue();
                targetEntity.updateAttributes(newValueMap);
                return this.getEntityByUID(conceptionEntityValueForUpdate.getEntityUID());
            }else{
                logger.error("Type {} does not contains entity with UID {}.", this.conceptionKindName, conceptionEntityValueForUpdate.getEntityUID());
                EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                exception.setCauseMessage("Type " + this.conceptionKindName + " does not contains entity with UID " +  conceptionEntityValueForUpdate.getEntityUID() + ".");
                throw exception;
            }
        }
        return null;
    }

    @Override
    public EntitiesOperationResult updateEntities(List<EntityValue> entityValues) {
        if(entityValues != null && entityValues.size()>0){
            CommonEntitiesOperationResultImpl commonEntitiesOperationResultImpl = new CommonEntitiesOperationResultImpl();
            boolean countFail = false;
            for(EntityValue currentEntityValue:entityValues){
                Entity targetEntity = this.getEntityByUID(currentEntityValue.getEntityUID());
                if(targetEntity != null){
                    Map<String,Object> newValueMap = currentEntityValue.getEntityAttributesValue();
                    List<String> updateSuccessResult = targetEntity.updateAttributes(newValueMap);
                    if(updateSuccessResult != null && updateSuccessResult.size()>0){
                        commonEntitiesOperationResultImpl.getSuccessEntityUIDs().add(currentEntityValue.getEntityUID());
                        commonEntitiesOperationResultImpl.getOperationStatistics().increaseSuccessCount();
                    }else{
                        commonEntitiesOperationResultImpl.getOperationStatistics().increaseFailCount();
                        countFail = true;
                    }
                }else{
                    commonEntitiesOperationResultImpl.getOperationStatistics().increaseFailCount();
                    countFail = true;
                }
            }
            if(countFail){
                commonEntitiesOperationResultImpl.getOperationStatistics().
                        setOperationSummary("updateEntities operation for conceptionKind "+this.conceptionKindName+" partial success.");
            }else{
                commonEntitiesOperationResultImpl.getOperationStatistics().
                        setOperationSummary("updateEntities operation for conceptionKind "+this.conceptionKindName+" success.");
            }
            commonEntitiesOperationResultImpl.finishEntitiesOperation();
            return commonEntitiesOperationResultImpl;
        }
        return null;
    }

    @Override
    public boolean deleteEntity(String conceptionEntityUID) throws EngineServiceRuntimeException {
        if(conceptionEntityUID != null){
            Entity targetEntity = this.getEntityByUID(conceptionEntityUID);
            if(targetEntity != null){
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
                try{
                    String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.valueOf(conceptionEntityUID),null,null);
                    GetSingleEntityTransformer getSingleEntityTransformer =
                            new GetSingleEntityTransformer(this.conceptionKindName, this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                    Object deletedEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer, deleteCql);
                    if(deletedEntityRes == null){
                        throw new EngineServiceRuntimeException();
                    }else{
                        return true;
                    }
                }finally {
                    this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
                }
            }else{
                logger.error("Type {} does not contains entity with UID {}.", this.conceptionKindName, conceptionEntityUID);
                EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                exception.setCauseMessage("Type " + this.conceptionKindName + " does not contains entity with UID " + conceptionEntityUID + ".");
                throw exception;
            }
        }
        return false;
    }

    @Override
    public EntitiesOperationResult deleteEntities(List<String> conceptionEntityUIDs) {
        if(conceptionEntityUIDs != null && conceptionEntityUIDs.size()>0){
            CommonEntitiesOperationResultImpl commonEntitiesOperationResultImpl = new CommonEntitiesOperationResultImpl();
            boolean countFail = false;
            for(String currentEntityUID:conceptionEntityUIDs) {
                Entity targetEntity = this.getEntityByUID(currentEntityUID);
                if(targetEntity != null){
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
                        logger.error("Exception occurred during delete entity with UID {} of Type {}.", currentEntityUID , this.conceptionKindName);
                    }
                }else{
                    commonEntitiesOperationResultImpl.getOperationStatistics().increaseFailCount();
                    countFail = true;
                }
            }
            if(countFail){
                commonEntitiesOperationResultImpl.getOperationStatistics().
                        setOperationSummary("deleteEntities operation for conceptionKind "+this.conceptionKindName+" partial success.");
            }else{
                commonEntitiesOperationResultImpl.getOperationStatistics().
                        setOperationSummary("deleteEntities operation for conceptionKind "+this.conceptionKindName+" success.");
            }
            commonEntitiesOperationResultImpl.finishEntitiesOperation();
            return commonEntitiesOperationResultImpl;
        }
        return null;
    }

    @Override
    public EntitiesOperationResult purgeAllEntities() throws EngineServiceRuntimeException {
        CommonEntitiesOperationResultImpl commonEntitiesOperationResultImpl = new CommonEntitiesOperationResultImpl();
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            // Using below solution for improving performance or execute operation success
            //https://neo4j.com/developer/kb/how-to-bulk-delete-dense-nodes/
            //https://www.freesion.com/article/24571268014/
            String bulkDeleteCql ="MATCH (n:"+this.conceptionKindName+")\n" +
                    "WITH collect(n) AS nn\n" +
                    "CALL apoc.periodic.commit(\"\n" +
                    "  UNWIND $nodes AS n\n" +
                    "  WITH sum(size([p=(n)-[]-() | p])) AS count_remaining,\n" +
                    "       collect(n) AS nn\n" +
                    "  UNWIND nn AS n\n" +
                    "  OPTIONAL MATCH (n)-[r]-()\n" +
                    "  WITH n, r, count_remaining\n" +
                    "  LIMIT $limit\n" +
                    "  DELETE r\n" +
                    "  RETURN count_remaining\n" +
                    "\",{limit:10000, nodes:nn}) yield updates, executions, runtime, batches, failedBatches, batchErrors, failedCommits, commitErrors\n" +
                    "UNWIND nn AS n\n" +
                    "DETACH DELETE n\n" +
                    "RETURN updates, executions, runtime, batches";

            String countQueryCql = CypherBuilder.matchLabelWithSinglePropertyValueAndFunction(getTypeName(), CypherBuilder.CypherFunctionType.COUNT, null, null);
            long beforeExecuteEntityCount = 0;
            GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer("count");
            Object countConceptionEntitiesRes = workingGraphOperationExecutor.executeRead(getLongFormatAggregatedReturnValueTransformer, countQueryCql);
            if (countConceptionEntitiesRes == null) {
                throw new EngineServiceRuntimeException();
            } else {
               beforeExecuteEntityCount = (Long) countConceptionEntitiesRes;
            }

            logger.debug("Generated Cypher Statement: {}", bulkDeleteCql);
            workingGraphOperationExecutor.executeWrite(new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    return null;
                }
            },bulkDeleteCql);

            long afterExecuteEntityCount = 0;
            countConceptionEntitiesRes = workingGraphOperationExecutor.executeRead(getLongFormatAggregatedReturnValueTransformer, countQueryCql);
            if (countConceptionEntitiesRes == null) {
                throw new EngineServiceRuntimeException();
            } else {
                afterExecuteEntityCount = (Long) countConceptionEntitiesRes;
            }

            commonEntitiesOperationResultImpl.getOperationStatistics().setSuccessItemsCount(beforeExecuteEntityCount-afterExecuteEntityCount);
            commonEntitiesOperationResultImpl.getOperationStatistics().
                    setOperationSummary("purgeAllEntities operation for conceptionKind "+this.conceptionKindName+" success.");

            /*
            String deleteCql = CypherBuilder.deleteLabelWithSinglePropertyValueAndFunction(this.conceptionKindName,
                    CypherBuilder.CypherFunctionType.COUNT,null,null);
            GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer =
                    new GetLongFormatAggregatedReturnValueTransformer("count");
            Object deleteResultObject = workingGraphOperationExecutor.executeWrite(getLongFormatAggregatedReturnValueTransformer,deleteCql);

            if(deleteResultObject == null){
                throw new CoreRealmServiceRuntimeException();
            }else{
                commonEntitiesOperationResultImpl.getOperationStatistics().setSuccessItemsCount((Long)deleteResultObject);
                commonEntitiesOperationResultImpl.getOperationStatistics().
                        setOperationSummary("purgeAllEntities operation for conceptionKind "+this.conceptionKindName+" success.");
            }
            */

            commonEntitiesOperationResultImpl.finishEntitiesOperation();
            return commonEntitiesOperationResultImpl;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Long countEntities(AttributesParameters attributesParameters, boolean isDistinctMode) throws EngineServiceEntityExploreException, EngineServiceRuntimeException {
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
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(this.conceptionKindName,queryParameters, CypherBuilder.CypherFunctionType.COUNT);
                GetLongFormatAggregatedReturnValueTransformer GetLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer("count");
                Object queryRes = workingGraphOperationExecutor.executeRead(GetLongFormatAggregatedReturnValueTransformer,queryCql);
                if(queryRes != null){
                    return (Long)queryRes;
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
            return null;

        }else{
            return countConceptionEntities();
        }
    }

    @Override
    public EntitiesRetrieveResult getEntities(QueryParameters queryParameters) throws EngineServiceEntityExploreException {
        if (queryParameters != null) {
            CommonEntitiesRetrieveResultImpl commonConceptionEntitiesRetrieveResultImpl = new CommonEntitiesRetrieveResultImpl();
            commonConceptionEntitiesRetrieveResultImpl.getOperationStatistics().setQueryParameters(queryParameters);
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(this.conceptionKindName,queryParameters,null);
                GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(this.conceptionKindName,
                        this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object queryRes = workingGraphOperationExecutor.executeRead(getListEntityTransformer,queryCql);
                if(queryRes != null){
                    List<Entity> resultEntityList = (List<Entity>)queryRes;
                    commonConceptionEntitiesRetrieveResultImpl.addConceptionEntities(resultEntityList);
                    commonConceptionEntitiesRetrieveResultImpl.getOperationStatistics().setResultEntitiesCount(resultEntityList.size());
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
            commonConceptionEntitiesRetrieveResultImpl.finishEntitiesRetrieving();
            return commonConceptionEntitiesRetrieveResultImpl;
        }
       return null;
    }

    @Override
    public Entity getEntityByUID(String conceptionEntityUID) {
        if (conceptionEntityUID != null) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(conceptionEntityUID), null, null);
                GetSingleEntityTransformer getSingleEntityTransformer =
                        new GetSingleEntityTransformer(this.conceptionKindName, this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object resEntityRes = workingGraphOperationExecutor.executeRead(getSingleEntityTransformer, queryCql);
                return resEntityRes != null ? (Entity) resEntityRes : null;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public EntitiesAttributesRetrieveResult getSingleValueEntityAttributesByViewKinds(List<String> attributesViewKindNames, QueryParameters exploreParameters) throws EngineServiceEntityExploreException {
        if(attributesViewKindNames != null && attributesViewKindNames.size()>0){
            List<AttributesView> resultRealAttributesViewKindList = new ArrayList<>();
            for(String currentTargetViewKindName:attributesViewKindNames){
                List<AttributesView> currentAttributesViewKinds = getContainsAttributesViewKinds(currentTargetViewKindName);
                if(currentAttributesViewKinds != null){
                    resultRealAttributesViewKindList.addAll(currentAttributesViewKinds);
                }
            }
            List<AttributeKind> allResultTargetAttributeKindList = new ArrayList<>();
            for(AttributesViewKind resultAttributesViewKind:resultRealAttributesViewKindList){
                List<AttributeKind> currentAttributeKinds = resultAttributesViewKind.getContainsAttributeKinds();
                if(currentAttributeKinds != null){
                    allResultTargetAttributeKindList.addAll(currentAttributeKinds);
                }
            }
            List<String> targetAttributeKindNameList = filterSingleValueAttributeKindNames(allResultTargetAttributeKindList);
           return getSingleValueEntityAttributesByAttributeNames(targetAttributeKindNameList,exploreParameters);
        }
        return null;
    }

    @Override
    public ConceptionEntitiesAttributesRetrieveResult getSingleValueEntityAttributesByAttributeNames(List<String> attributeNames, QueryParameters exploreParameters) throws EngineServiceEntityExploreException {
        if(attributeNames != null && attributeNames.size()>0){
            CommonEntitiesAttributesRetrieveResultImpl commonConceptionEntitiesAttributesRetrieveResultImpl
                    = new CommonEntitiesAttributesRetrieveResultImpl();
            commonConceptionEntitiesAttributesRetrieveResultImpl.getOperationStatistics().setQueryParameters(exploreParameters);
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchAttributesWithQueryParameters(this.conceptionKindName,exploreParameters,attributeNames);
                List<AttributeKind> containsAttributesKinds = getContainsSingleValueAttributeKinds(workingGraphOperationExecutor);
                GetListEntityValueTransformer getListEntityValueTransformer =
                        new GetListEntityValueTransformer(attributeNames,containsAttributesKinds);
                Object resEntityRes = workingGraphOperationExecutor.executeRead(getListEntityValueTransformer, queryCql);
                if(resEntityRes != null){
                    List<EntityValue> resultEntitiesValues = (List<EntityValue>)resEntityRes;
                    commonConceptionEntitiesAttributesRetrieveResultImpl.addConceptionEntitiesAttributes(resultEntitiesValues);
                    commonConceptionEntitiesAttributesRetrieveResultImpl.getOperationStatistics().setResultEntitiesCount(resultEntitiesValues.size());
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
            commonConceptionEntitiesAttributesRetrieveResultImpl.finishEntitiesRetrieving();
            return commonConceptionEntitiesAttributesRetrieveResultImpl;
        }
        return null;
    }

    @Override
    public boolean attachAttributesViewKind(String attributesViewKindUID) throws EngineServiceRuntimeException {
        if(attributesViewKindUID == null){
            return false;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(attributesViewKindUID), null, null);
            GetSingleAttributesViewKindTransformer getSingleAttributesViewKindTransformer =
                    new GetSingleAttributesViewKindTransformer(coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object checkAttributesViewKindRes = workingGraphOperationExecutor.executeWrite(getSingleAttributesViewKindTransformer,queryCql);
            if(checkAttributesViewKindRes != null){
                String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(conceptionKindUID),Long.parseLong(attributesViewKindUID),
                        Constant.Type_AttributesViewKindRelationClass);

                GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (Constant.Type_AttributesViewKindRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, queryRelationCql);
                if(existingRelationshipEntityRes != null){
                    return true;
                }

                Map<String,Object> relationPropertiesMap = new HashMap<>();
                CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesMap);
                String createCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(conceptionKindUID),Long.parseLong(attributesViewKindUID),
                        Constant.Type_AttributesViewKindRelationClass,relationPropertiesMap);
                getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (Constant.Type_AttributesViewKindRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object newRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, createCql);
                if(newRelationshipEntityRes == null){
                    throw new EngineServiceRuntimeException();
                }else{
                    return true;
                }
            }else{
                logger.error("AttributesViewKind does not contains entity with UID {}.", attributesViewKindUID);
                EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                exception.setCauseMessage("AttributesViewKind does not contains entity with UID " + attributesViewKindUID + ".");
                throw exception;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<AttributesView> getContainsAttributesViewKinds(String attributesViewKindName) {
        if(attributesViewKindName == null){
            return null;
        }else{
            List<AttributesView> resultAttributesViewKindList = new ArrayList<>();
            List<AttributesView> allContainsAttributesViewKinds = this.getContainsAttributesViewKinds();
            if(allContainsAttributesViewKinds != null && allContainsAttributesViewKinds.size()>0){
                for(AttributesView currentAttributesViewKind : allContainsAttributesViewKinds){
                    if(currentAttributesViewKind.getAttributesViewKindName().equals(attributesViewKindName.trim())){
                        resultAttributesViewKindList.add(currentAttributesViewKind);
                    }
                }
            }
            return resultAttributesViewKindList;
        }
    }

    @Override
    public boolean detachAttributesViewKind(String attributesViewKindUID) throws EngineServiceRuntimeException {
        if(attributesViewKindUID == null){
            return false;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(attributesViewKindUID), null, null);
            GetSingleAttributesViewKindTransformer getSingleAttributesViewKindTransformer =
                    new GetSingleAttributesViewKindTransformer(coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object checkAttributesViewKindRes = workingGraphOperationExecutor.executeWrite(getSingleAttributesViewKindTransformer,queryCql);
            if(checkAttributesViewKindRes != null){
                String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(conceptionKindUID),Long.parseLong(attributesViewKindUID),
                        Constant.Type_AttributesViewKindRelationClass);

                GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (Constant.Type_AttributesViewKindRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, queryRelationCql);

                if(existingRelationshipEntityRes == null){
                    return false;
                }
                RelationshipEntity relationEntity = (RelationshipEntity)existingRelationshipEntityRes;

                String deleteCql = CypherBuilder.deleteRelationWithSingleFunctionValueEqual(
                        CypherBuilder.CypherFunctionType.ID,Long.valueOf(relationEntity.getRelationshipEntityUID()),null,null);

                getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (Constant.Type_AttributesViewKindRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object deleteRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, deleteCql);
                if(deleteRelationshipEntityRes == null){
                    return false;
                }else{
                    return true;
                }
            }else{
                logger.error("AttributesViewKind does not contains entity with UID {}.", attributesViewKindUID);
                EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                exception.setCauseMessage("AttributesViewKind does not contains entity with UID " + attributesViewKindUID + ".");
                throw exception;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<AttributesView> getContainsAttributesViewKinds() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchRelatedNodesFromSpecialStartNodes(
                    CypherBuilder.CypherFunctionType.ID, Long.parseLong(conceptionKindUID),
                    Constant.AttributesViewKindClass, Constant.Type_AttributesViewRelationClass, Direction.TO, null);
            GetListAttributesViewKindTransformer getListAttributesViewKindTransformer =
                    new GetListAttributesViewKindTransformer(Constant.Type_AttributesViewRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object attributesViewKindsRes = workingGraphOperationExecutor.executeWrite(getListAttributesViewKindTransformer,queryCql);
            return attributesViewKindsRes != null ? (List<AttributesView>) attributesViewKindsRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<AttributeKind> getContainsSingleValueAttributeKinds() {
        return getSingleValueAttributeKinds(null);
    }

    @Override
    public List<AttributeKind> getContainsSingleValueAttributeKinds(String attributeKindName) {
        if(attributeKindName == null){
            return null;
        }else{
          return getSingleValueAttributeKinds(attributeKindName);
        }
    }

    @Override
    public ConceptionEntitiesRetrieveResult getKindDirectRelatedEntities(List<String> startEntityUIDS,String relationKind, Direction relationDirection, String targetType, QueryParameters queryParameters) throws EngineServiceEntityExploreException {
        if(relationKind == null){
            logger.error("RelationKind is required.");
            EngineServiceEntityExploreException exception = new EngineServiceEntityExploreException();
            exception.setCauseMessage("RelationKind is required.");
            throw exception;
        }
        Direction realDirection = relationDirection != null ? relationDirection : Direction.TWO_WAY;
        CommonEntitiesRetrieveResultImpl commonConceptionEntitiesRetrieveResultImpl = new CommonEntitiesRetrieveResultImpl();
        commonConceptionEntitiesRetrieveResultImpl.getOperationStatistics().setQueryParameters(queryParameters);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchNodeWithSpecialRelationAndAttributeFilter(relationKind,realDirection,
                    this.conceptionKindName,startEntityUIDS,targetType,queryParameters);
            GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(null,
                    this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object queryRes = workingGraphOperationExecutor.executeRead(getListEntityTransformer,queryCql);
            if(queryRes != null){
                List<Entity> resultEntityList = (List<Entity>)queryRes;
                commonConceptionEntitiesRetrieveResultImpl.addConceptionEntities(resultEntityList);
                commonConceptionEntitiesRetrieveResultImpl.getOperationStatistics().setResultEntitiesCount(resultEntityList.size());
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        commonConceptionEntitiesRetrieveResultImpl.finishEntitiesRetrieving();
        return commonConceptionEntitiesRetrieveResultImpl;
    }

    @Override
    public ConceptionEntitiesAttributesRetrieveResult getAttributesOfKindDirectRelatedEntities(List<String> startEntityUIDS, List<String> attributeNames, String relationKind, Direction relationDirection, String targetType, QueryParameters queryParameters) throws EngineServiceEntityExploreException {
        if(relationKind == null){
            logger.error("RelationKind is required.");
            EngineServiceEntityExploreException exception = new EngineServiceEntityExploreException();
            exception.setCauseMessage("RelationKind is required.");
            throw exception;
        }
        if(attributeNames != null && attributeNames.size()>0){
            CommonEntitiesAttributesRetrieveResultImpl commonConceptionEntitiesAttributesRetrieveResultImpl
                    = new CommonEntitiesAttributesRetrieveResultImpl();
            Direction realDirection = relationDirection != null ? relationDirection : Direction.TWO_WAY;

            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String queryCql = CypherBuilder.matchNodeWithSpecialRelationAndAttributeFilter(relationKind,realDirection,
                        this.conceptionKindName,startEntityUIDS,targetType,queryParameters);

                GetListEntityValueTransformer getListEntityValueTransformer = new GetListEntityValueTransformer(attributeNames);
                Object resEntityRes = workingGraphOperationExecutor.executeRead(getListEntityValueTransformer, queryCql);
                if(resEntityRes != null){
                    List<EntityValue> resultEntitiesValues = (List<EntityValue>)resEntityRes;
                    commonConceptionEntitiesAttributesRetrieveResultImpl.addConceptionEntitiesAttributes(resultEntitiesValues);
                    commonConceptionEntitiesAttributesRetrieveResultImpl.getOperationStatistics().setResultEntitiesCount(resultEntitiesValues.size());
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
            commonConceptionEntitiesAttributesRetrieveResultImpl.finishEntitiesRetrieving();
            return commonConceptionEntitiesAttributesRetrieveResultImpl;
        }else{
            return null;
        }
    }

    @Override
    public ConceptionEntitiesRetrieveResult getEntitiesByDirectRelations(String relationKind, Direction relationDirection, String aimType, QueryParameters queryParameters) throws EngineServiceEntityExploreException {
        if(relationKind == null){
            logger.error("RelationKind is required.");
            EngineServiceEntityExploreException exception = new EngineServiceEntityExploreException();
            exception.setCauseMessage("RelationKind is required.");
            throw exception;
        }
        Direction realDirection =  Direction.TWO_WAY;

        if(relationDirection != null){
            switch(relationDirection){
                case FROM:
                    realDirection = Direction.TO;
                    break;
                case TO:
                    realDirection = Direction.FROM;
                    break;
                case TWO_WAY:
                    realDirection =  Direction.TWO_WAY;
            }
        }
        CommonEntitiesRetrieveResultImpl commonConceptionEntitiesRetrieveResultImpl = new CommonEntitiesRetrieveResultImpl();
        commonConceptionEntitiesRetrieveResultImpl.getOperationStatistics().setQueryParameters(queryParameters);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchNodesWithQueryParameters(aimType,queryParameters,null);
            DataTransformer<List<String>> aimTypeEntityUIDListDataTransformer = new DataTransformer<List<String>>() {
                @Override
                public List<String> transformResult(Result result) {
                    List<String> conceptionEntityUIDList = new ArrayList<>();
                    while(result.hasNext()){
                        Record nodeRecord = result.next();
                        Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                        long nodeUID = resultNode.id();
                        String conceptionEntityUID = ""+nodeUID;
                        conceptionEntityUIDList.add(conceptionEntityUID);

                    }
                    return conceptionEntityUIDList;
                }
            };
            Object queryRes = workingGraphOperationExecutor.executeRead(aimTypeEntityUIDListDataTransformer,queryCql);
            List aimTypeEntityUIDList = (List<String>)queryRes;
            queryCql = CypherBuilder.matchNodeWithSpecialRelationAndAttributeFilter(relationKind,realDirection,
                    aimType,aimTypeEntityUIDList,this.conceptionKindName,null);
            GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(null,
                    this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            queryRes = workingGraphOperationExecutor.executeRead(getListEntityTransformer,queryCql);
            if(queryRes != null){
                List<Entity> resultEntityList = (List<Entity>)queryRes;
                commonConceptionEntitiesRetrieveResultImpl.addConceptionEntities(resultEntityList);
                commonConceptionEntitiesRetrieveResultImpl.getOperationStatistics().setResultEntitiesCount(resultEntityList.size());
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        commonConceptionEntitiesRetrieveResultImpl.finishEntitiesRetrieving();
        return commonConceptionEntitiesRetrieveResultImpl;
    }

    @Override
    public Set<KindAttributeDistributionInfo> getKindAttributesDistributionStatistics(double sampleRatio) throws EngineServiceRuntimeException {
        if(sampleRatio >1 || sampleRatio<=0){
            logger.error("Sample Ratio should between (0,1] .");
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("Sample Ratio should between (0,1] .");
            throw exception;
        }
        String cql = "MATCH (n:"+this.conceptionKindName+") WHERE rand() <= "+sampleRatio+"\n" +
                "RETURN\n" +
                "DISTINCT labels(n),max(keys(n)) as PropertyList,count(*) AS SampleSize";
        logger.debug("Generated Cypher Statement: {}", cql);
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            DataTransformer<Set<KindAttributeDistributionInfo>> aimTypeEntityUIDListDataTransformer = new DataTransformer<Set<KindAttributeDistributionInfo>>() {
                @Override
                public Set<KindAttributeDistributionInfo> transformResult(Result result) {
                    Set<KindAttributeDistributionInfo> resultSet = new HashSet<>();
                    while(result.hasNext()){
                        Record nodeRecord = result.next();
                        List<Object> kindNames = nodeRecord.get("labels(n)").asList();
                        List<Object> attributesNames = nodeRecord.get("PropertyList").asList();

                        String[] kindNamesArray = new String[kindNames.size()];
                        for(int i=0;i<kindNamesArray.length;i++){
                            kindNamesArray[i] = kindNames.get(i).toString();
                        }
                        String[] attributeNamesArray = new String[attributesNames.size()];
                        for(int i=0;i<attributeNamesArray.length;i++){
                            attributeNamesArray[i] = attributesNames.get(i).toString();
                        }
                        KindAttributeDistributionInfo currentKindAttributeDistributionInfo =
                                new KindAttributeDistributionInfo(kindNamesArray,attributeNamesArray);

                        resultSet.add(currentKindAttributeDistributionInfo);
                    }
                    return resultSet;
                }
            };
            Object queryRes = workingGraphOperationExecutor.executeRead(aimTypeEntityUIDListDataTransformer,cql);
            if(queryRes != null){
                return (Set<KindAttributeDistributionInfo>)queryRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public Set<KindDataDistributionInfo> getKindDataDistributionStatistics(double sampleRatio) throws EngineServiceRuntimeException {
        if(sampleRatio >1 || sampleRatio<=0){
            logger.error("Sample Ratio should between (0,1] .");
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("Sample Ratio should between (0,1] .");
            throw exception;
        }
        String cql = "MATCH (n:"+this.conceptionKindName+") WHERE rand() <= "+sampleRatio+"\n" +
                "        RETURN\n" +
                "        DISTINCT labels(n),\n" +
                "        count(*) AS SampleSize,\n" +
                "        avg(size(keys(n))) as Avg_PropertyCount,\n" +
                "        min(size(keys(n))) as Min_PropertyCount,\n" +
                "        max(size(keys(n))) as Max_PropertyCount,\n" +
                "        percentileDisc(size(keys(n)),0.5) as Middle_PropertyCount,\n" +
                "        avg(size([p=(n)-[]-() | p]) ) as Avg_RelationshipCount,\n" +
                "        min(size([p=(n)-[]-() | p]) ) as Min_RelationshipCount,\n" +
                "        max(size([p=(n)-[]-() | p]) ) as Max_RelationshipCount,\n" +
                "        percentileDisc(size([p=(n)-[]-() | p]), 0.5) as Middle_RelationshipCount";
        logger.debug("Generated Cypher Statement: {}", cql);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            DataTransformer<Set<KindDataDistributionInfo>> aimTypeEntityUIDListDataTransformer = new DataTransformer<Set<KindDataDistributionInfo>>() {
                @Override
                public Set<KindDataDistributionInfo> transformResult(Result result) {
                    Set<KindDataDistributionInfo> resultSet = new HashSet<>();
                    while(result.hasNext()){
                        Record nodeRecord = result.next();
                        List<Object> kindNames = nodeRecord.get("labels(n)").asList();
                        long entitySampleSize = nodeRecord.get("SampleSize").asLong();
                        double avgAttributeCount = nodeRecord.get("Avg_PropertyCount").asDouble();
                        int minAttributeCount = nodeRecord.get("Min_PropertyCount").asInt();
                        int maxAttributeCount = nodeRecord.get("Max_PropertyCount").asInt();
                        int medianAttributeCount = nodeRecord.get("Middle_PropertyCount").asInt();
                        double avgRelationCount = nodeRecord.get("Avg_RelationshipCount").asDouble();
                        int minRelationCount = nodeRecord.get("Min_RelationshipCount").asInt();
                        int maxRelationCount = nodeRecord.get("Max_RelationshipCount").asInt();
                        int medianRelationCount = nodeRecord.get("Middle_RelationshipCount").asInt();

                        String[] kindNamesArray = new String[kindNames.size()];
                        for(int i=0;i<kindNamesArray.length;i++){
                            kindNamesArray[i] = kindNames.get(i).toString();
                        }
                        KindDataDistributionInfo currentKindDataDistributionInfo = new KindDataDistributionInfo(kindNamesArray,entitySampleSize,
                                avgAttributeCount,minAttributeCount,maxAttributeCount,medianAttributeCount,
                                avgRelationCount,minRelationCount,maxRelationCount,medianRelationCount);

                        resultSet.add(currentKindDataDistributionInfo);
                    }
                    return resultSet;
                }
            };
            Object queryRes = workingGraphOperationExecutor.executeRead(aimTypeEntityUIDListDataTransformer,cql);
            if(queryRes != null){
                return (Set<KindDataDistributionInfo>)queryRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public Set<TypeCorrelationInfo> getKindRelationDistributionStatistics() {
        String cql ="CALL db.schema.visualization()";
        logger.debug("Generated Cypher Statement: {}", cql);
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            DataTransformer<Set<TypeCorrelationInfo>> statisticsDataTransformer = new DataTransformer(){
                @Override
                public Set<TypeCorrelationInfo> transformResult(Result result) {
                    Record currentRecord = result.next();
                    List<Object> nodesList = currentRecord.get("nodes").asList();
                    List<Object> relationshipsList = currentRecord.get("relationships").asList();

                    Set<TypeCorrelationInfo> conceptionKindCorrelationInfoSet = new HashSet<>();
                    String currentTypeID = null;
                    Map<String,String> conceptionKindId_nameMapping = new HashMap<>();
                    for(Object currentNodeObj:nodesList){
                        Node currentNode = (Node)currentNodeObj;
                        long currentNodeId = currentNode.id();
                        String currentTypeName = currentNode.labels().iterator().next();
                        if(conceptionKindName.equals(currentTypeName)){
                            currentTypeID = ""+currentNodeId;
                        }
                        conceptionKindId_nameMapping.put(""+currentNodeId,currentTypeName);
                    }

                    for(Object currentRelationshipObj:relationshipsList){
                        Relationship currentRelationship = (Relationship)currentRelationshipObj;
                        //long relationshipId = currentRelationship.id();
                        String relationshipType = currentRelationship.type();
                        String startTypeId = ""+currentRelationship.startNodeId();
                        String endTypeId = ""+currentRelationship.endNodeId();
                        if(startTypeId.equals(currentTypeID)||
                                endTypeId.equals(currentTypeID)){
                            TypeCorrelationInfo currentTypeCorrelationInfo =
                                    new TypeCorrelationInfo(
                                            conceptionKindId_nameMapping.get(startTypeId),
                                            conceptionKindId_nameMapping.get(endTypeId),
                                            relationshipType,1);
                            conceptionKindCorrelationInfoSet.add(currentTypeCorrelationInfo);
                        }
                    }
                    return conceptionKindCorrelationInfoSet;
                }
            };
            Object queryRes = workingGraphOperationExecutor.executeRead(statisticsDataTransformer,cql);
            if(queryRes != null){
                return (Set<TypeCorrelationInfo>)queryRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public Set<Entity> getRandomEntities(int entitiesCount) throws EngineServiceEntityExploreException {
        if(entitiesCount < 1){
            logger.error("entitiesCount must equal or great then 1.");
            EngineServiceEntityExploreException exception = new EngineServiceEntityExploreException();
            exception.setCauseMessage("entitiesCount must equal or great then 1.");
            throw exception;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = "MATCH (n:"+this.conceptionKindName+") RETURN apoc.coll.randomItems(COLLECT(n),"+entitiesCount+") AS " +CypherBuilder.operationResultName;
            logger.debug("Generated Cypher Statement: {}", queryCql);
            RandomItemsEntitySetDataTransformer randomItemsEntitySetDataTransformer =
                    new RandomItemsEntitySetDataTransformer(workingGraphOperationExecutor);
            Object queryRes = workingGraphOperationExecutor.executeRead(randomItemsEntitySetDataTransformer,queryCql);
            if(queryRes != null){
                Set<Entity> resultEntityList = (Set<Entity>)queryRes;
                return resultEntityList;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public Set<Entity> getRandomEntities(AttributesParameters attributesParameters, boolean isDistinctMode, int entitiesCount) throws EngineServiceEntityExploreException, EngineServiceRuntimeException {
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
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(this.conceptionKindName,queryParameters,null);
                String replaceContent = isDistinctMode ? "RETURN DISTINCT "+CypherBuilder.operationResultName+" LIMIT 100000000":
                        "RETURN "+CypherBuilder.operationResultName+" LIMIT 100000000";
                String newContent = isDistinctMode ? "RETURN apoc.coll.randomItems(COLLECT("+CypherBuilder.operationResultName+"),"+entitiesCount+",false) AS " +CypherBuilder.operationResultName:
                        "RETURN apoc.coll.randomItems(COLLECT("+CypherBuilder.operationResultName+"),"+entitiesCount+",true) AS " +CypherBuilder.operationResultName;
                queryCql = queryCql.replace(replaceContent,newContent);
                logger.debug("Generated Cypher Statement: {}", queryCql);
                RandomItemsEntitySetDataTransformer randomItemsEntitySetDataTransformer =
                        new RandomItemsEntitySetDataTransformer(workingGraphOperationExecutor);
                Object queryRes = workingGraphOperationExecutor.executeRead(randomItemsEntitySetDataTransformer,queryCql);
                if(queryRes != null){
                    Set<Entity> resultEntityList = (Set<Entity>)queryRes;
                    return resultEntityList;
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
            String queryCql = CypherBuilder.setTypeProperties(this.conceptionKindName,attributes);
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

    private class RandomItemsEntitySetDataTransformer implements DataTransformer<Set<Entity>>{
        GraphOperationExecutor workingGraphOperationExecutor;
        public RandomItemsEntitySetDataTransformer(GraphOperationExecutor workingGraphOperationExecutor){
            this.workingGraphOperationExecutor = workingGraphOperationExecutor;
        }
        @Override
        public Set<Entity> transformResult(Result result) {
            Set<Entity> conceptionEntitySet = new HashSet<>();
            if(result.hasNext()){
                List<Value> resultList = result.next().values();
                if(resultList.size() > 0){
                    List<Object> nodeObjList = resultList.get(0).asList();
                    for(Object currentNodeObj : nodeObjList){
                        Node resultNode = (Node)currentNodeObj;
                        List<String> allTypeNames = Lists.newArrayList(resultNode.labels());
                        boolean isMatchedType = true;
                        if(allTypeNames.size()>0){
                            isMatchedType = allTypeNames.contains(conceptionKindName);
                        }
                        if(isMatchedType){
                            long nodeUID = resultNode.id();
                            String conceptionEntityUID = ""+nodeUID;
                            String resultTypeName = conceptionKindName;
                            Neo4JEntityImpl neo4jEntityImpl =
                                    new Neo4JEntityImpl(resultTypeName,conceptionEntityUID);
                            neo4jEntityImpl.setAllTypeNames(allTypeNames);
                            neo4jEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                            conceptionEntitySet.add(neo4jEntityImpl);
                        }
                    }
                }
            }
            return conceptionEntitySet;
        }
    }

    private List<AttributeKind> getContainsSingleValueAttributeKinds(GraphOperationExecutor workingGraphOperationExecutor) {
        return getSingleValueAttributeKinds(null,workingGraphOperationExecutor);
    }

    private List<AttributeKind> getSingleValueAttributeKinds(String attributeKindName) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            return getSingleValueAttributeKinds(attributeKindName,workingGraphOperationExecutor);
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    private List<AttributeKind> getSingleValueAttributeKinds(String attributeKindName,GraphOperationExecutor workingGraphOperationExecutor) {
        Map<String,Object> attributeKindNameFilterMap = null;
        if(attributeKindName != null){
            /*
               MATCH (sourceNode)-[:`TGDA_ConceptionContainsViewKindIs`]->
               (middleNode:`TGDA_AttributesViewKind` {viewKindDataForm: 'SINGLE_VALUE'})-[:`TGDA_ViewContainsAttributeKindIs`]->
               (operationResult:`TGDA_AttributeKind` {name: 'attributeKind02'}) WHERE id(sourceNode) = 1415 RETURN operationResult
               */
            attributeKindNameFilterMap = new HashMap<>();
            attributeKindNameFilterMap.put(Constant._NameProperty,attributeKindName);
        }else{
            /*
                  MATCH (sourceNode)-[:`TGDA_ConceptionContainsViewKindIs`]->
                  (middleNode:`TGDA_AttributesViewKind` {viewKindDataForm: 'SINGLE_VALUE'})-[:`TGDA_ViewContainsAttributeKindIs`]->
                  (operationResult:`TGDA_AttributeKind`) WHERE id(sourceNode) = 1399 RETURN operationResult
               */
        }
        String queryCql = CypherBuilder.match2JumpRelatedNodesFromSpecialStartNodes(
                CypherBuilder.CypherFunctionType.ID, Long.parseLong(conceptionKindUID),
                Constant.AttributesViewKindClass, Constant.Type_AttributesViewKindRelationClass,Direction.TO,singleValueAttributesViewKindTypeFilter,
                Constant.AttributeClass, Constant.AttributesViewKind_AttributeKindRelationClass,Direction.TO,attributeKindNameFilterMap,
                null);
        GetListAttributeKindTransformer getListAttributeKindTransformer = new GetListAttributeKindTransformer(Constant.AttributeClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
        Object attributeKindsRes = workingGraphOperationExecutor.executeWrite(getListAttributeKindTransformer,queryCql);
        return attributeKindsRes != null ? (List<AttributeKind>) attributeKindsRes : null;
    }

    private List<String> filterSingleValueAttributeKindNames(List<AttributeKind> targetAttributeKindList){
        List<String> singleValueAttributeKindNames = new ArrayList<>();
        List<AttributeKind> singleValueAttributesKindsList = getContainsSingleValueAttributeKinds();

        List<String> singleValueAttributesKindNamesList = new ArrayList<>();
        for(AttributeKind currentAttributeKind:singleValueAttributesKindsList){
            singleValueAttributesKindNamesList.add(currentAttributeKind.getAttributeKindName());
        }
        for(AttributeKind currentTargetAttributeKind:targetAttributeKindList){
            String currentAttributeKindName = currentTargetAttributeKind.getAttributeKindName();
            if(singleValueAttributesKindNamesList.contains(currentAttributeKindName)){
                singleValueAttributeKindNames.add(currentAttributeKindName);
            }
        }
        return singleValueAttributeKindNames;
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }

    @Override
    public String getEntityUID() {
        return conceptionKindUID;
    }

    @Override
    public GraphOperationExecutorHelper getGraphOperationExecutorHelper() {
        return graphOperationExecutorHelper;
    }
}
