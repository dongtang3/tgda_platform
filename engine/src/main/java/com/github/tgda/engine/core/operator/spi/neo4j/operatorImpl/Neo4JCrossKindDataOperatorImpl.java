package com.github.tgda.engine.core.operator.spi.neo4j.operatorImpl;

import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetListRelationshipEntityTransformer;
import com.github.tgda.engine.core.operator.CrossKindDataOperator;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.term.RelationshipEntity;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.FilteringItem;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.util.BatchDataOperationUtil;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.payload.EntitiesAttributesRetrieveResult;
import com.github.tgda.engine.core.payload.EntitiesOperationResult;
import com.github.tgda.engine.core.payload.RelationshipEntityValue;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonEntitiesOperationResultImpl;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Neo4JCrossKindDataOperatorImpl implements CrossKindDataOperator {

    private Engine engine;
    private GraphOperationExecutorHelper graphOperationExecutorHelper;
    private static Logger logger = LoggerFactory.getLogger(CrossKindDataOperator.class);

    public Neo4JCrossKindDataOperatorImpl(Engine engine){
        this.engine = engine;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    @Override
    public List<RelationshipEntity> getRelationsOfEntityPair(List<String> conceptionEntityUIDs) throws EngineServiceEntityExploreException {
        /*
        Example:
        https://neo4j.com/labs/apoc/4.1/overview/apoc.algo/apoc.algo.cover/
        */
        if(conceptionEntityUIDs == null || conceptionEntityUIDs.size() < 2){
            logger.error("At least two conception entity UID is required");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("At least two conception entity UID is required");
            throw e;
        }

        String cypherProcedureString = "MATCH (targetNodes) WHERE id(targetNodes) IN " + conceptionEntityUIDs.toString()+"\n"+
                "with collect(targetNodes) as nodes\n" +
                "CALL apoc.algo.cover(nodes)\n" +
                "YIELD rel\n" +
                "RETURN startNode(rel) as startNode, rel as operationResult, endNode(rel) as endNode;";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(null,
                    this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor(),true);
            Object relationEntityList = workingGraphOperationExecutor.executeRead(getListRelationshipEntityTransformer,cypherProcedureString);
            return relationEntityList != null ? (List<RelationshipEntity>)relationEntityList : null;
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public long removeRelationsOfEntityPair(List<String> conceptionEntityPairUIDs, String relationKind) throws EngineServiceEntityExploreException {
        /*
        Example:
        https://neo4j.com/labs/apoc/4.1/overview/apoc.algo/apoc.algo.cover/
        */
        if(conceptionEntityPairUIDs == null || conceptionEntityPairUIDs.size() < 2){
            logger.error("At least two conception entity UID is required");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("At least two conception entity UID is required");
            throw e;
        }

        String cypherProcedureString = "MATCH (targetNodes) WHERE id(targetNodes) IN " + conceptionEntityPairUIDs.toString()+"\n"+
                "with collect(targetNodes) as nodes\n" +
                "CALL apoc.algo.cover(nodes)\n" +
                "YIELD rel\n" +
                "RETURN startNode(rel) as startNode, rel as operationResult, endNode(rel) as endNode;";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(null,
                    this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor(),true);
            Object relationEntityList = workingGraphOperationExecutor.executeRead(getListRelationshipEntityTransformer,cypherProcedureString);
            if(relationEntityList != null){
                List<RelationshipEntity> resultRelationEntities = (List<RelationshipEntity>)relationEntityList;
                List<String> relationsForDeleteList = new ArrayList<>();
                for(RelationshipEntity currentRelationshipEntity:resultRelationEntities){
                    if(relationKind != null){
                        if(currentRelationshipEntity.getRelationTypeName().equals(relationKind)){
                            relationsForDeleteList.add(currentRelationshipEntity.getRelationshipEntityUID());
                        }

                    }else{
                        relationsForDeleteList.add(currentRelationshipEntity.getRelationshipEntityUID());
                    }
                }
                if(resultRelationEntities.size() > 0){
                    cypherProcedureString = "MATCH ()-[r]->() WHERE id(r) IN "+relationsForDeleteList.toString()+
                            " DELETE r";
                    logger.debug("Generated Cypher Statement: {}", cypherProcedureString);
                    workingGraphOperationExecutor.executeWrite(new DataTransformer() {
                        @Override
                        public Object transformResult(Result result) {
                            return null;
                        }
                    },cypherProcedureString);
                }
                return relationsForDeleteList.size();
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return 0;
    }

    @Override
    public List<RelationshipEntity> getRelationEntitiesByUIDs(List<String> relationEntityUIDs) throws EngineServiceEntityExploreException {
        if(relationEntityUIDs == null || relationEntityUIDs.size() < 1){
            logger.error("At least one relation entity UID is required");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("At least one relation entity UID is required");
            throw e;
        }

        String cypherProcedureString = "MATCH (source)-[r]->(target)\n" +
                "WHERE id(r) IN "+relationEntityUIDs.toString()+"\n" +
                "RETURN DISTINCT r as operationResult,source as sourceNode, target as targetNode";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(null,
                    this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor(),false);
            Object relationEntityList = workingGraphOperationExecutor.executeRead(getListRelationshipEntityTransformer,cypherProcedureString);
            return relationEntityList != null ? (List<RelationshipEntity>)relationEntityList : null;
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<Entity> getConceptionEntitiesByUIDs(List<String> conceptionEntityUIDs) throws EngineServiceEntityExploreException {
        if(conceptionEntityUIDs == null || conceptionEntityUIDs.size() < 1){
            logger.error("At least one conception entity UID is required");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("At least one conception entity UID is required");
            throw e;
        }

        String cypherProcedureString = "MATCH (targetNodes) WHERE id(targetNodes) IN " + conceptionEntityUIDs.toString()+"\n"+
                "RETURN DISTINCT targetNodes as operationResult";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(null,
                    this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object conceptionEntityList = workingGraphOperationExecutor.executeRead(getListEntityTransformer,cypherProcedureString);
            return conceptionEntityList != null ? (List<Entity>)conceptionEntityList : null;
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Double computeEntityPairTopologySimilarity(String conceptionEntityAUID, String conceptionEntityBUID,
                                                                TopologySimilarityComputeAlgorithm topologySimilarityComputeAlgorithm,
                                                                TopologySimilarityComputeDirection topologySimilarityComputeDirection,
                                                                String relationKindForCompute) throws EngineServiceEntityExploreException, EngineServiceRuntimeException {
        if(conceptionEntityAUID == null || conceptionEntityBUID == null){
            logger.error("Both conception entityA and conception entityB UID are required");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("Both conception entityA and conception entityB UID are required");
            throw e;
        }

        String neo4jTopologySimilarityComputeAlgorithm = "";
        switch (topologySimilarityComputeAlgorithm){
            case AdamicAdar:
                /*
                Example:
                https://neo4j.com/docs/graph-data-science/current/alpha-algorithms/adamic-adar/
                */
                neo4jTopologySimilarityComputeAlgorithm = "gds.alpha.linkprediction.adamicAdar";
                break;
            case CommonNeighbors:
                /*
                Example:
                https://neo4j.com/docs/graph-data-science/current/alpha-algorithms/common-neighbors/
                */
                neo4jTopologySimilarityComputeAlgorithm = "gds.alpha.linkprediction.commonNeighbors";
                break;
            case ResourceAllocation:
                /*
                Example:
                https://neo4j.com/docs/graph-data-science/current/alpha-algorithms/resource-allocation/
                */
                neo4jTopologySimilarityComputeAlgorithm = "gds.alpha.linkprediction.resourceAllocation";
                break;
            case TotalNeighbors:
                /*
                Example:
                https://neo4j.com/docs/graph-data-science/current/alpha-algorithms/total-neighbors/
                */
                neo4jTopologySimilarityComputeAlgorithm = "gds.alpha.linkprediction.totalNeighbors";
                break;
            case PreferentialAttachment:
                /*
                Example:
                https://neo4j.com/docs/graph-data-science/current/alpha-algorithms/preferential-attachment/
                */
                neo4jTopologySimilarityComputeAlgorithm = "gds.alpha.linkprediction.preferentialAttachment";
        }
        String relationshipQueryString = relationKindForCompute != null ?
                "relationshipQuery: '"+relationKindForCompute+"'," : "";

        String directionQueryString = topologySimilarityComputeDirection != null ?
                "direction: '"+topologySimilarityComputeDirection+"'":
                "direction: '"+TopologySimilarityComputeDirection.BOTH+"'";

        String cypherProcedureString = "MATCH (nodeA) WHERE id(nodeA)= " + conceptionEntityAUID+"\n" +
                        "MATCH (nodeB) WHERE id(nodeB)= " + conceptionEntityBUID+"\n" +
                        "RETURN "+neo4jTopologySimilarityComputeAlgorithm+"(nodeA, nodeB, {" +
                relationshipQueryString + directionQueryString+
                        "}) AS score";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        DataTransformer<Double> dataTransformer = new DataTransformer() {
            @Override
            public Object transformResult(Result result) {
                while(result.hasNext()){
                    Record nodeRecord = result.next();
                    double score = nodeRecord.get("score").asNumber().doubleValue();
                    return score;
                }
                return null;
            }
        };
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            Object queryResponse = workingGraphOperationExecutor.executeRead(dataTransformer,cypherProcedureString);
            return queryResponse != null ? (Double)queryResponse : null;
        } catch(org.neo4j.driver.exceptions.ClientException e){
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage(e.getMessage());
            throw e1;
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<EntityValue> getSingleValueEntityAttributesByUIDs(List<String> conceptionEntityUIDs, List<String> attributeNames) throws EngineServiceEntityExploreException {
        if(conceptionEntityUIDs == null || conceptionEntityUIDs.size() < 1){
            logger.error("At least one conception entity UID is required");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("At least one conception entity UID is required");
            throw e;
        }
        if(attributeNames == null || attributeNames.size() < 1){
            logger.error("At least one attribute name is required");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("At least one attribute name is required");
            throw e;
        }

        String cypherProcedureString = CypherBuilder.matchAttributesWithNodeIDs(conceptionEntityUIDs,attributeNames);
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListEntityValueTransformer getListEntityValueTransformer = new GetListEntityValueTransformer(attributeNames);
            getListEntityValueTransformer.setUseIDMatchLogic(true);
            Object resEntityRes = workingGraphOperationExecutor.executeRead(getListEntityValueTransformer,cypherProcedureString);
            if(resEntityRes != null){
                List<EntityValue> resultEntitiesValues = (List<EntityValue>)resEntityRes;
               return resultEntitiesValues;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public List<RelationshipEntityValue> getSingleValueRelationshipEntityAttributesByUIDs(List<String> relationEntityUIDs, List<String> attributeNames) throws EngineServiceEntityExploreException {
        if(relationEntityUIDs == null || relationEntityUIDs.size() < 1){
            logger.error("At least one relation entity UID is required");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("At least one relation entity UID is required");
            throw e;
        }
        if(attributeNames == null || attributeNames.size() < 1){
            logger.error("At least one attribute name is required");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("At least one attribute name is required");
            throw e;
        }

        String cypherProcedureString = CypherBuilder.matchRelationsWithUIDs(relationEntityUIDs);
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListRelationshipEntityValueTransformer getListRelationshipEntityValueTransformer =
                    new GetListRelationshipEntityValueTransformer(null,attributeNames);
            Object queryRes = workingGraphOperationExecutor.executeRead(getListRelationshipEntityValueTransformer,cypherProcedureString);
            if(queryRes != null){
                List<RelationshipEntityValue> resultEntitiesValues = (List<RelationshipEntityValue>)queryRes;
                return resultEntitiesValues;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public EntitiesOperationResult fuseConceptionKindsAttributes(String fuseSourceKindName, String sourceKindMatchAttributeName,
                  List<String> attributesForFusion, String fuseTargetKindName, String targetKindMatchAttributeName) throws EngineServiceEntityExploreException {
        CommonEntitiesOperationResultImpl commonEntitiesOperationResultImpl = new CommonEntitiesOperationResultImpl();

        QueryParameters recordNumberSettingQueryParameters = new QueryParameters();
        recordNumberSettingQueryParameters.setResultNumber(100000000);

        ConceptionKind sourceNeo4JConceptionKindImpl = this.engine.getConceptionKind(fuseSourceKindName);
        attributesForFusion.add(sourceKindMatchAttributeName);
        EntitiesAttributesRetrieveResult sourceRetrieveResult = sourceNeo4JConceptionKindImpl.getSingleValueEntityAttributesByAttributeNames(attributesForFusion,recordNumberSettingQueryParameters);

        ConceptionKind targetNeo4JConceptionKindImpl = this.engine.getConceptionKind(fuseTargetKindName);
        List<String> targetKindMatchAttList = new ArrayList<>();
        targetKindMatchAttList.add(targetKindMatchAttributeName);
        EntitiesAttributesRetrieveResult targetRetrieveResult = targetNeo4JConceptionKindImpl.getSingleValueEntityAttributesByAttributeNames(targetKindMatchAttList,recordNumberSettingQueryParameters);

        List<EntityValue> targetEntityValues = targetRetrieveResult.getEntityValues();
        HashMultimap<Object,String> targetEntitiesValueMap = HashMultimap.create();
        for(EntityValue currentEntityValue : targetEntityValues){
            Map<String, Object> attributesValueMap = currentEntityValue.getEntityAttributesValue();
            String entityUID = currentEntityValue.getEntityUID();
            if(attributesValueMap.containsKey(targetKindMatchAttributeName)){
                targetEntitiesValueMap.put(attributesValueMap.get(targetKindMatchAttributeName),entityUID);
            }
        }

        List<Map<String,Object>> targetConceptionKindUpdateDataList = new ArrayList<>();
        String targetEntityUIDKey = "TGDA_CONCEPTION_ENTITY_UID";

        List<EntityValue> sourceEntityValues = sourceRetrieveResult.getEntityValues();
        for(EntityValue currentEntityValue :sourceEntityValues){
            Map<String, Object> attributesValue = currentEntityValue.getEntityAttributesValue();
            if(attributesValue.containsKey(sourceKindMatchAttributeName)){
                Object matchValue = attributesValue.get(sourceKindMatchAttributeName);
                Set<String> targetEntityUIDs = targetEntitiesValueMap.get(matchValue);
                if(targetEntityUIDs != null && targetEntityUIDs.size()>0){
                    attributesValue.remove(sourceKindMatchAttributeName);
                    for(String currentTargetEntityUID:targetEntityUIDs){
                        Map<String,Object> currentEntityAttributesMap = new HashMap<>();
                        currentEntityAttributesMap.putAll(attributesValue);
                        currentEntityAttributesMap.put(targetEntityUIDKey,currentTargetEntityUID);
                        targetConceptionKindUpdateDataList.add(currentEntityAttributesMap);
                        commonEntitiesOperationResultImpl.getSuccessEntityUIDs().add(currentTargetEntityUID);
                    }
                }
            }
        }

        Map<String,Object> updateResult = BatchDataOperationUtil.batchAddNewOrUpdateEntityAttributes
                (targetEntityUIDKey,targetConceptionKindUpdateDataList, BatchDataOperationUtil.CPUUsageRate.Middle);
        long totalSuccessCount = 0;
        Set<String> resultKeySet = updateResult.keySet();
        for(String currentKey:resultKeySet){
            if(!currentKey.equals("StartTime")&!currentKey.equals("FinishTime")){
                long currentSuccessfulCount = (Long)updateResult.get(currentKey);
                totalSuccessCount = totalSuccessCount + currentSuccessfulCount;
            }
        }
        commonEntitiesOperationResultImpl.getOperationStatistics().setSuccessItemsCount(totalSuccessCount);
        commonEntitiesOperationResultImpl.getOperationStatistics().
                setOperationSummary("fuseConceptionKindsAttributes operation for conceptionKind "+fuseTargetKindName+" success.");
        commonEntitiesOperationResultImpl.finishEntitiesOperation();
        return commonEntitiesOperationResultImpl;
    }

    @Override
    public EntitiesOperationResult joinEntitiesToConceptionKinds(String sourceKindName, AttributesParameters attributesParameters, String[] newKindNames) throws EngineServiceEntityExploreException {
        CommonEntitiesOperationResultImpl commonEntitiesOperationResultImpl = new CommonEntitiesOperationResultImpl();
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setDistinctMode(false);
        queryParameters.setResultNumber(100);
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

        String entityQueryCQL = CypherBuilder.matchNodesWithQueryParameters(sourceKindName,queryParameters, CypherBuilder.CypherFunctionType.ID);
        entityQueryCQL = entityQueryCQL.replace("RETURN id("+CypherBuilder.operationResultName+") LIMIT 100","");

        String labelModifyText = CypherBuilder.operationResultName;
        for(String currentLabel:newKindNames){
            labelModifyText = labelModifyText+ ":"+currentLabel;
        }
        entityQueryCQL= entityQueryCQL+ "SET "+labelModifyText+" RETURN id("+CypherBuilder.operationResultName+")";
        logger.debug("Generated Cypher Statement: {}", entityQueryCQL);

        List<String> resultEntityUIDsList = null;
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListEntityUIDTransformer getListEntityUIDTransformer = new GetListEntityUIDTransformer();
            Object queryRes = workingGraphOperationExecutor.executeWrite(getListEntityUIDTransformer,entityQueryCQL);
            if(queryRes != null){
                resultEntityUIDsList = (List<String>)queryRes;
                commonEntitiesOperationResultImpl.getSuccessEntityUIDs().addAll(resultEntityUIDsList);
                commonEntitiesOperationResultImpl.getOperationStatistics().setSuccessItemsCount(resultEntityUIDsList.size());
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }

        commonEntitiesOperationResultImpl.getOperationStatistics().
                setOperationSummary("joinEntitiesToConceptionKinds operation for conceptionKind "+sourceKindName+" success.");
        commonEntitiesOperationResultImpl.finishEntitiesOperation();
        return commonEntitiesOperationResultImpl;
    }

    @Override
    public EntitiesOperationResult retreatEntitiesFromConceptionKind(String sourceKindName, AttributesParameters attributesParameters, String kindName) throws EngineServiceEntityExploreException {
        CommonEntitiesOperationResultImpl commonEntitiesOperationResultImpl = new CommonEntitiesOperationResultImpl();
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setDistinctMode(false);
        queryParameters.setResultNumber(100);
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

        String entityQueryCQL = CypherBuilder.matchNodesWithQueryParameters(sourceKindName,queryParameters, CypherBuilder.CypherFunctionType.ID);
        entityQueryCQL = entityQueryCQL.replace("RETURN id("+CypherBuilder.operationResultName+") LIMIT 100","");

        entityQueryCQL= entityQueryCQL+ "REMOVE "+CypherBuilder.operationResultName+":"+kindName+" RETURN id("+CypherBuilder.operationResultName+")";
        logger.debug("Generated Cypher Statement: {}", entityQueryCQL);

        List<String> resultEntityUIDsList = null;
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListEntityUIDTransformer getListEntityUIDTransformer = new GetListEntityUIDTransformer();
            Object queryRes = workingGraphOperationExecutor.executeWrite(getListEntityUIDTransformer,entityQueryCQL);
            if(queryRes != null){
                resultEntityUIDsList = (List<String>)queryRes;
                commonEntitiesOperationResultImpl.getSuccessEntityUIDs().addAll(resultEntityUIDsList);
                commonEntitiesOperationResultImpl.getOperationStatistics().setSuccessItemsCount(resultEntityUIDsList.size());
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }

        commonEntitiesOperationResultImpl.getOperationStatistics().
                setOperationSummary("retreatEntitiesFromConceptionKind operation for conceptionKind "+sourceKindName+" success.");
        commonEntitiesOperationResultImpl.finishEntitiesOperation();
        return commonEntitiesOperationResultImpl;
    }

    @Override
    public EntitiesOperationResult mergeEntitiesToConceptionKind(String sourceKindName, AttributesParameters attributesParameters, String relationKindName, Direction relationDirection, String targetConceptionKindName) throws EngineServiceEntityExploreException {
        if(relationDirection == null || Direction.TWO_WAY.equals(relationDirection)){
            logger.error("relationDirection must set and can't be TWO_WAY");
            EngineServiceEntityExploreException exception = new EngineServiceEntityExploreException();
            exception.setCauseMessage("relationDirection must set and can't be TWO_WAY");
            throw exception;
        }
        CommonEntitiesOperationResultImpl commonEntitiesOperationResultImpl = new CommonEntitiesOperationResultImpl();
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setDistinctMode(false);
        queryParameters.setResultNumber(100);
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

        String entityQueryCQL = CypherBuilder.matchNodesWithQueryParameters(sourceKindName,queryParameters, CypherBuilder.CypherFunctionType.ID);
        entityQueryCQL = entityQueryCQL.replace("RETURN id("+CypherBuilder.operationResultName+") LIMIT 100","");

        String relationTypeMatchingStr = relationKindName != null ? "r:"+relationKindName:"r";
        String relationPathMatchingStr = "-[r]-";
        if(relationDirection == null){
            relationPathMatchingStr = "-["+relationTypeMatchingStr+"]-";
        }else{
            switch(relationDirection){
                case FROM: relationPathMatchingStr = "-["+relationTypeMatchingStr+"]->"; break;
                case TO: relationPathMatchingStr = "<-["+relationTypeMatchingStr+"]-"; break;
                case TWO_WAY: relationPathMatchingStr = "-["+relationTypeMatchingStr+"]-";
            }
        }
        String targetKindMatchingStr = targetConceptionKindName != null ? "target:"+targetConceptionKindName : "target";
        String cypherProcedureString = "MATCH (source)"+relationPathMatchingStr+"("+targetKindMatchingStr+") WHERE id(source) IN sourceUIDs RETURN r AS "+CypherBuilder.operationResultName;
        entityQueryCQL = entityQueryCQL+" \n"+
                "WITH collect(id("+CypherBuilder.operationResultName+")) AS sourceUIDs"+" \n"+
                cypherProcedureString;
        logger.debug("Generated Cypher Statement: {}", entityQueryCQL);
        List<RelationshipEntityValue> resultEntitiesValues = null;
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListRelationshipEntityValueTransformer getListRelationshipEntityValueTransformer =
                    new GetListRelationshipEntityValueTransformer(relationKindName,null);
            Object queryRes = workingGraphOperationExecutor.executeRead(getListRelationshipEntityValueTransformer,entityQueryCQL);
            if(queryRes != null){
                resultEntitiesValues = (List<RelationshipEntityValue>)queryRes;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }

        int degreeOfParallelism = BatchDataOperationUtil.calculateRuntimeCPUCoresByUsageRate(resultEntitiesValues.size(),BatchDataOperationUtil.CPUUsageRate.High);
        int singlePartitionSize = (resultEntitiesValues.size()/degreeOfParallelism)+1;
        List<List<RelationshipEntityValue>> rsList = Lists.partition(resultEntitiesValues, singlePartitionSize);
        Map<String,Object> threadReturnDataMap = new Hashtable<>();

        ExecutorService executor = Executors.newFixedThreadPool(rsList.size());
        for(List<RelationshipEntityValue> currentRelationshipEntityValueList:rsList){
            MergeEntitiesToConceptionKindThread mergeEntitiesToConceptionKindThread =  new MergeEntitiesToConceptionKindThread(currentRelationshipEntityValueList,relationDirection,threadReturnDataMap);
            executor.execute(mergeEntitiesToConceptionKindThread);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long wholeSuccessCount = 0;
        for (Map.Entry<String, Object> entry : threadReturnDataMap.entrySet()) {

            String key = entry.getKey();
            Object value = entry.getValue();
            if(key.startsWith("SUCCESSCOUNT_")){
                Integer threadSuccessCount = (Integer)value;
                wholeSuccessCount = wholeSuccessCount +threadSuccessCount.longValue();
            }
            if(key.startsWith("SUCCESSUIDS_")){
                List<String> successUIDs = (List<String>)value;
                commonEntitiesOperationResultImpl.getSuccessEntityUIDs().addAll(successUIDs);
            }
        }
        commonEntitiesOperationResultImpl.getOperationStatistics().setSuccessItemsCount(wholeSuccessCount);
        commonEntitiesOperationResultImpl.getOperationStatistics().
                setOperationSummary("mergeEntitiesToConceptionKind operation success.");
        commonEntitiesOperationResultImpl.finishEntitiesOperation();
        return commonEntitiesOperationResultImpl;
    }

    @Override
    public List<RelationshipEntityValue> getRelationAttributesByEntitiesRelation(List<String> conceptionEntityUIDs, String relationKind, List<String> returnedAttributeList,Direction relationDirection, String targetConceptionKindName) throws EngineServiceEntityExploreException {
        if(conceptionEntityUIDs == null || conceptionEntityUIDs.size() < 1){
            logger.error("At least one conception entity UID is required");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("At least one conception entity UID is required");
            throw e;
        }
        String relationTypeMatchingStr = relationKind != null ? "r:"+relationKind:"r";
        String relationPathMatchingStr = "-[r]-";
        if(relationDirection == null){
            relationPathMatchingStr = "-["+relationTypeMatchingStr+"]-";
        }else{
            switch(relationDirection){
                case FROM: relationPathMatchingStr = "-["+relationTypeMatchingStr+"]->"; break;
                case TO: relationPathMatchingStr = "<-["+relationTypeMatchingStr+"]-"; break;
                case TWO_WAY: relationPathMatchingStr = "-["+relationTypeMatchingStr+"]-";
            }
        }
        String targetKindMatchingStr = targetConceptionKindName != null ? "target:"+targetConceptionKindName : "target";
        String cypherProcedureString = "MATCH (source)"+relationPathMatchingStr+"("+targetKindMatchingStr+") WHERE id(source) IN "+ conceptionEntityUIDs.toString()+" RETURN r AS "+CypherBuilder.operationResultName;
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListRelationshipEntityValueTransformer getListRelationshipEntityValueTransformer =
                    new GetListRelationshipEntityValueTransformer(relationKind,returnedAttributeList);
            Object queryRes = workingGraphOperationExecutor.executeRead(getListRelationshipEntityValueTransformer,cypherProcedureString);
            if(queryRes != null){
                List<RelationshipEntityValue> resultEntitiesValues = (List<RelationshipEntityValue>)queryRes;
                return resultEntitiesValues;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public List<RelationshipEntity> extractRelationsFromBridgeConceptionEntities(String sourceKindName,String targetKindName, String bridgeKindName,
                                                                             AttributesParameters attributesParameters,String sourceToBridgeRelationKindName,
                                                                             String bridgeToTargetRelationKindName, String sourceToTargetRelationKindName,
                                                                             boolean allowRepeat) throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        if(sourceToTargetRelationKindName == null){
            logger.error("Param sourceToTargetRelationKindName in method createRelationEntitiesFromBridgeConceptionEntities is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param sourceToTargetRelationKindName in method createRelationEntitiesFromBridgeConceptionEntities is required");
            throw e1;
        }

        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setDistinctMode(false);
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
        String bridgeEntityQueryCQL = CypherBuilder.matchNodesWithQueryParameters(bridgeKindName,queryParameters, CypherBuilder.CypherFunctionType.ID);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListEntityUIDTransformer getListEntityUIDTransformer = new GetListEntityUIDTransformer();
            Object queryRes = workingGraphOperationExecutor.executeWrite(getListEntityUIDTransformer,bridgeEntityQueryCQL);
            if(queryRes != null){
                List<String> resultEntityUIDsList = (List<String>)queryRes;

                String sourceNodesPart = sourceKindName == null ? "sourceNodes":"sourceNodes:"+sourceKindName;
                String targetNodesPart = targetKindName == null ? "targetNodes":"targetNodes:"+targetKindName;
                String sourceToBridgeRelPart = sourceToBridgeRelationKindName == null ? "r1":"r1:"+sourceToBridgeRelationKindName;
                String bridgeToTargetRelPart = bridgeToTargetRelationKindName == null ? "r2":"r2:"+bridgeToTargetRelationKindName;

                String createRelAccordingToAllowRepeatPart = allowRepeat ?
                        "CREATE (sourceNodes)-[sToTRel:"+sourceToTargetRelationKindName+"]->(targetNodes)\n " +
                                "WITH sourceNodes,targetNodes\n" +
                                "MATCH (sourceNodes)-[sToTRel:"+sourceToTargetRelationKindName+"]->(targetNodes) RETURN sToTRel AS operationResult" :
                        "MERGE (sourceNodes)-[sToTRel:"+sourceToTargetRelationKindName+"]->(targetNodes) RETURN sToTRel AS operationResult";
                String creatRelationCQL = "MATCH ("+sourceNodesPart+")-["+sourceToBridgeRelPart+"]->(middleNodes)-["+bridgeToTargetRelPart+"]->("+targetNodesPart+") WHERE id(middleNodes) IN "+resultEntityUIDsList.toString()+" \n" +
                        "WITH sourceNodes,targetNodes\n" +
                        createRelAccordingToAllowRepeatPart;
                logger.debug("Generated Cypher Statement: {}", creatRelationCQL);

                GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(sourceToTargetRelationKindName,workingGraphOperationExecutor,false);
                queryRes = workingGraphOperationExecutor.executeWrite(getListRelationshipEntityTransformer,creatRelationCQL);
                if(queryRes != null){
                    return (List<RelationshipEntity>)queryRes;
                }
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public List<RelationshipEntity> extractRelationsFromBridgeConceptionEntities(String sourceKindName, String targetKindName, List<String> bridgeEntityUIDs,
                                                                             String sourceToBridgeRelationKindName, String bridgeToTargetRelationKindName,
                                                                             String sourceToTargetRelationKindName, boolean allowRepeat) throws EngineServiceRuntimeException {
        if(sourceToTargetRelationKindName == null){
            logger.error("Param sourceToTargetRelationKindName in method createRelationEntitiesFromBridgeConceptionEntities is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param sourceToTargetRelationKindName in method createRelationEntitiesFromBridgeConceptionEntities is required");
            throw e1;
        }
        if(bridgeEntityUIDs == null || bridgeEntityUIDs.size() == 0){
            logger.error("At lease one bridge conceptionEntityUID is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("At lease one bridge conceptionEntityUID is required");
            throw e1;
        }

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            String sourceNodesPart = sourceKindName == null ? "sourceNodes":"sourceNodes:"+sourceKindName;
            String targetNodesPart = targetKindName == null ? "targetNodes":"targetNodes:"+targetKindName;
            String sourceToBridgeRelPart = sourceToBridgeRelationKindName == null ? "r1":"r1:"+sourceToBridgeRelationKindName;
            String bridgeToTargetRelPart = bridgeToTargetRelationKindName == null ? "r2":"r2:"+bridgeToTargetRelationKindName;

            String createRelAccordingToAllowRepeatPart = allowRepeat ?
                    "CREATE (sourceNodes)-[sToTRel:"+sourceToTargetRelationKindName+"]->(targetNodes)\n " +
                            "WITH sourceNodes,targetNodes\n" +
                            "MATCH (sourceNodes)-[sToTRel:"+sourceToTargetRelationKindName+"]->(targetNodes) RETURN sToTRel AS operationResult" :
                    "MERGE (sourceNodes)-[sToTRel:"+sourceToTargetRelationKindName+"]->(targetNodes) RETURN sToTRel AS operationResult";
            String creatRelationCQL = "MATCH ("+sourceNodesPart+")-["+sourceToBridgeRelPart+"]->(middleNodes)-["+bridgeToTargetRelPart+"]->("+targetNodesPart+") WHERE id(middleNodes) IN "+bridgeEntityUIDs.toString()+" \n" +
                    "WITH sourceNodes,targetNodes\n" +
                    createRelAccordingToAllowRepeatPart;
            logger.debug("Generated Cypher Statement: {}", creatRelationCQL);

            GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(sourceToTargetRelationKindName,workingGraphOperationExecutor,false);
            Object queryRes = workingGraphOperationExecutor.executeWrite(getListRelationshipEntityTransformer,creatRelationCQL);
            if(queryRes != null){
                return (List<RelationshipEntity>)queryRes;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public List<Entity> extractIntermediateConceptionEntitiesFromRelations(List<String> relationEntityUIDs, String intermediateConceptionKindName, String fromRelationKind, String toRelationKind) throws EngineServiceRuntimeException {
        //https://neo4j.com/docs/apoc/current/overview/apoc.refactor/apoc.refactor.extractNode/
        if(intermediateConceptionKindName == null){
            logger.error("Param intermediateConceptionKindName in method extractIntermediateConceptionEntitiesFromRelations is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param intermediateConceptionKindName in method extractIntermediateConceptionEntitiesFromRelations is required");
            throw e1;
        }
        if(fromRelationKind == null){
            logger.error("Param fromRelationKind in method extractIntermediateConceptionEntitiesFromRelations is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param fromRelationKind in method extractIntermediateConceptionEntitiesFromRelations is required");
            throw e1;
        }
        if(toRelationKind == null){
            logger.error("Param toRelationKind in method extractIntermediateConceptionEntitiesFromRelations is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param toRelationKind in method extractIntermediateConceptionEntitiesFromRelations is required");
            throw e1;
        }
        if(relationEntityUIDs == null || relationEntityUIDs.size() == 0){
            logger.error("At lease one relationEntityUID is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("At lease one relationEntityUID is required");
            throw e1;
        }

        String cypherProcedureString = "MATCH (source)-[rel]->(target)\n" +
                "WHERE id(rel) IN "+relationEntityUIDs.toString()+"\n" +
                "WITH collect(rel) AS rels\n" +
                "CALL apoc.refactor.extractNode(rels,['"+intermediateConceptionKindName+"'],'"+fromRelationKind+"','"+toRelationKind+"')\n" +
                "YIELD output\n" +
                "RETURN output AS operationResult;";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(intermediateConceptionKindName,
                    this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object conceptionEntityList = workingGraphOperationExecutor.executeWrite(getListEntityTransformer,cypherProcedureString);
            return conceptionEntityList != null ? (List<Entity>)conceptionEntityList : null;
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<RelationshipEntity> collapseConceptionEntities(List<String> conceptionEntityUIDs, String relationKindName) throws EngineServiceRuntimeException {
        //https://neo4j.com/docs/apoc/current/overview/apoc.refactor/apoc.refactor.collapseNode/
        if(conceptionEntityUIDs == null || conceptionEntityUIDs.size() == 0){
            logger.error("At lease one conceptionEntityUID is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("At lease one conceptionEntityUID is required");
            throw e1;
        }
        if(relationKindName == null){
            logger.error("Param relationKindName in method collapseConceptionEntities is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param relationKindName in method collapseConceptionEntities is required");
            throw e1;
        }

        String cypherProcedureString = "MATCH (targetNodes) WHERE id(targetNodes) IN " + conceptionEntityUIDs.toString()+"\n"+
                "CALL apoc.refactor.collapseNode(targetNodes,'"+relationKindName+"')\n" +
                "YIELD output\n" +
                "RETURN output AS operationResult;";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(relationKindName,workingGraphOperationExecutor,false);
            Object queryRes = workingGraphOperationExecutor.executeWrite(getListRelationshipEntityTransformer,cypherProcedureString);
            if(queryRes != null){
                return (List<RelationshipEntity>)queryRes;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public List<RelationshipEntity> changeEntitiesRelationKind(List<String> relationEntityUIDs, String newRelationKind) throws EngineServiceRuntimeException {
        //https://neo4j.com/docs/apoc/current/overview/apoc.refactor/apoc.refactor.setType/
        if(relationEntityUIDs == null || relationEntityUIDs.size() == 0){
            logger.error("At lease one relationEntityUID is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("At lease one relationEntityUID is required");
            throw e1;
        }
        if(newRelationKind == null){
            logger.error("Param newRelationKind in method changeEntitiesRelationKind is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param newRelationKind in method changeEntitiesRelationKind is required");
            throw e1;
        }

        String cypherProcedureString = "MATCH (source)-[rel]->(target)\n" +
                "WHERE id(rel) IN "+relationEntityUIDs.toString()+"\n" +
                "CALL apoc.refactor.setType(rel, '"+newRelationKind+"')\n" +
                "YIELD output\n" +
                "RETURN output AS operationResult;";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(newRelationKind,workingGraphOperationExecutor,false);
            Object queryRes = workingGraphOperationExecutor.executeWrite(getListRelationshipEntityTransformer,cypherProcedureString);
            if(queryRes != null){
                return (List<RelationshipEntity>)queryRes;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public List<Entity> changeEntitiesConceptionKind(List<String> conceptionEntityUIDs, String oldConceptionKind,String newConceptionKind) throws EngineServiceRuntimeException {
        if(conceptionEntityUIDs == null || conceptionEntityUIDs.size() == 0){
            logger.error("At lease one relationEntityUID is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("At lease one relationEntityUID is required");
            throw e1;
        }
        if(oldConceptionKind == null){
            logger.error("Param oldConceptionKind in method changeEntitiesConceptionKind is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param oldConceptionKind in method changeEntitiesConceptionKind is required");
            throw e1;
        }
        if(newConceptionKind == null){
            logger.error("Param newConceptionKind in method changeEntitiesConceptionKind is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param newConceptionKind in method changeEntitiesConceptionKind is required");
            throw e1;
        }

        String cypherProcedureString = "MATCH (targetNodes:"+oldConceptionKind+") WHERE id(targetNodes) IN " + conceptionEntityUIDs.toString()+"\n"+
                "SET targetNodes:"+newConceptionKind +"\n"+
                "REMOVE targetNodes:"+oldConceptionKind +"\n"+
                "RETURN count(targetNodes) AS operationResult;";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        String cypherProcedureString2 = "MATCH (targetNodes:"+newConceptionKind+") WHERE id(targetNodes) IN " + conceptionEntityUIDs.toString()+"\n"+
                "RETURN targetNodes AS operationResult;";

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            workingGraphOperationExecutor.executeWrite(new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    return null;
                }
            }, cypherProcedureString);

            GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(newConceptionKind,
                    this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object conceptionEntityList = workingGraphOperationExecutor.executeRead(getListEntityTransformer,cypherProcedureString2);
            return conceptionEntityList != null ? (List<Entity>)conceptionEntityList : null;
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<RelationshipEntity> invertRelationEntitiesDirection(List<String> relationEntityUIDs) throws EngineServiceRuntimeException {
        https://neo4j.com/docs/apoc/current/overview/apoc.refactor/apoc.refactor.invert/
        if(relationEntityUIDs == null || relationEntityUIDs.size() == 0){
            logger.error("At lease one relationEntityUID is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("At lease one relationEntityUID is required");
            throw e1;
        }

        String cypherProcedureString = "MATCH (source)-[rel]->(target)\n" +
                "WHERE id(rel) IN "+relationEntityUIDs.toString()+"\n" +
                "CALL apoc.refactor.invert(rel)\n"+
                "YIELD output\n" +
                "RETURN output AS operationResult;";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(null,workingGraphOperationExecutor,false);
            Object queryRes = workingGraphOperationExecutor.executeWrite(getListRelationshipEntityTransformer,cypherProcedureString);
            if(queryRes != null){
                return (List<RelationshipEntity>)queryRes;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public List<RelationshipEntity> redirectRelationsToNewEntity(List<String> relationEntityUIDs, String targetEntityUID, Direction relationDirection) throws EngineServiceRuntimeException {
        //https://neo4j.com/docs/apoc/current/overview/apoc.refactor/apoc.refactor.from/
        //https://neo4j.com/docs/apoc/current/overview/apoc.refactor/apoc.refactor.to/
        if(relationEntityUIDs == null || relationEntityUIDs.size() == 0){
            logger.error("At lease one relationEntityUID is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("At lease one relationEntityUID is required");
            throw e1;
        }
        if(targetEntityUID == null){
            logger.error("Param targetEntityUID in method redirectRelationsToNewEntity is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param targetEntityUID in method redirectRelationsToNewEntity is required");
            throw e1;
        }
        if(relationDirection == null){
            logger.error("Param relationDirection in method redirectRelationsToNewEntity is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param relationDirection in method redirectRelationsToNewEntity is required");
            throw e1;
        }
        if(relationDirection.equals(Direction.TWO_WAY)){
            logger.error("Param relationDirection in method redirectRelationsToNewEntity can't be TWO_WAY direction");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param relationDirection in method redirectRelationsToNewEntity can't be TWO_WAY direction");
            throw e1;
        }

        String redirectLogicCQL="";
        switch(relationDirection) {
            case FROM ->
                    redirectLogicCQL = "CALL apoc.refactor.from(rel, target)";
            case TO ->
                    redirectLogicCQL = "CALL apoc.refactor.to(rel, target)";
        }
        String cypherProcedureString = "MATCH ()-[rel]->()\n" +
                "WHERE id(rel) IN "+relationEntityUIDs.toString()+"\n" +
                "MATCH (target)\n" +
                "WHERE id(target) = "+targetEntityUID+"\n" +
                redirectLogicCQL+"\n" +
                "YIELD output\n" +
                "RETURN output AS operationResult;";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(null,workingGraphOperationExecutor,false);
            Object queryRes = workingGraphOperationExecutor.executeWrite(getListRelationshipEntityTransformer,cypherProcedureString);
            if(queryRes != null){
                return (List<RelationshipEntity>)queryRes;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public Entity mergeConceptionEntities(String remainsEntityUID, List<String> mergedConceptionEntitiesUIDs) throws EngineServiceRuntimeException {
        //https://neo4j.com/docs/apoc/current/overview/apoc.refactor/apoc.refactor.mergeNodes/
        if(remainsEntityUID == null){
            logger.error("Param remainsEntityUID in method mergeConceptionEntities is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param remainsEntityUID in method mergeConceptionEntities is required");
            throw e1;
        }
        if(mergedConceptionEntitiesUIDs == null || mergedConceptionEntitiesUIDs.size() == 0){
            logger.error("At lease one mergedConceptionEntitiesUID is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("At lease one mergedConceptionEntitiesUID is required");
            throw e1;
        }

        ArrayList allConceptionEntitiesUIDList = new ArrayList();
        allConceptionEntitiesUIDList.add(remainsEntityUID);
        allConceptionEntitiesUIDList.addAll(mergedConceptionEntitiesUIDs);

        String cypherProcedureString = "MATCH (targetNodes) WHERE id(targetNodes) IN " + allConceptionEntitiesUIDList.toString()+"\n"+
                "WITH collect(targetNodes) as nodes\n"+
                "CALL apoc.refactor.mergeNodes(nodes,{\n" +
                "  properties:\"combine\",\n" +
                "  mergeRels:true\n" +
                "})"+
                "YIELD node\n" +
                "RETURN node as operationResult";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetSingleEntityTransformer getSingleEntityTransformer = new GetSingleEntityTransformer(null,workingGraphOperationExecutor);
            Object queryRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,cypherProcedureString);
            if(queryRes != null){
                return (Entity)queryRes;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public RelationshipEntity mergeRelationEntities(String remainsRelationshipEntityUID, List<String> mergedRelationEntitiesUIDs) throws EngineServiceRuntimeException {
        //https://neo4j.com/docs/apoc/current/overview/apoc.refactor/apoc.refactor.mergeRelationships/
        if(remainsRelationshipEntityUID == null){
            logger.error("Param remainsRelationshipEntityUID in method mergeConceptionEntities is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("Param remainsRelationshipEntityUID in method mergeConceptionEntities is required");
            throw e1;
        }
        if(mergedRelationEntitiesUIDs == null || mergedRelationEntitiesUIDs.size() == 0){
            logger.error("At lease one mergedRelationEntitiesUID is required");
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage("At lease one mergedRelationEntitiesUID is required");
            throw e1;
        }

        ArrayList allRelationEntitiesUIDList = new ArrayList();
        allRelationEntitiesUIDList.add(remainsRelationshipEntityUID);
        allRelationEntitiesUIDList.addAll(mergedRelationEntitiesUIDs);

        String cypherProcedureString = "MATCH ()-[targetRel]->() WHERE id(targetRel) IN " + allRelationEntitiesUIDList.toString()+"\n"+
                "WITH collect(targetRel) as rels\n"+
                "CALL apoc.refactor.mergeRelationships(rels,{\n" +
                "  properties:\"combine\"\n" +
                "})"+
                "YIELD rel\n" +
                "RETURN rel as operationResult";
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer(null,workingGraphOperationExecutor);
            Object queryRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer,cypherProcedureString);
            if(queryRes != null){
                return (RelationshipEntity)queryRes;
            }
        }catch (org.neo4j.driver.exceptions.ClientException e){
            EngineServiceRuntimeException e1 = new EngineServiceRuntimeException();
            e1.setCauseMessage(e.getMessage());
            throw e1;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }

    private class MergeEntitiesToConceptionKindThread implements Runnable {
        private List<RelationshipEntityValue> relationEntityValueList;
        private Map<String,Object> threadReturnDataMap;
        private Direction relationDirection;
        public MergeEntitiesToConceptionKindThread(List<RelationshipEntityValue> relationEntityValueList,Direction relationDirection,Map<String,Object> threadReturnDataMap){
            this.relationEntityValueList = relationEntityValueList;
            this.relationDirection = relationDirection;
            this.threadReturnDataMap = threadReturnDataMap;
        }

        @Override
        public void run() {
            String currentThreadName = Thread.currentThread().getName();
            GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
            DataTransformer<Integer> resultDataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    if(result.hasNext()){
                        Record record = result.next();
                        if(record.containsKey("count(*)")){
                            return record.get("count(*)").asInt();
                        }
                    }
                    return null;
                }
            };
            int successCount = 0;
            List<String> remainEntityUIDList = new ArrayList<>();
            for(RelationshipEntityValue currentRelationshipEntityValue : this.relationEntityValueList){
                String fromEntityUID = currentRelationshipEntityValue.getFromEntityUID();
                String toEntityUID = currentRelationshipEntityValue.getToEntityUID();
                String removedEntityUID = "";
                String remainedEntityUID = "";
                switch(this.relationDirection){
                    case FROM:
                        remainedEntityUID = toEntityUID;
                        removedEntityUID = fromEntityUID;
                        break;
                    case TO:
                        removedEntityUID = toEntityUID;
                        remainedEntityUID = fromEntityUID;
                }

                String cypherProcedureString = "MATCH (nodeA) WHERE id(nodeA)= " + remainedEntityUID+"\n" +
                        "MATCH (nodeB) WHERE id(nodeB)= " + removedEntityUID+"\n" +
                        "WITH head(collect([nodeA,nodeB])) as nodes\n" +
                        "CALL apoc.refactor.mergeNodes(nodes,{\n" +
                        "    properties:\"combine\",\n" +
                        "    mergeRels:true,\n" +
                        "    produceSelfRel:false\n" +
                        "})\n" +
                        "YIELD node\n" +
                        "RETURN count(*)";
                Object queryRes = graphOperationExecutor.executeWrite(resultDataTransformer,cypherProcedureString);
                if(queryRes != null){
                    int resultNo = (Integer)queryRes;
                    successCount = successCount + resultNo;
                    remainEntityUIDList.add(remainedEntityUID);
                }
            }
            threadReturnDataMap.put("SUCCESSCOUNT_"+currentThreadName,successCount);
            threadReturnDataMap.put("SUCCESSUIDS_"+currentThreadName,remainEntityUIDList);
            graphOperationExecutor.close();
        }
    }
}
