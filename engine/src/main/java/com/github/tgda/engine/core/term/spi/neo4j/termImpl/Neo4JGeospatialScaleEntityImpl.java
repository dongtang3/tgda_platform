package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.FilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.*;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.payload.EntitiesRetrieveResult;
import com.github.tgda.engine.core.payload.GeospatialScaleEventsRetrieveResult;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonEntitiesRetrieveResultImpl;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonGeospatialScaleEventsRetrieveResultImpl;
import com.github.tgda.engine.core.structure.InheritanceTree;
import com.github.tgda.engine.core.structure.spi.common.structureImpl.CommonInheritanceTreeImpl;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.Geospatial;
import com.github.tgda.engine.core.term.GeospatialScaleEntity;
import com.github.tgda.engine.core.term.GeospatialScaleEvent;
import com.github.tgda.engine.core.util.Constant;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JGeospatialScaleEntity;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Neo4JGeospatialScaleEntityImpl implements Neo4JGeospatialScaleEntity {

    private static Logger logger = LoggerFactory.getLogger(Neo4JGeospatialScaleEntityImpl.class);
    private String coreRealmName;
    private String geospatialScaleEntityUID;
    private String geospatialRegionName;
    private Geospatial.GeospatialScaleGrade geospatialScaleGrade;
    private String geospatialCode;
    private String entityChineseName;
    private String entityEnglishName;

    public Neo4JGeospatialScaleEntityImpl(String coreRealmName, String geospatialRegionName, String geospatialScaleEntityUID,
                                          Geospatial.GeospatialScaleGrade geospatialScaleGrade, String geospatialCode,
                                          String entityChineseName,String entityEnglishName){
        this.coreRealmName = coreRealmName;
        this.geospatialRegionName = geospatialRegionName;
        this.geospatialScaleEntityUID = geospatialScaleEntityUID;
        this.geospatialScaleGrade = geospatialScaleGrade;
        this.geospatialCode = geospatialCode;
        this.entityChineseName = entityChineseName;
        this.entityEnglishName = entityEnglishName;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    @Override
    public Geospatial.GeospatialScaleGrade getGeospatialScaleGrade() {
        return this.geospatialScaleGrade;
    }

    @Override
    public String getGeospatialCode() {
        return this.geospatialCode;
    }

    @Override
    public String getChineseName() {
        return this.entityChineseName;
    }

    @Override
    public String getEnglishName() {
        return this.entityEnglishName;
    }

    @Override
    public GeospatialScaleEntity getParentEntity() {
        String queryCql = "MATCH(currentEntity:TGDA_GeospatialScaleEntity)<-[:TGDA_GS_SpatialContains]-(targetEntities:TGDA_GeospatialScaleEntity) WHERE id(currentEntity) = "+ this.getGeospatialScaleEntityUID() +" RETURN targetEntities as operationResult ORDER BY targetEntities.id LIMIT 1";
        return getSingleGeospatialScaleEntity(queryCql);
    }

    @Override
    public List<GeospatialScaleEntity> getFellowEntities() {
        GeospatialScaleEntity parentGeospatialScaleEntity = getParentEntity();
        String queryCql;
        if(parentGeospatialScaleEntity == null){
            queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.GeospatialScaleContinentEntityClass, Constant.GeospatialProperty,geospatialRegionName,100);
        }else{
            String parentEntityUID = ((Neo4JGeospatialScaleEntityImpl) parentGeospatialScaleEntity).getGeospatialScaleEntityUID();
            queryCql = "MATCH(parentEntity:TGDA_GeospatialScaleEntity)-[:TGDA_GS_SpatialContains]->(fellowEntities:TGDA_GeospatialScaleEntity) WHERE id(parentEntity) = "+ parentEntityUID +" RETURN fellowEntities as operationResult ORDER BY fellowEntities.id";
        }
        return getListGeospatialScaleEntity(queryCql);
    }

    @Override
    public List<GeospatialScaleEntity> getChildEntities() {
        String queryCql = "MATCH(currentEntity:TGDA_GeospatialScaleEntity)-[:TGDA_GS_SpatialContains]->(targetEntities:TGDA_GeospatialScaleEntity) WHERE id(currentEntity) = "+ this.getGeospatialScaleEntityUID() +" RETURN targetEntities as operationResult ORDER BY targetEntities.id";
        return getListGeospatialScaleEntity(queryCql);
    }

    @Override
    public InheritanceTree<GeospatialScaleEntity> getOffspringEntities() {
        Table<String,String, GeospatialScaleEntity> treeElementsTable = HashBasedTable.create();
        treeElementsTable.put(InheritanceTree.Virtual_ParentID_Of_Root_Node,this.geospatialScaleEntityUID,this);
        final String currentCoreRealmName = this.coreRealmName;
        final String currentGeospatialName = this.geospatialRegionName;

        String queryCql = "MATCH (currentEntity:TGDA_GeospatialScaleEntity)-[relationResult:`TGDA_GS_SpatialContains`*1..7]->(operationResult:`TGDA_GeospatialScaleEntity`) WHERE id(currentEntity) = "+this.getGeospatialScaleEntityUID()+" RETURN operationResult,relationResult";
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            logger.debug("Generated Cypher Statement: {}", queryCql);

            DataTransformer offspringGeospatialScaleEntitiesDataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    List<Record> recordList = result.list();
                    if(recordList != null){
                        for(Record nodeRecord : recordList){
                            Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                            long nodeUID = resultNode.id();
                            String entityUID = ""+nodeUID;
                            String conceptionEntityUID = ""+nodeUID;
                            String targetGeospatialCode = resultNode.get(Constant.GeospatialCodeProperty).asString();
                            String targetGeospatialScaleGradeString = resultNode.get(Constant.GeospatialScaleGradeProperty).asString();
                            String _ChineseName = null;
                            String _EnglishName = null;
                            if(resultNode.containsKey(Constant.GeospatialChineseNameProperty)){
                                _ChineseName = resultNode.get(Constant.GeospatialChineseNameProperty).asString();
                            }
                            if(resultNode.containsKey(Constant.GeospatialEnglishNameProperty)){
                                _EnglishName = resultNode.get(Constant.GeospatialEnglishNameProperty).asString();
                            }

                            Geospatial.GeospatialScaleGrade geospatialScaleGrade = null;
                            switch (targetGeospatialScaleGradeString){
                                case "CONTINENT":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.CONTINENT;break;
                                case "COUNTRY_REGION":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.COUNTRY_REGION;break;
                                case "PROVINCE":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.PROVINCE;break;
                                case "PREFECTURE":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.PREFECTURE;break;
                                case "COUNTY":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.COUNTY;break;
                                case "TOWNSHIP":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.TOWNSHIP;break;
                                case "VILLAGE":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.VILLAGE;break;
                            }
                            Neo4JGeospatialScaleEntityImpl neo4JGeospatialScaleEntityImpl =
                                    new Neo4JGeospatialScaleEntityImpl(currentCoreRealmName,currentGeospatialName,conceptionEntityUID,geospatialScaleGrade,targetGeospatialCode,_ChineseName,_EnglishName);
                            neo4JGeospatialScaleEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);

                            List<Object> relationships = nodeRecord.get(CypherBuilder.relationResultName).asList();
                            String parentClassificationUID = null;
                            for(Object currentRelationship : relationships){
                                Relationship currentTargetRelationship = (Relationship)currentRelationship;
                                String startNodeUID = "" + currentTargetRelationship.startNodeId();
                                String endNodeUID = "" + currentTargetRelationship.endNodeId();
                                if(endNodeUID.equals(entityUID)){
                                    parentClassificationUID = startNodeUID;
                                    break;
                                }
                            }
                            treeElementsTable.put(parentClassificationUID,entityUID,neo4JGeospatialScaleEntityImpl);
                        }
                    }
                    return null;
                }
            };
            workingGraphOperationExecutor.executeRead(offspringGeospatialScaleEntitiesDataTransformer,queryCql);
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }

        CommonInheritanceTreeImpl<GeospatialScaleEntity> resultInheritanceTree = new CommonInheritanceTreeImpl(this.geospatialScaleEntityUID,treeElementsTable);
        return resultInheritanceTree;
    }

    @Override
    public Long countAttachedGeospatialScaleEvents(AttributesParameters attributesParameters, boolean isDistinctMode, GeospatialScaleLevel geospatialScaleLevel) {
        if(attributesParameters != null){
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
            try {
                String eventEntitiesQueryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleEventClass,queryParameters,CypherBuilder.CypherFunctionType.COUNT);
                eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_GeospatialScaleEvent`)","(childEntities)-[:`TGDA_GS_GeospatialReferTo`]->(operationResult:`TGDA_GeospatialScaleEvent`)");
                String queryCql = addGeospatialScaleGradeTravelLogic(geospatialScaleLevel,eventEntitiesQueryCql);
                logger.debug("Generated Cypher Statement: {}", queryCql);

                DataTransformer<Long> _DataTransformer = new DataTransformer<Long>() {
                    @Override
                    public Long transformResult(Result result) {
                        if (result.hasNext()) {
                            Record record = result.next();
                            if (record.containsKey("count("+CypherBuilder.operationResultName+")")) {
                                return record.get("count("+CypherBuilder.operationResultName+")").asLong();
                            }
                            return null;
                        }
                        return null;
                    }
                };
                Long resultNumber = 0l;
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
                try{
                    Object countRes = workingGraphOperationExecutor.executeRead(_DataTransformer,queryCql);
                    resultNumber = countRes != null ? (Long) countRes: 0l;
                    return resultNumber;
                }finally {
                    this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
                }
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            }
            return null;
        }else{
            return countAttachedConceptionEntities(geospatialScaleLevel);
        }
    }

    @Override
    public GeospatialScaleEventsRetrieveResult getAttachedGeospatialScaleEvents(QueryParameters queryParameters, GeospatialScaleLevel geospatialScaleLevel) {
        try {
            CommonGeospatialScaleEventsRetrieveResultImpl commonGeospatialScaleEventsRetrieveResultImpl = new CommonGeospatialScaleEventsRetrieveResultImpl();
            commonGeospatialScaleEventsRetrieveResultImpl.getOperationStatistics().setQueryParameters(queryParameters);
            String eventEntitiesQueryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleEventClass,queryParameters,null);
            eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_GeospatialScaleEvent`)","(childEntities)-[:`TGDA_GS_GeospatialReferTo`]->(operationResult:`TGDA_GeospatialScaleEvent`)");
            String queryCql = addGeospatialScaleGradeTravelLogic(geospatialScaleLevel,eventEntitiesQueryCql);
            logger.debug("Generated Cypher Statement: {}", queryCql);

            try{
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
                GetListGeospatialScaleEventTransformer getListGeospatialScaleEventTransformer =
                        new GetListGeospatialScaleEventTransformer(this.geospatialRegionName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object queryRes = workingGraphOperationExecutor.executeRead(getListGeospatialScaleEventTransformer,queryCql);
                if(queryRes != null){
                    List<GeospatialScaleEvent> res = (List<GeospatialScaleEvent>)queryRes;
                    commonGeospatialScaleEventsRetrieveResultImpl.addGeospatialScaleEvents(res);
                    commonGeospatialScaleEventsRetrieveResultImpl.getOperationStatistics().setResultEntitiesCount(res.size());
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
            commonGeospatialScaleEventsRetrieveResultImpl.finishEntitiesRetrieving();
            return commonGeospatialScaleEventsRetrieveResultImpl;

        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long countAttachedConceptionEntities(GeospatialScaleLevel geospatialScaleLevel) {
        String relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..3";
        switch (geospatialScaleLevel){
            case CHILD: relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1";
                break;
            case OFFSPRING:
                switch(this.geospatialScaleGrade){
                    case CONTINENT:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..6";
                        break;
                    case COUNTRY_REGION:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..5";
                        break;
                    case PROVINCE:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..4";
                        break;
                    case PREFECTURE:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..3";
                        break;
                    case COUNTY:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..2";
                        break;
                    case TOWNSHIP:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1";
                        break;
                    case VILLAGE:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1";
                        break;
                }
        }

        String queryCql = "MATCH(currentEntity:TGDA_GeospatialScaleEntity)-["+relationTravelLogic+"]->(childEntities:`TGDA_GeospatialScaleEntity`) WHERE id(currentEntity) = "+this.geospatialScaleEntityUID+" \n" +
                "MATCH (childEntities)-[:`TGDA_GS_GeospatialReferTo`]->(relatedEvents:`TGDA_GeospatialScaleEvent`)<-[:`TGDA_AttachToGeospatialScale`]-(operationResult) RETURN count(operationResult) as operationResult";
        switch (geospatialScaleLevel){
            case SELF:
                queryCql = "MATCH(childEntities:TGDA_GeospatialScaleEntity) WHERE id(childEntities) = "+this.geospatialScaleEntityUID+" \n" +
                        "MATCH (childEntities)-[:`TGDA_GS_GeospatialReferTo`]->(relatedEvents:`TGDA_GeospatialScaleEvent`)<-[:`TGDA_AttachToGeospatialScale`]-(operationResult) RETURN count(operationResult) as operationResult";
        }
        logger.debug("Generated Cypher Statement: {}", queryCql);

        DataTransformer<Long> _DataTransformer = new DataTransformer<Long>() {
            @Override
            public Long transformResult(Result result) {

                if (result.hasNext()) {
                    Record record = result.next();
                    if (record.containsKey(CypherBuilder.operationResultName)) {
                        return record.get(CypherBuilder.operationResultName).asLong();
                    }
                    return null;
                }
                return null;
            }
        };
        Long resultNumber = 0l;
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            Object countRes = workingGraphOperationExecutor.executeRead(_DataTransformer,queryCql);
            resultNumber = countRes != null ? (Long) countRes: 0l;
            return resultNumber;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Long countAttachedConceptionEntities(String conceptionKindName, AttributesParameters attributesParameters, boolean isDistinctMode, GeospatialScaleLevel geospatialScaleLevel) {
        if(attributesParameters != null){
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
            try {
                CommonEntitiesRetrieveResultImpl commonConceptionEntitiesRetrieveResultImpl = new CommonEntitiesRetrieveResultImpl();
                commonConceptionEntitiesRetrieveResultImpl.getOperationStatistics().setQueryParameters(queryParameters);
                String eventEntitiesQueryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleEntityClass,queryParameters,CypherBuilder.CypherFunctionType.COUNT);

                if(conceptionKindName != null){
                    eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_GeospatialScaleEntity`)","(childEntities)-[:`TGDA_GS_GeospatialReferTo`]->(geospatialScaleEvents:`TGDA_GeospatialScaleEvent`)<-[:`TGDA_AttachToGeospatialScale`]-(operationResult:`"+conceptionKindName+"`)");
                }else{
                    eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_GeospatialScaleEntity`)","(childEntities)-[:`TGDA_GS_GeospatialReferTo`]->(geospatialScaleEvents:`TGDA_GeospatialScaleEvent`)<-[:`TGDA_AttachToGeospatialScale`]-(operationResult)");
                }

                String queryCql = addGeospatialScaleGradeTravelLogic(geospatialScaleLevel,eventEntitiesQueryCql);
                logger.debug("Generated Cypher Statement: {}", queryCql);

                DataTransformer<Long> _DataTransformer = new DataTransformer<Long>() {
                    @Override
                    public Long transformResult(Result result) {
                        if (result.hasNext()) {
                            Record record = result.next();
                            if (record.containsKey("count("+CypherBuilder.operationResultName+")")) {
                                return record.get("count("+CypherBuilder.operationResultName+")").asLong();
                            }
                            return null;
                        }
                        return null;
                    }
                };
                Long resultNumber = 0l;
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
                try{
                    Object countRes = workingGraphOperationExecutor.executeRead(_DataTransformer,queryCql);
                    resultNumber = countRes != null ? (Long) countRes: 0l;
                    return resultNumber;
                }finally {
                    this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
                }
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            }
            return null;
        }else{
            return countAttachedConceptionEntities(geospatialScaleLevel);
        }
    }

    @Override
    public EntitiesRetrieveResult getAttachedConceptionEntities(String conceptionKindName, QueryParameters queryParameters, GeospatialScaleLevel geospatialScaleLevel) {
        try {
            CommonEntitiesRetrieveResultImpl commonConceptionEntitiesRetrieveResultImpl = new CommonEntitiesRetrieveResultImpl();
            commonConceptionEntitiesRetrieveResultImpl.getOperationStatistics().setQueryParameters(queryParameters);
            String eventEntitiesQueryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleEntityClass,queryParameters,null);
            if(conceptionKindName != null){
                eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_GeospatialScaleEntity`)","(childEntities)-[:`TGDA_GS_GeospatialReferTo`]->(geospatialScaleEvents:`TGDA_GeospatialScaleEvent`)<-[:`TGDA_AttachToGeospatialScale`]-(operationResult:`"+conceptionKindName+"`)");
            }else{
                eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_GeospatialScaleEntity`)","(childEntities)-[:`TGDA_GS_GeospatialReferTo`]->(geospatialScaleEvents:`TGDA_GeospatialScaleEvent`)<-[:`TGDA_AttachToGeospatialScale`]-(operationResult)");
            }
            String queryCql = addGeospatialScaleGradeTravelLogic(geospatialScaleLevel,eventEntitiesQueryCql);
            logger.debug("Generated Cypher Statement: {}", queryCql);

            try{
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
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
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }
        return null;
    }

    private String addGeospatialScaleGradeTravelLogic(GeospatialScaleLevel geospatialScaleLevel, String eventEntitiesQueryCql){
        String relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..3";
        switch (geospatialScaleLevel){
            case CHILD: relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1";
                break;
            case OFFSPRING:
                switch(this.geospatialScaleGrade){
                    case CONTINENT:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..6";
                        break;
                    case COUNTRY_REGION:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..5";
                        break;
                    case PROVINCE:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..4";
                        break;
                    case PREFECTURE:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..3";
                        break;
                    case COUNTY:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1..2";
                        break;
                    case TOWNSHIP:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1";
                        break;
                    case VILLAGE:
                        relationTravelLogic = "relationResult:`TGDA_GS_SpatialContains`*1";
                        break;
                }
        }

        String queryCql = "MATCH(currentEntity:TGDA_GeospatialScaleEntity)-["+relationTravelLogic+"]->(childEntities:`TGDA_GeospatialScaleEntity`) WHERE id(currentEntity) = "+this.geospatialScaleEntityUID+" \n" +
                eventEntitiesQueryCql;
        switch (geospatialScaleLevel){
            case SELF:
                queryCql = "MATCH(childEntities:TGDA_GeospatialScaleEntity) WHERE id(childEntities) = "+this.geospatialScaleEntityUID+" \n" +
                        eventEntitiesQueryCql;
        }
        return queryCql;
    }

    private List<GeospatialScaleEntity> getListGeospatialScaleEntity(String queryCql){
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            logger.debug("Generated Cypher Statement: {}", queryCql);
            GetListGeospatialScaleEntityTransformer getListGeospatialScaleEntityTransformer =
                    new GetListGeospatialScaleEntityTransformer(this.coreRealmName, this.geospatialRegionName, workingGraphOperationExecutor);
            Object queryRes = workingGraphOperationExecutor.executeRead(getListGeospatialScaleEntityTransformer,queryCql);
            if(queryRes != null){
                return (List<GeospatialScaleEntity>)queryRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return new ArrayList<>();
    }

    private GeospatialScaleEntity getSingleGeospatialScaleEntity(String queryCql){
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            logger.debug("Generated Cypher Statement: {}", queryCql);
            GetSingleGeospatialScaleEntityTransformer getSingleGeospatialScaleEntityTransformer =
                    new GetSingleGeospatialScaleEntityTransformer(this.coreRealmName, this.geospatialRegionName, workingGraphOperationExecutor);
            Object queryRes = workingGraphOperationExecutor.executeRead(getSingleGeospatialScaleEntityTransformer,queryCql);
            if(queryRes != null){
                return (GeospatialScaleEntity)queryRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    public String getGeospatialScaleEntityUID() {
        return geospatialScaleEntityUID;
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }
}
