package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.FilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.payload.EntitiesRetrieveResult;
import com.github.tgda.engine.core.payload.TimeScaleEventsRetrieveResult;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonEntitiesRetrieveResultImpl;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonTimeScaleEventsRetrieveResultImpl;
import com.github.tgda.engine.core.structure.InheritanceTree;
import com.github.tgda.engine.core.structure.spi.common.structureImpl.CommonInheritanceTreeImpl;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.util.Constant;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.github.tgda.coreRealm.realmServiceCore.internal.neo4j.dataTransformer.*;
import com.github.tgda.engine.core.term.TimeFlow;
import com.github.tgda.engine.core.term.TimeScaleEntity;
import com.github.tgda.engine.core.term.TimeScaleEvent;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JTimeScaleEntity;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

public class Neo4JTimeScaleEntityImpl implements Neo4JTimeScaleEntity {

    private static Logger logger = LoggerFactory.getLogger(Neo4JTimeScaleEntityImpl.class);
    private String coreRealmName;
    private String timeScaleEntityUID;
    private String timeFlowName;
    private TimeFlow.TimeScaleGrade timeScaleGrade;
    private int entityValue;

    public Neo4JTimeScaleEntityImpl(String coreRealmName,String timeFlowName,String timeScaleEntityUID,TimeFlow.TimeScaleGrade timeScaleGrade,int entityValue){
        this.coreRealmName = coreRealmName;
        this.timeFlowName = timeFlowName;
        this.timeScaleEntityUID = timeScaleEntityUID;
        this.timeScaleGrade = timeScaleGrade;
        this.entityValue = entityValue;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    @Override
    public TimeFlow.TimeScaleGrade getTimeScaleGrade() {
        return this.timeScaleGrade;
    }

    @Override
    public int getEntityValue() {
        return this.entityValue;
    }

    @Override
    public TimeScaleEntity getParentEntity() {
        String queryCql = "MATCH(currentEntity:TGDA_TimeScaleEntity)<-[:TGDA_TS_Contains]-(parentEntity:TGDA_TimeScaleEntity) WHERE id(currentEntity) = "+ this.getTimeScaleEntityUID() +" RETURN parentEntity as operationResult";
        return getSingleTimeScaleEntity(queryCql);
    }

    @Override
    public TimeScaleEntity getNextSameScaleEntity() {
        String queryCql = "MATCH(currentEntity:TGDA_TimeScaleEntity)-[:TGDA_TS_NextIs]->(targetEntity:TGDA_TimeScaleEntity) WHERE id(currentEntity) = "+ this.getTimeScaleEntityUID() +" RETURN targetEntity as operationResult";
        return getSingleTimeScaleEntity(queryCql);
    }

    @Override
    public TimeScaleEntity getPreviousSameScaleEntity() {
        String queryCql = "MATCH(currentEntity:TGDA_TimeScaleEntity)<-[:TGDA_TS_NextIs]-(targetEntity:TGDA_TimeScaleEntity) WHERE id(currentEntity) = "+ this.getTimeScaleEntityUID() +" RETURN targetEntity as operationResult";
        return getSingleTimeScaleEntity(queryCql);
    }

    @Override
    public LinkedList<TimeScaleEntity> getFellowEntities() {
        TimeScaleEntity parentTimeScaleEntity = getParentEntity();
        if(parentTimeScaleEntity == null){
            //this is a YEAR level TimeScaleEntity, need query all year entities, these years maybe not all linked together
            String queryCql = "MATCH(timeFlow:TGDA_TimeFlow{name:\""+this.timeFlowName+"\"})-[:TGDA_TS_Contains]->(year:TGDA_TS_Year) RETURN year as operationResult ORDER BY year.year\n";
            return getListTimeScaleEntity(queryCql);
        }else{
            String parentEntityUID = ((Neo4JTimeScaleEntityImpl)parentTimeScaleEntity).getTimeScaleEntityUID();
            String queryCql = "MATCH(parentEntity:TGDA_TimeScaleEntity)-[:TGDA_TS_Contains]->(fellowEntities:TGDA_TimeScaleEntity) WHERE id(parentEntity) = "+ parentEntityUID +" RETURN fellowEntities as operationResult ORDER BY fellowEntities.id";
            return getListTimeScaleEntity(queryCql);
        }
    }

    @Override
    public LinkedList<TimeScaleEntity> getChildEntities() {
        String queryCql = "MATCH(currentEntity:TGDA_TimeScaleEntity)-[:TGDA_TS_Contains]->(targetEntities:TGDA_TimeScaleEntity) WHERE id(currentEntity) = "+ this.getTimeScaleEntityUID() +" RETURN targetEntities as operationResult ORDER BY targetEntities.id";
        return getListTimeScaleEntity(queryCql);
    }

    @Override
    public TimeScaleEntity getFirstChildEntity() {
        String queryCql = "MATCH(currentEntity:TGDA_TimeScaleEntity)-[:TGDA_TS_FirstChildIs]->(targetEntity:TGDA_TimeScaleEntity) WHERE id(currentEntity) = "+ this.getTimeScaleEntityUID() +" RETURN targetEntity as operationResult";
        return getSingleTimeScaleEntity(queryCql);
    }

    @Override
    public TimeScaleEntity getLastChildEntity() {
        String queryCql = "MATCH(currentEntity:TGDA_TimeScaleEntity)-[:TGDA_TS_LastChildIs]->(targetEntity:TGDA_TimeScaleEntity) WHERE id(currentEntity) = "+ this.getTimeScaleEntityUID() +" RETURN targetEntity as operationResult";
        return getSingleTimeScaleEntity(queryCql);
    }

    @Override
    public InheritanceTree<TimeScaleEntity> getOffspringEntities() {
        Table<String,String, TimeScaleEntity> treeElementsTable = HashBasedTable.create();
        treeElementsTable.put(InheritanceTree.Virtual_ParentID_Of_Root_Node,this.getTimeScaleEntityUID(),this);
        final String currentCoreRealmName = this.coreRealmName;

        String queryCql = "MATCH (currentEntity:TGDA_TimeScaleEntity)-[relationResult:`TGDA_TS_Contains`*1..5]->(operationResult:`TGDA_TimeScaleEntity`) WHERE id(currentEntity) = "+this.getTimeScaleEntityUID()+" RETURN operationResult,relationResult";
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            logger.debug("Generated Cypher Statement: {}", queryCql);

            DataTransformer offspringTimeScaleEntitiesDataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    List<Record> recordList = result.list();

                    if(recordList != null){
                        for(Record nodeRecord : recordList){
                            Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                            long nodeUID = resultNode.id();

                            TimeFlow.TimeScaleGrade timeScaleGrade = null;

                            String entityUID = ""+nodeUID;
                            int value = resultNode.get("id").asInt();
                            String timeFlowName = resultNode.get("timeFlow").asString();
                            if(resultNode.get("year").asObject() != null){
                                timeScaleGrade = TimeFlow.TimeScaleGrade.YEAR;
                            }else if(resultNode.get("month").asObject() != null){
                                timeScaleGrade = TimeFlow.TimeScaleGrade.MONTH;
                            }else if(resultNode.get("day").asObject() != null){
                                timeScaleGrade = TimeFlow.TimeScaleGrade.DAY;
                            }else if(resultNode.get("hour").asObject() != null){
                                timeScaleGrade = TimeFlow.TimeScaleGrade.HOUR;
                            }else if(resultNode.get("minute").asObject() != null){
                                timeScaleGrade = TimeFlow.TimeScaleGrade.MINUTE;
                            }
                            Neo4JTimeScaleEntityImpl neo4JTimeScaleEntityImpl = new Neo4JTimeScaleEntityImpl(currentCoreRealmName,timeFlowName,entityUID,timeScaleGrade,value);
                            neo4JTimeScaleEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);

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
                            treeElementsTable.put(parentClassificationUID,entityUID,neo4JTimeScaleEntityImpl);
                        }
                    }
                    return null;
                }
            };
            workingGraphOperationExecutor.executeRead(offspringTimeScaleEntitiesDataTransformer,queryCql);
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }

        CommonInheritanceTreeImpl<TimeScaleEntity> resultInheritanceTree = new CommonInheritanceTreeImpl(this.getTimeScaleEntityUID(),treeElementsTable);
        return resultInheritanceTree;
    }

    @Override
    public Long countAttachedTimeScaleEvents(AttributesParameters attributesParameters, boolean isDistinctMode, TimeScaleLevel timeScaleLevel) {
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
                String eventEntitiesQueryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.TimeScaleEventClass,queryParameters,CypherBuilder.CypherFunctionType.COUNT);
                eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_TimeScaleEvent`)","(childEntities)-[:`TGDA_TS_TimeReferTo`]->(operationResult:`TGDA_TimeScaleEvent`)");
                String queryCql = addTimeScaleGradeTravelLogic(timeScaleLevel,eventEntitiesQueryCql);
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
            return countAttachedConceptionEntities(timeScaleLevel);
        }
    }

    @Override
    public TimeScaleEventsRetrieveResult getAttachedTimeScaleEvents(QueryParameters queryParameters, TimeScaleLevel timeScaleLevel) {
        try {
            CommonTimeScaleEventsRetrieveResultImpl commonTimeScaleEventsRetrieveResultImpl = new CommonTimeScaleEventsRetrieveResultImpl();
            commonTimeScaleEventsRetrieveResultImpl.getOperationStatistics().setQueryParameters(queryParameters);
            String eventEntitiesQueryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.TimeScaleEventClass,queryParameters,null);
            eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_TimeScaleEvent`)","(childEntities)-[:`TGDA_TS_TimeReferTo`]->(operationResult:`TGDA_TimeScaleEvent`)");
            String queryCql = addTimeScaleGradeTravelLogic(timeScaleLevel,eventEntitiesQueryCql);
            logger.debug("Generated Cypher Statement: {}", queryCql);

            try{
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
                GetListTimeScaleEventTransformer getListTimeScaleEventTransformer = new GetListTimeScaleEventTransformer(timeFlowName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object queryRes = workingGraphOperationExecutor.executeRead(getListTimeScaleEventTransformer,queryCql);
                if(queryRes != null){
                    List<TimeScaleEvent> res = (List<TimeScaleEvent>)queryRes;
                    commonTimeScaleEventsRetrieveResultImpl.addTimeScaleEvents(res);
                    commonTimeScaleEventsRetrieveResultImpl.getOperationStatistics().setResultEntitiesCount(res.size());
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
            commonTimeScaleEventsRetrieveResultImpl.finishEntitiesRetrieving();
            return commonTimeScaleEventsRetrieveResultImpl;
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Long countAttachedConceptionEntities(TimeScaleLevel timeScaleLevel) {
        String relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1..3";
        switch (timeScaleLevel){
            case CHILD: relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1";
            break;
            case OFFSPRING:
                switch(this.timeScaleGrade){
                    case YEAR:
                        relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1..4";
                        break;
                    case MONTH:
                        relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1..3";
                        break;
                    case DAY:
                        relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1..2";
                        break;
                    case HOUR:
                        relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1";
                        break;
                    case MINUTE:
                        return 0l;
                }
        }
        switch(this.timeScaleGrade){
            case YEAR: break;
            case MONTH: break;
            case DAY:break;
            case HOUR:break;
            case MINUTE:
                switch (timeScaleLevel){
                    case CHILD:return 0l;
                    case OFFSPRING:return 0l;
                }
                break;
        }
        String queryCql = "MATCH(currentEntity:TGDA_TimeScaleEntity)-["+relationTravelLogic+"]->(childEntities:`TGDA_TimeScaleEntity`) WHERE id(currentEntity) = "+this.getTimeScaleEntityUID()+" \n" +
                "MATCH (childEntities)-[:`TGDA_TS_TimeReferTo`]->(relatedEvents:`TGDA_TimeScaleEvent`) RETURN count(relatedEvents) as operationResult";
        if(timeScaleLevel.equals(TimeScaleLevel.SELF)){
            queryCql = "MATCH(currentEntity:TGDA_TimeScaleEntity) WHERE id(currentEntity) = "+this.getTimeScaleEntityUID()+" \n" +
                    "MATCH (currentEntity)-[:`TGDA_TS_TimeReferTo`]->(relatedEvents:`TGDA_TimeScaleEvent`) RETURN count(relatedEvents) as operationResult";
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
    public Long countAttachedConceptionEntities(String conceptionKindName,AttributesParameters attributesParameters, boolean isDistinctMode, TimeScaleLevel timeScaleLevel) {
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
                String eventEntitiesQueryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.TimeScaleEventClass,queryParameters,CypherBuilder.CypherFunctionType.COUNT);
                if(conceptionKindName != null){
                    eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_TimeScaleEvent`)","(childEntities)-[:`TGDA_TS_TimeReferTo`]->(timeScaleEvents:`TGDA_TimeScaleEvent`)<-[:`TGDA_AttachToTimeScale`]-(operationResult:`"+conceptionKindName+"`)");
                }else{
                    eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_TimeScaleEvent`)","(childEntities)-[:`TGDA_TS_TimeReferTo`]->(timeScaleEvents:`TGDA_TimeScaleEvent`)<-[:`TGDA_AttachToTimeScale`]-(operationResult)");
                }
                String queryCql = addTimeScaleGradeTravelLogic(timeScaleLevel,eventEntitiesQueryCql);
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
            return countAttachedConceptionEntities(timeScaleLevel);
        }
    }

    @Override
    public EntitiesRetrieveResult getAttachedConceptionEntities(String conceptionKindName, QueryParameters queryParameters, TimeScaleLevel timeScaleLevel) {
        try {
            CommonEntitiesRetrieveResultImpl commonConceptionEntitiesRetrieveResultImpl = new CommonEntitiesRetrieveResultImpl();
            commonConceptionEntitiesRetrieveResultImpl.getOperationStatistics().setQueryParameters(queryParameters);
            String eventEntitiesQueryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.TimeScaleEventClass,queryParameters,null);
            if(conceptionKindName != null){
                eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_TimeScaleEvent`)","(childEntities)-[:`TGDA_TS_TimeReferTo`]->(timeScaleEvents:`TGDA_TimeScaleEvent`)<-[:`TGDA_AttachToTimeScale`]-(operationResult:`"+conceptionKindName+"`)");
            }else{
                eventEntitiesQueryCql = eventEntitiesQueryCql.replace("(operationResult:`TGDA_TimeScaleEvent`)","(childEntities)-[:`TGDA_TS_TimeReferTo`]->(timeScaleEvents:`TGDA_TimeScaleEvent`)<-[:`TGDA_AttachToTimeScale`]-(operationResult)");
            }
            String queryCql = addTimeScaleGradeTravelLogic(timeScaleLevel,eventEntitiesQueryCql);
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

    public String getTimeScaleEntityUID() {
        return timeScaleEntityUID;
    }

    private String addTimeScaleGradeTravelLogic(TimeScaleLevel timeScaleLevel,String eventEntitiesQueryCql){
        String relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1..3";
        switch (timeScaleLevel){
            case CHILD: relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1";
                break;
            case OFFSPRING:
                switch(this.timeScaleGrade){
                    case YEAR:
                        relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1..4";
                        break;
                    case MONTH:
                        relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1..3";
                        break;
                    case DAY:
                        relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1..2";
                        break;
                    case HOUR:
                        relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1";
                        break;
                    case MINUTE:
                        relationTravelLogic = "relationResult:`TGDA_TS_Contains`*1";
                        break;
                }
        }
        String queryCql = "MATCH(currentEntity:TGDA_TimeScaleEntity)-["+relationTravelLogic+"]->(childEntities:`TGDA_TimeScaleEntity`) WHERE id(currentEntity) = "+this.getTimeScaleEntityUID()+" \n" +
                eventEntitiesQueryCql;
        if(timeScaleLevel.equals(TimeScaleLevel.SELF)){
            queryCql = "MATCH(childEntities:TGDA_TimeScaleEntity) WHERE id(childEntities) = "+this.getTimeScaleEntityUID()+" \n" +
                    eventEntitiesQueryCql;
        }
        return queryCql;
    }

    private TimeScaleEntity getSingleTimeScaleEntity(String queryCql){
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            logger.debug("Generated Cypher Statement: {}", queryCql);
            GetSingleTimeScaleEntityTransformer getSingleTimeScaleEntityTransformer =
                    new GetSingleTimeScaleEntityTransformer(this.coreRealmName,graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object queryRes = workingGraphOperationExecutor.executeRead(getSingleTimeScaleEntityTransformer,queryCql);
            if(queryRes != null){
                return (TimeScaleEntity)queryRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    private LinkedList<TimeScaleEntity> getListTimeScaleEntity(String queryCql){
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            logger.debug("Generated Cypher Statement: {}", queryCql);
            GetLinkedListTimeScaleEntityTransformer getLinkedListTimeScaleEntityTransformer =
                    new GetLinkedListTimeScaleEntityTransformer(this.coreRealmName,graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object queryRes = workingGraphOperationExecutor.executeRead(getLinkedListTimeScaleEntityTransformer,queryCql);
            if(queryRes != null){
                return (LinkedList<TimeScaleEntity>)queryRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }
}
