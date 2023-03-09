package com.github.tgda.engine.core.feature.spi.neo4j.featureInf;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.payload.TimeScaleDataPair;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.feature.TimeScaleFeatureSupportable;
import com.github.tgda.coreRealm.realmServiceCore.internal.neo4j.dataTransformer.*;
import com.github.tgda.coreRealm.realmServiceCore.term.*;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JTimeScaleEntityImpl;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JTimeScaleEventImpl;
import com.github.tgda.engine.core.util.Constant;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.*;
import java.time.temporal.Temporal;
import java.util.*;

public interface Neo4JTimeScaleFeatureSupportable extends TimeScaleFeatureSupportable,Neo4JKeyResourcesRetrievable {

    static Logger logger = LoggerFactory.getLogger(Neo4JTimeScaleFeatureSupportable.class);
    static ZoneId zone = ZoneId.systemDefault();

    public default TimeScaleEvent attachTimeScaleEvent(long dateTime, String eventComment, Map<String, Object> eventData,
                                                       TimeFlow.TimeScaleGrade timeScaleGrade) throws EngineServiceRuntimeException {
        Instant instant = Instant.ofEpochMilli(dateTime);
        LocalDateTime timeStamp = LocalDateTime.ofInstant(instant,zone);
        return attachTimeScaleEventInnerLogic(Constant._defaultTimeFlowName,getReferTime(timeStamp,timeScaleGrade),eventComment,eventData,timeScaleGrade);
    }

    public default TimeScaleEvent attachTimeScaleEvent(String timeFlowName,long dateTime, String eventComment, Map<String, Object> eventData,
                                                       TimeFlow.TimeScaleGrade timeScaleGrade) throws EngineServiceRuntimeException {
        Instant instant = Instant.ofEpochMilli(dateTime);
        LocalDateTime timeStamp = LocalDateTime.ofInstant(instant,zone);
        return attachTimeScaleEventInnerLogic(timeFlowName,getReferTime(timeStamp,timeScaleGrade),eventComment,eventData,timeScaleGrade);
    }

    public default TimeScaleEvent attachTimeScaleEvent(LocalDateTime dateTime, String eventComment, Map<String, Object> eventData,
                                                       TimeFlow.TimeScaleGrade timeScaleGrade) throws EngineServiceRuntimeException {
        return attachTimeScaleEventInnerLogic(Constant._defaultTimeFlowName,getReferTime(dateTime,timeScaleGrade),eventComment,eventData,timeScaleGrade);
    }

    public default TimeScaleEvent attachTimeScaleEvent(String timeFlowName,LocalDateTime dateTime, String eventComment, Map<String, Object> eventData,
                                                       TimeFlow.TimeScaleGrade timeScaleGrade) throws EngineServiceRuntimeException {
        return attachTimeScaleEventInnerLogic(timeFlowName,getReferTime(dateTime,timeScaleGrade),eventComment,eventData,timeScaleGrade);
    }

    public default TimeScaleEvent attachTimeScaleEvent(LocalDate date, String eventComment, Map<String, Object> eventData) throws EngineServiceRuntimeException {
        return attachTimeScaleEventInnerLogic(Constant._defaultTimeFlowName,getReferTime(date,TimeFlow.TimeScaleGrade.DAY),eventComment,eventData,TimeFlow.TimeScaleGrade.DAY);
    }

    public default TimeScaleEvent attachTimeScaleEvent(String timeFlowName,LocalDate date, String eventComment, Map<String, Object> eventData) throws EngineServiceRuntimeException {
        return attachTimeScaleEventInnerLogic(timeFlowName,getReferTime(date,TimeFlow.TimeScaleGrade.DAY),eventComment,eventData,TimeFlow.TimeScaleGrade.DAY);
    }

    public default boolean detachTimeScaleEvent(String timeScaleEventUID) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(timeScaleEventUID), null, null);
                GetSingleEntityTransformer getSingleEntityTransformer =
                        new GetSingleEntityTransformer(Constant.TimeScaleEventClass, getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                Object resEntityRes = workingGraphOperationExecutor.executeRead(getSingleEntityTransformer, queryCql);
                if(resEntityRes == null){
                    logger.error("TimeScaleEvent does not contains entity with UID {}.", timeScaleEventUID);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("TimeScaleEvent does not contains entity with UID " + timeScaleEventUID + ".");
                    throw exception;
                }else{
                    Neo4JTimeScaleEventImpl neo4JTimeScaleEventImpl = new Neo4JTimeScaleEventImpl(null,null,null,null,timeScaleEventUID);
                    neo4JTimeScaleEventImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                    if(neo4JTimeScaleEventImpl.getAttachEntity().getEntityUID().equals(this.getEntityUID())){
                        String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.valueOf(timeScaleEventUID),null,null);
                        Object deletedEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer, deleteCql);
                        if(deletedEntityRes == null){
                            throw new EngineServiceRuntimeException();
                        }else{
                            return true;
                        }
                    }else{
                        logger.error("TimeScaleEvent with entity UID {} doesn't attached to current Entity with UID {}.", timeScaleEventUID,this.getEntityUID());
                        EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                        exception.setCauseMessage("TimeScaleEvent with entity UID " + timeScaleEventUID + " doesn't attached to current Entity with UID "+ this.getEntityUID()+ ".");
                        throw exception;
                    }
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    public default List<TimeScaleEvent> getAttachedTimeScaleEvents(){
        if(this.getEntityUID() != null) {
            String queryCql = "MATCH(currentEntity)-[:`" + Constant.TimeScale_AttachToRelationClass + "`]->(timeScaleEvents:TGDA_TimeScaleEvent) WHERE id(currentEntity) = " + this.getEntityUID() + " \n" +
                    "RETURN timeScaleEvents as operationResult";
            logger.debug("Generated Cypher Statement: {}", queryCql);

            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                GetListTimeScaleEventTransformer getListTimeScaleEventTransformer = new GetListTimeScaleEventTransformer(null,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                Object queryRes = workingGraphOperationExecutor.executeRead(getListTimeScaleEventTransformer,queryCql);
                if(queryRes != null){
                    List<TimeScaleEvent> res = (List<TimeScaleEvent>)queryRes;
                    return res;
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return new ArrayList<>();
    }

    public default List<TimeScaleEntity> getAttachedTimeScaleEntities(){
        if(this.getEntityUID() != null) {
            String queryCql = "MATCH(currentEntity)-[:`" + Constant.TimeScale_AttachToRelationClass + "`]->(timeScaleEvents:TGDA_TimeScaleEvent)<-[:`"+ Constant.TimeScale_TimeReferToRelationClass+"`]-(timeScaleEntities:`TGDA_TimeScaleEntity`) WHERE id(currentEntity) = " + this.getEntityUID() + " \n" +
                    "RETURN timeScaleEntities as operationResult";
            logger.debug("Generated Cypher Statement: {}", queryCql);

            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                GetLinkedListTimeScaleEntityTransformer getLinkedListTimeScaleEntityTransformer =
                        new GetLinkedListTimeScaleEntityTransformer(null,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                Object queryRes = workingGraphOperationExecutor.executeRead(getLinkedListTimeScaleEntityTransformer,queryCql);
                if(queryRes != null){
                    return (LinkedList<TimeScaleEntity>)queryRes;
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return new ArrayList<>();
    }

    public default List<TimeScaleDataPair> getAttachedTimeScaleDataPairs(){
        List<TimeScaleDataPair> timeScaleDataPairList = new ArrayList<>();
        if(this.getEntityUID() != null) {
            String queryCql = "MATCH(currentEntity)-[:`" + Constant.TimeScale_AttachToRelationClass + "`]->(timeScaleEvents:TGDA_TimeScaleEvent)<-[:`"+ Constant.TimeScale_TimeReferToRelationClass+"`]-(timeScaleEntities:`TGDA_TimeScaleEntity`) WHERE id(currentEntity) = " + this.getEntityUID() + " \n" +
                    "RETURN timeScaleEntities ,timeScaleEvents";
            logger.debug("Generated Cypher Statement: {}", queryCql);

            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                DataTransformer<Object> _DataTransformer = new DataTransformer<Object>() {
                    @Override
                    public Object transformResult(Result result) {
                        while(result.hasNext()) {
                            Record record = result.next();

                            Neo4JTimeScaleEntityImpl neo4JTimeScaleEntityImpl = null;
                            Neo4JTimeScaleEventImpl neo4JTimeScaleEventImpl = null;

                            Node timeScaleEntityNode = record.get("timeScaleEntities").asNode();
                            List<String> allLabelNames = Lists.newArrayList(timeScaleEntityNode.labels());
                            boolean isMatchedEntity = true;
                            if (allLabelNames.size() > 0) {
                                isMatchedEntity = allLabelNames.contains(Constant.TimeScaleEntityClass);
                            }
                            if (isMatchedEntity) {
                                TimeFlow.TimeScaleGrade timeScaleGrade = null;
                                long nodeUID = timeScaleEntityNode.id();
                                String entityUID = "" + nodeUID;
                                int value = timeScaleEntityNode.get("id").asInt();
                                String timeFlowName = timeScaleEntityNode.get("timeFlow").asString();

                                if (timeScaleEntityNode.get("year").asObject() != null) {
                                    timeScaleGrade = TimeFlow.TimeScaleGrade.YEAR;
                                } else if (timeScaleEntityNode.get("month").asObject() != null) {
                                    timeScaleGrade = TimeFlow.TimeScaleGrade.MONTH;
                                } else if (timeScaleEntityNode.get("day").asObject() != null) {
                                    timeScaleGrade = TimeFlow.TimeScaleGrade.DAY;
                                } else if (timeScaleEntityNode.get("hour").asObject() != null) {
                                    timeScaleGrade = TimeFlow.TimeScaleGrade.HOUR;
                                } else if (timeScaleEntityNode.get("minute").asObject() != null) {
                                    timeScaleGrade = TimeFlow.TimeScaleGrade.MINUTE;
                                }
                                neo4JTimeScaleEntityImpl = new Neo4JTimeScaleEntityImpl(
                                        null, timeFlowName, entityUID, timeScaleGrade, value);
                                neo4JTimeScaleEntityImpl.setGlobalGraphOperationExecutor(getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                            }

                            Node timeScaleEventNode = record.get("timeScaleEvents").asNode();
                            List<String> allConceptionKindNames = Lists.newArrayList(timeScaleEventNode.labels());
                            boolean isMatchedEvent = false;
                            if(allConceptionKindNames.size()>0){
                                isMatchedEvent = allConceptionKindNames.contains(Constant.TimeScaleEventClass);
                            }
                            if(isMatchedEvent){
                                long nodeUID = timeScaleEventNode.id();
                                String timeScaleEventUID = ""+nodeUID;
                                String eventComment = timeScaleEventNode.get(Constant._TimeScaleEventComment).asString();
                                String timeScaleGrade = timeScaleEventNode.get(Constant._TimeScaleEventScaleGrade).asString();
                                LocalDateTime referTime = timeScaleEventNode.get(Constant._TimeScaleEventReferTime).asLocalDateTime();
                                neo4JTimeScaleEventImpl = new Neo4JTimeScaleEventImpl(null,eventComment,referTime,getTimeScaleGrade(timeScaleGrade),timeScaleEventUID);
                                neo4JTimeScaleEventImpl.setGlobalGraphOperationExecutor(getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                            }

                            if(neo4JTimeScaleEntityImpl != null && neo4JTimeScaleEventImpl != null){
                                timeScaleDataPairList.add(
                                        new TimeScaleDataPair(neo4JTimeScaleEventImpl,neo4JTimeScaleEntityImpl)
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
        return timeScaleDataPairList;
    }

    private TimeScaleEvent attachTimeScaleEventInnerLogic(String timeFlowName,LocalDateTime dateTime, String eventComment,
                                                       Map<String, Object> eventData, TimeFlow.TimeScaleGrade timeScaleGrade) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null){
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                Map<String, Object> propertiesMap = eventData != null ? eventData : new HashMap<>();
                CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
                propertiesMap.put(Constant._TimeScaleEventReferTime,dateTime);
                propertiesMap.put(Constant._TimeScaleEventComment,eventComment);
                propertiesMap.put(Constant._TimeScaleEventScaleGrade,""+timeScaleGrade);
                propertiesMap.put(Constant._TimeScaleEventTimeFlow,timeFlowName);
                String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.TimeScaleEventClass}, propertiesMap);
                logger.debug("Generated Cypher Statement: {}", createCql);
                GetSingleEntityTransformer getSingleEntityTransformer =
                        new GetSingleEntityTransformer(Constant.TimeScaleEventClass, workingGraphOperationExecutor);
                Object newEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer, createCql);
                if(newEntityRes != null) {
                    Entity timeScaleEventEntity = (Entity) newEntityRes;
                    timeScaleEventEntity.attachToRelation(this.getEntityUID(), Constant.TimeScale_AttachToRelationClass, null, true);
                    RelationshipEntity linkToTimeScaleEntityRelation = linkTimeScaleEntity(dateTime,timeFlowName,timeScaleGrade,timeScaleEventEntity,workingGraphOperationExecutor);
                    if(linkToTimeScaleEntityRelation != null){
                        Neo4JTimeScaleEventImpl neo4JTimeScaleEventImpl = new Neo4JTimeScaleEventImpl(timeFlowName,
                                eventComment,dateTime,timeScaleGrade,timeScaleEventEntity.getEntityUID());
                        neo4JTimeScaleEventImpl.setGlobalGraphOperationExecutor(getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                        return neo4JTimeScaleEventImpl;
                    }
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    private RelationshipEntity linkTimeScaleEntity(LocalDateTime temporal, String timeFlowName, TimeFlow.TimeScaleGrade timeScaleGrade,
                                               Entity timeScaleEventEntity, GraphOperationExecutor workingGraphOperationExecutor){
        int year = temporal.getYear();
        int month = temporal.getMonthValue();
        int day = temporal.getDayOfMonth();
        int hour = temporal.getHour();
        int minute = temporal.getMinute();
        int second = temporal.getSecond();
        return linkTimeScaleEntity(year,month,day,hour,minute,second,timeFlowName,timeScaleGrade,timeScaleEventEntity,workingGraphOperationExecutor);
    }

    private RelationshipEntity linkTimeScaleEntity(int year, int month,int day, int hour,int minute,int second,
                                               String timeFlowName, TimeFlow.TimeScaleGrade timeScaleGrade,
                                               Entity timeScaleEventEntity, GraphOperationExecutor workingGraphOperationExecutor){
        String queryCql = null;
        switch (timeScaleGrade) {
            case YEAR:
                queryCql ="MATCH(timeFlow:TGDA_TimeFlow{name:\""+timeFlowName+"\"})-[:TGDA_TS_Contains]->(timeScaleEntity:TGDA_TS_Year{year:"+year+"})";
                break;
            case MONTH:
                queryCql ="MATCH(timeFlow:TGDA_TimeFlow{name:\""+timeFlowName+"\"})-[:TGDA_TS_Contains]->(year:TGDA_TS_Year{year:"+year+"})-[:TGDA_TS_Contains]->(timeScaleEntity:TGDA_TS_Month{month:"+month+"})";
                break;
            case DAY:
                queryCql ="MATCH(timeFlow:TGDA_TimeFlow{name:\""+timeFlowName+"\"})-[:TGDA_TS_Contains]->(year:TGDA_TS_Year{year:"+year+"})-[:TGDA_TS_Contains]->(month:TGDA_TS_Month{month:"+month+"})-[:TGDA_TS_Contains]->(timeScaleEntity:TGDA_TS_Day{day:"+day+"})";
                break;
            case HOUR:
                queryCql = "MATCH(timeFlow:TGDA_TimeFlow{name:\""+timeFlowName+"\"})-[:TGDA_TS_Contains]->(year:TGDA_TS_Year{year:"+year+"})-[:TGDA_TS_Contains]->(month:TGDA_TS_Month{month:"+month+"})-[:TGDA_TS_Contains]->(day:TGDA_TS_Day{day:"+day+"})-[:TGDA_TS_Contains]->(timeScaleEntity:TGDA_TS_Hour{hour:"+hour+"})";
                break;
            case MINUTE:
                queryCql = "MATCH(timeFlow:TGDA_TimeFlow{name:\""+timeFlowName+"\"})-[:TGDA_TS_Contains]->(year:TGDA_TS_Year{year:"+year+"})-[:TGDA_TS_Contains]->(month:TGDA_TS_Month{month:"+month+"})-[:TGDA_TS_Contains]->(day:TGDA_TS_Day{day:"+day+"})-[:TGDA_TS_Contains]->(hour:TGDA_TS_Hour{hour:"+hour+"})-[:TGDA_TS_Contains]->(timeScaleEntity:TGDA_TS_Minute{minute:"+minute+"})";
                break;
            case SECOND:
                break;
        }
        String createCql = queryCql + ",(timeScaleEvent:TGDA_TimeScaleEvent) WHERE id(timeScaleEvent) = "+ timeScaleEventEntity.getEntityUID() +" CREATE (timeScaleEntity)-[r:"+ Constant.TimeScale_TimeReferToRelationClass+"]->(timeScaleEvent) return r as operationResult";
        logger.debug("Generated Cypher Statement: {}", createCql);
        GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer(Constant.TimeScale_TimeReferToRelationClass,null);
        Object linkRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer,createCql);
        return linkRes != null? (RelationshipEntity)linkRes : null;
    }

    private TimeFlow.TimeScaleGrade getTimeScaleGrade(String timeScaleGradeValue){
        if(timeScaleGradeValue.equals("YEAR")){
            return TimeFlow.TimeScaleGrade.YEAR;
        }else if(timeScaleGradeValue.equals("MONTH")){
            return TimeFlow.TimeScaleGrade.MONTH;
        }else if(timeScaleGradeValue.equals("DAY")){
            return TimeFlow.TimeScaleGrade.DAY;
        }else if(timeScaleGradeValue.equals("HOUR")){
            return TimeFlow.TimeScaleGrade.HOUR;
        }else if(timeScaleGradeValue.equals("MINUTE")){
            return TimeFlow.TimeScaleGrade.MINUTE;
        }else if(timeScaleGradeValue.equals("SECOND")){
            return TimeFlow.TimeScaleGrade.SECOND;
        }
        return null;
    }

    private LocalDateTime getReferTime(Temporal dateTime,TimeFlow.TimeScaleGrade timeScaleGrade){
        if(dateTime instanceof LocalDate){
            LocalDateTime referTime = ((LocalDate) dateTime).atTime(LocalTime.of(0,0,0));
            return referTime;
        }else if(dateTime instanceof LocalDateTime){
            LocalDateTime timeStampDateTime = (LocalDateTime)dateTime;
            int year = timeStampDateTime.getYear();
            int month = timeStampDateTime.getMonthValue();
            int day = timeStampDateTime.getDayOfMonth();
            int hour = timeStampDateTime.getHour();
            int minute = timeStampDateTime.getMinute();
            int second = timeStampDateTime.getSecond();
            LocalDateTime referTime = null;
            switch (timeScaleGrade){
                case YEAR:
                    referTime = LocalDateTime.of(year,1,1,0,0,0);
                    break;
                case MONTH:
                    referTime = LocalDateTime.of(year,month,1,0,0,0);
                    break;
                case DAY:
                    referTime = LocalDateTime.of(year,month,day,0,0,0);
                    break;
                case HOUR:
                    referTime = LocalDateTime.of(year,month,day,hour,0,0);
                    break;
                case MINUTE:
                    referTime = LocalDateTime.of(year,month,day,hour,minute,0);
                    break;
                case SECOND:
                    referTime = LocalDateTime.of(year,month,day,hour,minute,second);
                    break;
            }
            return referTime;
        }else{
            return null;
        }
    }
}
