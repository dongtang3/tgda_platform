package com.github.tgda.engine.core.internal.neo4j.util;

import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.DataTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleEntityTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleRelationshipEntityTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleTimeScaleEntityTransformer;
import com.github.tgda.engine.core.operator.CrossKindDataOperator;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.RelationshipEntityValue;
import com.github.tgda.engine.core.term.*;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.payload.EntitiesAttributesRetrieveResult;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JTimeScaleEntityImpl;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.Constant;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class BatchDataOperationUtil {

    public enum CPUUsageRate {Low, Middle, High}
    private static Logger logger = LoggerFactory.getLogger(BatchDataOperationUtil.class);
    private static ZoneId zone = ZoneId.systemDefault();

    public static Map<String,Object> batchAddNewEntities(String targetConceptionTypeName, List<EntityValue> entityValuesList, CPUUsageRate _CPUUsageRate){
        int degreeOfParallelism = calculateRuntimeCPUCoresByUsageRate(entityValuesList.size(),_CPUUsageRate);
        return batchAddNewEntities(targetConceptionTypeName, entityValuesList,degreeOfParallelism);
    }

    public static Map<String,Object> batchAddNewEntities(String targetConceptionTypeName, List<EntityValue> entityValuesList, int degreeOfParallelism){
        int singlePartitionSize = (entityValuesList.size()/degreeOfParallelism)+1;
        List<List<EntityValue>> rsList = Lists.partition(entityValuesList, singlePartitionSize);
        Map<String,Object> threadReturnDataMap = new Hashtable<>();
        threadReturnDataMap.put("StartTime", LocalDateTime.now());
        ExecutorService executor = Executors.newFixedThreadPool(rsList.size());
        for(List<EntityValue> currentEntityValueList :rsList){
            InsertRecordThread insertRecordThread = new InsertRecordThread(targetConceptionTypeName, currentEntityValueList,threadReturnDataMap);
            executor.execute(insertRecordThread);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        threadReturnDataMap.put("FinishTime", LocalDateTime.now());
        return threadReturnDataMap;
    }

    private static class InsertRecordThread implements Runnable{
        private List<EntityValue> entityValueList;
        private String typeName;
        private Map<String,Object> threadReturnDataMap;

        public InsertRecordThread(String typeName, List<EntityValue> entityValueList, Map<String,Object> threadReturnDataMap){
            this.entityValueList = entityValueList;
            this.typeName = typeName;
            this.threadReturnDataMap = threadReturnDataMap;
        }

        @Override
        public void run(){
            String currentThreadName = Thread.currentThread().getName();
            Engine engine = EngineFactory.getDefaultEngine();
            long successfulCount = 0;
            Type conceptionKind = engine.getType(typeName);
            int singleBatchLoopInsertCount = 1000;
            if(entityValueList.size() <= singleBatchLoopInsertCount){
                engine.openGlobalSession();
                for(EntityValue currentEntityValue : entityValueList){
                    Entity resultEntity = conceptionKind.newEntity(currentEntityValue,false);
                    if(resultEntity != null){
                        successfulCount ++;
                    }
                }
                engine.closeGlobalSession();
            }else{
                GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
                GetSingleEntityTransformer getSingleEntityTransformer =
                        new GetSingleEntityTransformer(this.typeName, graphOperationExecutor);
                List<List<EntityValue>> cutSubLists = Lists.partition(entityValueList, 100);
                ZonedDateTime currentDateTime = ZonedDateTime.now();
                for(List<EntityValue> currentCutList:cutSubLists){
                    List<String> entityParas = new ArrayList<>();
                    for(EntityValue currentEntityValue :currentCutList){
                        Map<String, Object> relationPropertiesValue = currentEntityValue.getEntityAttributesValue();
                        CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesValue,currentDateTime);
                        String propertiesCQLPart = CypherBuilder.createEntityProperties(relationPropertiesValue);
                        entityParas.add(propertiesCQLPart);
                    }
                    String cql = "UNWIND  " + entityParas +" AS entityParas"+"\n "+
                            "CREATE (operationResult:`"+this.typeName +"`)"
                            +"SET operationResult = entityParas";
                    graphOperationExecutor.executeWrite(getSingleEntityTransformer,cql);
                    successfulCount = entityValueList.size();
                }
                graphOperationExecutor.close();
            }
            threadReturnDataMap.put(currentThreadName,successfulCount);
        }
    }

    public static Map<String,Object> batchAttachTimeScaleEvents(List<EntityValue> entityValueList, String timeEventAttributeName, String eventComment,
                                                                Map<String,Object> globalEventData, TimeFlow.TimeScaleGrade timeScaleGrade, CPUUsageRate _CPUUsageRate){
        int degreeOfParallelism = calculateRuntimeCPUCoresByUsageRate(entityValueList.size(),_CPUUsageRate);
        return batchAttachTimeScaleEvents(entityValueList,timeEventAttributeName,eventComment,globalEventData,timeScaleGrade,degreeOfParallelism);
    }

    public static Map<String,Object> batchAttachTimeScaleEvents(List<EntityValue> entityValueList, String timeEventAttributeName, String eventComment,
                                                                Map<String,Object> globalEventData, TimeFlow.TimeScaleGrade timeScaleGrade, int degreeOfParallelism){
        int singlePartitionSize = (entityValueList.size()/degreeOfParallelism)+1;
        List<List<EntityValue>> rsList = Lists.partition(entityValueList, singlePartitionSize);
        Map<String,String> timeScaleEntitiesMetaInfoMapping = new HashMap<>();
        Map<String,Object> threadReturnDataMap = new Hashtable<>();
        threadReturnDataMap.put("StartTime", LocalDateTime.now());
        ExecutorService executor = Executors.newFixedThreadPool(rsList.size());
        for(List<EntityValue> currentEntityValueList :rsList){
            LinkTimeScaleEventThread linkTimeScaleEventThread = new LinkTimeScaleEventThread(timeEventAttributeName,
                    eventComment,globalEventData,timeScaleGrade, currentEntityValueList,timeScaleEntitiesMetaInfoMapping,threadReturnDataMap);
            executor.execute(linkTimeScaleEventThread);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        threadReturnDataMap.put("FinishTime", LocalDateTime.now());
        return threadReturnDataMap;
    }

    private static class LinkTimeScaleEventThread implements Runnable{
        private String timeEventAttributeName;
        private String eventComment;
        private Map<String,Object> globalEventData;
        private TimeFlow.TimeScaleGrade timeScaleGrade;
        private List<EntityValue> entityValueList;
        private Map<String,String> timeScaleEntitiesMetaInfoMapping;
        private Map<String,Object> threadReturnDataMap;
        private DateTimeFormatter formatter;
        public LinkTimeScaleEventThread(String timeEventAttributeName,String eventComment,Map<String,Object> globalEventData,
                                        TimeFlow.TimeScaleGrade timeScaleGrade,List<EntityValue> entityValueList,
                                        Map<String,String> timeScaleEntitiesMetaInfoMapping,Map<String,Object> threadReturnDataMap){
            this.timeEventAttributeName = timeEventAttributeName;
            this.eventComment = eventComment;
            this.globalEventData = globalEventData;
            this.timeScaleGrade = timeScaleGrade;
            this.entityValueList = entityValueList;
            this.timeScaleEntitiesMetaInfoMapping = timeScaleEntitiesMetaInfoMapping;
            this.threadReturnDataMap = threadReturnDataMap;
        }

        public LinkTimeScaleEventThread(String timeEventAttributeName,DateTimeFormatter formatter,String eventComment,Map<String,Object> globalEventData,
                                        TimeFlow.TimeScaleGrade timeScaleGrade,List<EntityValue> entityValueList,
                                        Map<String,String> timeScaleEntitiesMetaInfoMapping,Map<String,Object> threadReturnDataMap){
            this.timeEventAttributeName = timeEventAttributeName;
            this.eventComment = eventComment;
            this.globalEventData = globalEventData;
            this.timeScaleGrade = timeScaleGrade;
            this.entityValueList = entityValueList;
            this.timeScaleEntitiesMetaInfoMapping = timeScaleEntitiesMetaInfoMapping;
            this.threadReturnDataMap = threadReturnDataMap;
            this.formatter = formatter;
        }

        @Override
        public void run() {
            String currentThreadName = Thread.currentThread().getName();
            long successfulCount = 0;
            GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
            for(EntityValue currentEntityValue : entityValueList){
                if(currentEntityValue.getEntityAttributesValue().get(timeEventAttributeName) != null){
                    Object targetDateValue = currentEntityValue.getEntityAttributesValue().get(timeEventAttributeName);
                    boolean attachResult;
                    if(formatter != null){
                        attachResult = attachTimeScaleEventLogic(timeScaleEntitiesMetaInfoMapping, currentEntityValue.getEntityUID(),targetDateValue,formatter,
                            eventComment,globalEventData,timeScaleGrade,graphOperationExecutor);
                    }else{
                        attachResult = attachTimeScaleEventLogic(timeScaleEntitiesMetaInfoMapping, currentEntityValue.getEntityUID(),targetDateValue,
                                eventComment,globalEventData,timeScaleGrade,graphOperationExecutor);
                    }
                    if(attachResult){
                        successfulCount++;
                    }
                }
            }
            graphOperationExecutor.close();
            threadReturnDataMap.put(currentThreadName,successfulCount);
        }
    }

    private static boolean attachTimeScaleEventLogic(Map<String,String> timeScaleEntitiesMetaInfoMapping,String conceptionEntityUID,Object dateTime,String eventComment,
                                                 Map<String,Object> globalEventData,TimeFlow.TimeScaleGrade timeScaleGrade,GraphOperationExecutor workingGraphOperationExecutor){
        Map<String, Object> propertiesMap = new HashMap<>();
        if(globalEventData != null){
            propertiesMap.putAll(globalEventData);
        }
        CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
        TimeFlow.TimeScaleGrade referTimeScaleGrade = timeScaleGrade;
        LocalDateTime eventReferTime = null;
        if(dateTime instanceof ZonedDateTime){
            eventReferTime = ((ZonedDateTime)dateTime).toLocalDateTime();
        }else if(dateTime instanceof LocalDateTime){
            eventReferTime = (LocalDateTime)dateTime;
        }else if(dateTime instanceof LocalDate){
            eventReferTime = ((LocalDate) dateTime).atTime(LocalTime.of(0,0,0));
        }else if(dateTime instanceof Date){
            Instant instant = ((Date)dateTime).toInstant();
            eventReferTime = LocalDateTime.ofInstant(instant, zone);
        }
        propertiesMap.put(Constant._TimeScaleEventReferTime,eventReferTime);
        propertiesMap.put(Constant._TimeScaleEventComment,eventComment);
        propertiesMap.put(Constant._TimeScaleEventScaleGrade,""+referTimeScaleGrade);
        propertiesMap.put(Constant._TimeScaleEventTimeFlow, Constant._defaultTimeFlowName);

        String createEventCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.TimeScaleEventClass}, propertiesMap);
        GetSingleEntityTransformer getSingleEntityTransformer =
                new GetSingleEntityTransformer(Constant.TimeScaleEventClass, workingGraphOperationExecutor);
        Object newEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer, createEventCql);
        if(newEntityRes != null) {
            String timeEventUID = ((Entity)newEntityRes).getEntityUID();
            Map<String,Object> relationPropertiesMap = new HashMap<>();
            CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesMap);
            String createCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(conceptionEntityUID),Long.parseLong(timeEventUID), Constant.TimeScale_AttachToRelationClass,relationPropertiesMap);
            GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                    (Constant.TimeScale_AttachToRelationClass,workingGraphOperationExecutor);
            Object newRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, createCql);
            if(newRelationshipEntityRes != null){
                String timeScaleEntityUID = getTimeScaleEntityUID(timeScaleEntitiesMetaInfoMapping,eventReferTime, Constant._defaultTimeFlowName, referTimeScaleGrade, workingGraphOperationExecutor);
                if(timeScaleEntityUID != null){
                    String linkToTimeScaleEntityCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(timeScaleEntityUID),Long.parseLong(timeEventUID), Constant.TimeScale_TimeReferToRelationClass,relationPropertiesMap);
                    workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, linkToTimeScaleEntityCql);
                }
            }
        }
        return true;
    }

    private static boolean attachTimeScaleEventLogic(Map<String,String> timeScaleEntitiesMetaInfoMapping,String conceptionEntityUID,Object dateTime,DateTimeFormatter formatter,String eventComment,
                                                     Map<String,Object> globalEventData,TimeFlow.TimeScaleGrade timeScaleGrade,GraphOperationExecutor workingGraphOperationExecutor){
        Map<String, Object> propertiesMap = new HashMap<>();
        if(globalEventData != null){
            propertiesMap.putAll(globalEventData);
        }
        CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
        TimeFlow.TimeScaleGrade referTimeScaleGrade = timeScaleGrade;
        try {
            LocalDateTime eventReferTime = LocalDateTime.parse(dateTime.toString(), formatter);

            propertiesMap.put(Constant._TimeScaleEventReferTime, eventReferTime);
            propertiesMap.put(Constant._TimeScaleEventComment,eventComment);
            propertiesMap.put(Constant._TimeScaleEventScaleGrade,""+referTimeScaleGrade);
            propertiesMap.put(Constant._TimeScaleEventTimeFlow, Constant._defaultTimeFlowName);

            String createEventCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.TimeScaleEventClass}, propertiesMap);
            GetSingleEntityTransformer getSingleEntityTransformer =
                    new GetSingleEntityTransformer(Constant.TimeScaleEventClass, workingGraphOperationExecutor);
            Object newEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer, createEventCql);
            if(newEntityRes != null) {
                String timeEventUID = ((Entity)newEntityRes).getEntityUID();
                Map<String,Object> relationPropertiesMap = new HashMap<>();
                CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesMap);
                String createCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(conceptionEntityUID),Long.parseLong(timeEventUID), Constant.TimeScale_AttachToRelationClass,relationPropertiesMap);
                GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (Constant.TimeScale_AttachToRelationClass,workingGraphOperationExecutor);
                Object newRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, createCql);
                if(newRelationshipEntityRes != null){
                    String timeScaleEntityUID = getTimeScaleEntityUID(timeScaleEntitiesMetaInfoMapping,eventReferTime, Constant._defaultTimeFlowName, referTimeScaleGrade, workingGraphOperationExecutor);
                    if(timeScaleEntityUID != null){
                        String linkToTimeScaleEntityCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(timeScaleEntityUID),Long.parseLong(timeEventUID), Constant.TimeScale_TimeReferToRelationClass,relationPropertiesMap);
                        workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, linkToTimeScaleEntityCql);
                    }
                }
            }
            return true;
        }catch(DateTimeParseException e){
            e.printStackTrace();
        }
        return false;
    }

    public static Map<String,Object> batchAttachTimeScaleEventsWithStringDateAttributeValue(List<EntityValue> entityValueList, String timeEventAttributeName, String eventComment,
                                                                                            DateTimeFormatter formatter, Map<String,Object> globalEventData, TimeFlow.TimeScaleGrade timeScaleGrade, CPUUsageRate _CPUUsageRate){
        int degreeOfParallelism = calculateRuntimeCPUCoresByUsageRate(entityValueList.size(),_CPUUsageRate);
        return batchAttachTimeScaleEvents(entityValueList,timeEventAttributeName,eventComment,formatter,globalEventData,timeScaleGrade,degreeOfParallelism);
    }

    public static Map<String,Object> batchAttachTimeScaleEvents(List<EntityValue> entityValueList, String timeEventAttributeName, String eventComment,
                                                                DateTimeFormatter formatter, Map<String,Object> globalEventData, TimeFlow.TimeScaleGrade timeScaleGrade, int degreeOfParallelism){
        int singlePartitionSize = (entityValueList.size()/degreeOfParallelism)+1;
        List<List<EntityValue>> rsList = Lists.partition(entityValueList, singlePartitionSize);
        Map<String,String> timeScaleEntitiesMetaInfoMapping = new HashMap<>();
        Map<String,Object> threadReturnDataMap = new Hashtable<>();
        threadReturnDataMap.put("StartTime", LocalDateTime.now());
        ExecutorService executor = Executors.newFixedThreadPool(rsList.size());
        for(List<EntityValue> currentEntityValueList :rsList){
            LinkTimeScaleEventThread linkTimeScaleEventThread = new LinkTimeScaleEventThread(timeEventAttributeName,formatter,
                    eventComment,globalEventData,timeScaleGrade, currentEntityValueList,timeScaleEntitiesMetaInfoMapping,threadReturnDataMap);
            executor.execute(linkTimeScaleEventThread);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        threadReturnDataMap.put("FinishTime", LocalDateTime.now());
        return threadReturnDataMap;
    }

    private static String getTimeScaleEntityUID(Map<String,String> timeScaleEntitiesMetaInfoMapping,LocalDateTime eventReferTime, String timeFlowName, TimeFlow.TimeScaleGrade timeScaleGrade,GraphOperationExecutor workingGraphOperationExecutor){
        int year = eventReferTime.getYear();
        int month = eventReferTime.getMonthValue();
        int day = eventReferTime.getDayOfMonth();
        int hour = eventReferTime.getHour();
        int minute = eventReferTime.getMinute();
        //int second = eventReferTime.getSecond();

        String TimeScaleEntityKey = timeFlowName+":"+year+"_"+month+"_"+day+"_"+hour+"_"+minute;

        if(timeScaleEntitiesMetaInfoMapping.containsKey(TimeScaleEntityKey)){
            return timeScaleEntitiesMetaInfoMapping.get(TimeScaleEntityKey);
        }else{
            String queryCql = null;
            switch (timeScaleGrade) {
                case YEAR:
                    queryCql ="MATCH(timeFlow:TGDA_TimeFlow{name:\""+timeFlowName+"\"})-[:TGDA_TS_Contains]->(timeScaleEntity:TGDA_TS_Year{year:"+year+"}) RETURN timeScaleEntity as operationResult";
                    break;
                case MONTH:
                    queryCql ="MATCH(timeFlow:TGDA_TimeFlow{name:\""+timeFlowName+"\"})-[:TGDA_TS_Contains]->(year:TGDA_TS_Year{year:"+year+"})-[:TGDA_TS_Contains]->(timeScaleEntity:TGDA_TS_Month{month:"+month+"}) RETURN timeScaleEntity as operationResult";
                    break;
                case DAY:
                    queryCql ="MATCH(timeFlow:TGDA_TimeFlow{name:\""+timeFlowName+"\"})-[:TGDA_TS_Contains]->(year:TGDA_TS_Year{year:"+year+"})-[:TGDA_TS_Contains]->(month:TGDA_TS_Month{month:"+month+"})-[:TGDA_TS_Contains]->(timeScaleEntity:TGDA_TS_Day{day:"+day+"}) RETURN timeScaleEntity as operationResult";
                    break;
                case HOUR:
                    queryCql = "MATCH(timeFlow:TGDA_TimeFlow{name:\""+timeFlowName+"\"})-[:TGDA_TS_Contains]->(year:TGDA_TS_Year{year:"+year+"})-[:TGDA_TS_Contains]->(month:TGDA_TS_Month{month:"+month+"})-[:TGDA_TS_Contains]->(day:TGDA_TS_Day{day:"+day+"})-[:TGDA_TS_Contains]->(timeScaleEntity:TGDA_TS_Hour{hour:"+hour+"}) RETURN timeScaleEntity as operationResult";
                    break;
                case MINUTE:
                    queryCql = "MATCH(timeFlow:TGDA_TimeFlow{name:\""+timeFlowName+"\"})-[:TGDA_TS_Contains]->(year:TGDA_TS_Year{year:"+year+"})-[:TGDA_TS_Contains]->(month:TGDA_TS_Month{month:"+month+"})-[:TGDA_TS_Contains]->(day:TGDA_TS_Day{day:"+day+"})-[:TGDA_TS_Contains]->(hour:TGDA_TS_Hour{hour:"+hour+"})-[:TGDA_TS_Contains]->(timeScaleEntity:TGDA_TS_Minute{minute:"+minute+"}) RETURN timeScaleEntity as operationResult";
                    break;
                case SECOND:
                    break;
            }
            logger.debug("Generated Cypher Statement: {}", queryCql);
            GetSingleTimeScaleEntityTransformer getSingleTimeScaleEntityTransformer =
                    new GetSingleTimeScaleEntityTransformer(null,workingGraphOperationExecutor);
            Object queryRes = workingGraphOperationExecutor.executeRead(getSingleTimeScaleEntityTransformer,queryCql);
            if(queryRes != null){
                String timeScaleEntityUID = ((Neo4JTimeScaleEntityImpl)queryRes).getTimeScaleEntityUID();
                timeScaleEntitiesMetaInfoMapping.put(TimeScaleEntityKey,timeScaleEntityUID);
                return timeScaleEntityUID;
            }
        }
        return null;
    }

    public static Map<String,Object> batchAttachNewRelations(List<RelationshipEntityValue> relationshipEntityValueList, String relationKindName, CPUUsageRate _CPUUsageRate){
        int degreeOfParallelism = calculateRuntimeCPUCoresByUsageRate(relationshipEntityValueList.size(),_CPUUsageRate);
        return batchAttachNewRelations(relationshipEntityValueList,relationKindName,degreeOfParallelism);
    }

    public static Map<String,Object> batchAttachNewRelations(List<RelationshipEntityValue> relationshipEntityValueList, String relationKindName, int degreeOfParallelism){
        int singlePartitionSize = (relationshipEntityValueList.size()/degreeOfParallelism)+1;
        List<List<RelationshipEntityValue>> rsList = Lists.partition(relationshipEntityValueList, singlePartitionSize);
        Map<String,Object> threadReturnDataMap = new Hashtable<>();
        threadReturnDataMap.put("StartTime", LocalDateTime.now());
        if(rsList.size() > 0){
            ExecutorService executor = Executors.newFixedThreadPool(rsList.size());
            for(List<RelationshipEntityValue> currentRelationshipEntityValueList :rsList){
                AttachRelationThread attachRelationThread = new AttachRelationThread(currentRelationshipEntityValueList,relationKindName,threadReturnDataMap);
                executor.execute(attachRelationThread);
            }
            executor.shutdown();
            try {
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            threadReturnDataMap.put("FinishTime", LocalDateTime.now());
        }
        return threadReturnDataMap;
    }

    private static class AttachRelationThread implements Runnable{
        private List<RelationshipEntityValue> relationshipEntityValueList;
        private String relationKindName;
        private Map<String,Object> threadReturnDataMap;

        public AttachRelationThread(List<RelationshipEntityValue> relationshipEntityValueList, String relationKindName, Map<String,Object> threadReturnDataMap){
            this.relationshipEntityValueList = relationshipEntityValueList;
            this.relationKindName = relationKindName;
            this.threadReturnDataMap = threadReturnDataMap;
        }

        @Override
        public void run(){
            if(this.relationshipEntityValueList != null && this.relationshipEntityValueList.size() >0){
                String currentThreadName = Thread.currentThread().getName();
                long successfulCount = 0;
                Engine coreRealm = EngineFactory.getDefaultEngine();
                if(coreRealm.getStorageImplTech().equals(StorageImplTech.NEO4J)){
                    GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
                    GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                            (this.relationKindName,graphOperationExecutor);
                    int singleBatchLoopInsertCount = 1000;

                    if(this.relationshipEntityValueList.size() <= singleBatchLoopInsertCount){
                        for(RelationshipEntityValue currentRelationshipEntityValue :this.relationshipEntityValueList){
                            String sourceEntityUID = currentRelationshipEntityValue.getFromEntityUID();
                            String targetEntityUID = currentRelationshipEntityValue.getToEntityUID();
                            Map<String, Object> relationPropertiesValue = currentRelationshipEntityValue.getEntityAttributesValue();
                            String attachRelationCQL = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(sourceEntityUID),Long.parseLong(targetEntityUID),this.relationKindName,relationPropertiesValue);
                            Object returnObj = graphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer,attachRelationCQL);
                            if(returnObj != null){
                                successfulCount++;
                            }
                        }
                    }else{
                        List<List<RelationshipEntityValue>> cutSubLists = Lists.partition(relationshipEntityValueList, 100);
                        for(List<RelationshipEntityValue> currentCutList:cutSubLists){
                            List<List<String>> relationParas = new ArrayList<>();
                            for(RelationshipEntityValue currentRelationshipEntityValue :currentCutList){
                                String sourceEntityUID = currentRelationshipEntityValue.getFromEntityUID();
                                String targetEntityUID = currentRelationshipEntityValue.getToEntityUID();
                                Map<String, Object> relationPropertiesValue = currentRelationshipEntityValue.getEntityAttributesValue();
                                String propertiesCQLPart = CypherBuilder.createEntityProperties(relationPropertiesValue);
                                List<String> currentPairList = new ArrayList<>();
                                currentPairList.add(sourceEntityUID);
                                currentPairList.add(targetEntityUID);
                                currentPairList.add(propertiesCQLPart);
                                relationParas.add(currentPairList);
                            }
                            String cql = "UNWIND  "+relationParas +" AS relationPair"+"\n "+
                            "MATCH (sourceNode), (targetNode) WHERE (id(sourceNode) = relationPair[0] AND id(targetNode) = relationPair[1]) CREATE (sourceNode)-[operationResult:`"+this.relationKindName+"`]->(targetNode)"+" \n "+
                                    "SET operationResult = relationPair[2]";
                            graphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer,cql);
                            successfulCount = relationshipEntityValueList.size();
                        }
                    }
                    graphOperationExecutor.close();
                    threadReturnDataMap.put(currentThreadName,successfulCount);
                }
            }
        }
    }

    public static Map<String,Object> batchAttachNewRelationsWithSinglePropertyValueMatch(
            String fromTypeName,QueryParameters fromExploreParameters,String fromAttributeName,
            String toTypeName, QueryParameters toExploreParameters,String toAttributeName,
            String relationKindName,CPUUsageRate _CPUUsageRate){
        LocalDateTime wholeStartDateTime = LocalDateTime.now();
        List<RelationshipEntityValue> relationshipEntityValueList = new ArrayList<>();
        Engine coreRealm = EngineFactory.getDefaultEngine();
        coreRealm.openGlobalSession();

        QueryParameters exeFromExploreParameters;
        if(fromExploreParameters != null){
            exeFromExploreParameters = fromExploreParameters;
        }else{
            exeFromExploreParameters = new QueryParameters();
            exeFromExploreParameters.setResultNumber(10000000);
        }

        QueryParameters exeToExploreParameters;
        if(toExploreParameters != null){
            exeToExploreParameters = toExploreParameters;
        }else{
            exeToExploreParameters = new QueryParameters();
            exeToExploreParameters.setResultNumber(10000000);
        }

        try {
            Type fromType = coreRealm.getType(fromTypeName);
            List<String> fromTypeAttributeList = new ArrayList<>();
            fromTypeAttributeList.add(fromAttributeName);
            EntitiesAttributesRetrieveResult fromEntitiesAttributesRetrieveResult =
                    fromType.getSingleValueEntityAttributesByAttributeNames(fromTypeAttributeList,exeFromExploreParameters);

            List<EntityValue> fromEntityValues = fromEntitiesAttributesRetrieveResult.getEntityValues();
            Multimap<Object,String> fromConceptionEntitiesValue_UIDMapping = ArrayListMultimap.create();
            for(EntityValue currentEntityValue : fromEntityValues){
                String conceptionEntityUID = currentEntityValue.getEntityUID();
                Object fromAttributeValue = currentEntityValue.getEntityAttributesValue().get(fromAttributeName);
                fromConceptionEntitiesValue_UIDMapping.put(fromAttributeValue,conceptionEntityUID);
            }

            Type toType = coreRealm.getType(toTypeName);
            List<String> toTypeAttributeList = new ArrayList<>();
            toTypeAttributeList.add(toAttributeName);
            EntitiesAttributesRetrieveResult toEntitiesAttributesRetrieveResult =
                    toType.getSingleValueEntityAttributesByAttributeNames(toTypeAttributeList,exeToExploreParameters);

            List<EntityValue> toEntityValues = toEntitiesAttributesRetrieveResult.getEntityValues();
            for(EntityValue currentEntityValue : toEntityValues){
                String conceptionEntityUID = currentEntityValue.getEntityUID();
                Object toAttributeValue = currentEntityValue.getEntityAttributesValue().get(toAttributeName);
                Collection<String> fromEntityUIDCollection = fromConceptionEntitiesValue_UIDMapping.get(toAttributeValue);
                if(fromEntityUIDCollection != null & fromEntityUIDCollection.size()>0){
                    for(String currentFromEntityUID:fromEntityUIDCollection){
                        RelationshipEntityValue relationshipEntityValue = new RelationshipEntityValue(null,currentFromEntityUID,conceptionEntityUID,null);
                        relationshipEntityValueList.add(relationshipEntityValue);
                    }
                }
            }
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }
        coreRealm.closeGlobalSession();
        if(relationshipEntityValueList.size()>0){
            Map<String,Object> batchLoadResultMap = BatchDataOperationUtil.batchAttachNewRelations(relationshipEntityValueList,relationKindName,_CPUUsageRate);
            batchLoadResultMap.put("StartTime", wholeStartDateTime);
            return batchLoadResultMap;
        }else{
            return null;
        }
    }

    public static Map<String,Object> batchDeleteEntities(List<String> conceptionEntityUIDs,CPUUsageRate _CPUUsageRate){
        int degreeOfParallelism = calculateRuntimeCPUCoresByUsageRate(conceptionEntityUIDs.size(),_CPUUsageRate);
        return batchDeleteEntities(conceptionEntityUIDs,degreeOfParallelism);
    }

    public static Map<String,Object> batchDeleteEntities(List<String> conceptionEntityUIDs,int degreeOfParallelism){
        int singlePartitionSize = (conceptionEntityUIDs.size()/degreeOfParallelism)+1;
        List<List<String>> rsList = Lists.partition(conceptionEntityUIDs, singlePartitionSize);
        Map<String,Object> threadReturnDataMap = new Hashtable<>();
        threadReturnDataMap.put("StartTime", LocalDateTime.now());
        ExecutorService executor = Executors.newFixedThreadPool(rsList.size());
        for(List<String> currentEntityUIDList:rsList){
            DeleteEntityThread deleteEntityThread = new DeleteEntityThread(currentEntityUIDList,threadReturnDataMap);
            executor.execute(deleteEntityThread);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        threadReturnDataMap.put("FinishTime", LocalDateTime.now());
        return threadReturnDataMap;
    }

    private static class DeleteEntityThread implements Runnable{
        private List<String> conceptionEntityUIDList;
        private Map<String,Object> threadReturnDataMap;

        public DeleteEntityThread(List<String> conceptionEntityUIDList,Map<String,Object> threadReturnDataMap){
            this.conceptionEntityUIDList = conceptionEntityUIDList;
            this.threadReturnDataMap = threadReturnDataMap;
        }

        @Override
        public void run(){
            if(this.conceptionEntityUIDList != null && this.conceptionEntityUIDList.size() >0){
                String currentThreadName = Thread.currentThread().getName();
                long successfulCount = 0;
                Engine coreRealm = EngineFactory.getDefaultEngine();
                if(coreRealm.getStorageImplTech().equals(StorageImplTech.NEO4J)){
                    GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
                    DataTransformer<Boolean> singleDeleteDataTransformer = new DataTransformer() {
                        @Override
                        public Object transformResult(Result result) {
                            if(result != null & result.hasNext()){
                                if(result.next()!=null){
                                    return true;
                                }
                            }
                            return false;
                        }
                    };
                    for(String currentEntityUID:this.conceptionEntityUIDList){
                        String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.valueOf(currentEntityUID),null,null);
                        Object returnObj = graphOperationExecutor.executeWrite(singleDeleteDataTransformer,deleteCql);
                        if(returnObj != null){
                            successfulCount++;
                        }
                    }
                    graphOperationExecutor.close();
                    threadReturnDataMap.put(currentThreadName,successfulCount);
                }
            }
        }
    }

    public static Map<String,Object> batchAddNewOrUpdateEntityAttributes(String conceptionEntityUIDKeyName,List<Map<String,Object>> entityPropertiesValueList,CPUUsageRate _CPUUsageRate){
        int degreeOfParallelism = calculateRuntimeCPUCoresByUsageRate(entityPropertiesValueList.size(),_CPUUsageRate);
        return batchAddNewOrUpdateEntityAttributes(conceptionEntityUIDKeyName,entityPropertiesValueList,degreeOfParallelism);
    }

    public static Map<String,Object> batchAddNewOrUpdateEntityAttributes(String conceptionEntityUIDKeyName,List<Map<String,Object>> entityPropertiesValueList,int degreeOfParallelism){
        int singlePartitionSize = (entityPropertiesValueList.size()/degreeOfParallelism)+1;
        List<List<Map<String,Object>>> rsList = Lists.partition(entityPropertiesValueList, singlePartitionSize);
        Map<String,Object> threadReturnDataMap = new Hashtable<>();

        threadReturnDataMap.put("StartTime", LocalDateTime.now());
        ExecutorService executor = Executors.newFixedThreadPool(rsList.size());
        for(List<Map<String,Object>> currentEntityPropertiesValueList:rsList){
            AddNewOrUpdateEntityAttributeThread addNewOrUpdateEntityAttributeThread = new AddNewOrUpdateEntityAttributeThread(conceptionEntityUIDKeyName,currentEntityPropertiesValueList,threadReturnDataMap);
            executor.execute(addNewOrUpdateEntityAttributeThread);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        threadReturnDataMap.put("FinishTime", LocalDateTime.now());
        return threadReturnDataMap;
    }

    private static class AddNewOrUpdateEntityAttributeThread implements Runnable{
        private String conceptionEntityUIDKeyName;
        private List<Map<String,Object>> entityPropertiesValueList;
        private Map<String,Object> threadReturnDataMap;

        public AddNewOrUpdateEntityAttributeThread(String conceptionEntityUIDKeyName,List<Map<String,Object>> entityPropertiesValueList,Map<String,Object> threadReturnDataMap){
            this.conceptionEntityUIDKeyName = conceptionEntityUIDKeyName;
            this.entityPropertiesValueList = entityPropertiesValueList;
            this.threadReturnDataMap = threadReturnDataMap;
        }

        @Override
        public void run() {
            if(this.conceptionEntityUIDKeyName != null && this.entityPropertiesValueList.size() >0){
                String currentThreadName = Thread.currentThread().getName();
                long successfulCount = 0;
                Engine coreRealm = EngineFactory.getDefaultEngine();
                coreRealm.openGlobalSession();
                CrossKindDataOperator crossKindDataOperator = coreRealm.getCrossKindDataOperator();

                if(coreRealm.getStorageImplTech().equals(StorageImplTech.NEO4J)){

                    Map<String,Map<String,Object>> entityUID_EntityMapping = new HashMap<>();
                    List<String> conceptionEntityUIDList = new ArrayList<>();
                    for(Map<String,Object> currentEntityProperties:this.entityPropertiesValueList){
                        Object currentEntityUIDObj = currentEntityProperties.get(this.conceptionEntityUIDKeyName);
                        if(currentEntityUIDObj != null){
                            String currentEntityUID = currentEntityUIDObj.toString();
                            currentEntityProperties.remove(this.conceptionEntityUIDKeyName);
                            entityUID_EntityMapping.put(currentEntityUID,currentEntityProperties);
                            conceptionEntityUIDList.add(currentEntityUID);
                        }
                    }
                    try {
                        List<Entity> resultEntityList = crossKindDataOperator.getConceptionEntitiesByUIDs(conceptionEntityUIDList);
                        if(resultEntityList != null){
                            for(Entity currentEntity:resultEntityList){
                                String currentUID = currentEntity.getEntityUID();
                                Map<String,Object> currentEntityProperties = entityUID_EntityMapping.get(currentUID);
                                List<String> updatedProperties = currentEntity.addNewOrUpdateAttributes(currentEntityProperties);
                                if(updatedProperties != null & updatedProperties.size() == currentEntityProperties.size()){
                                    successfulCount++;
                                }
                            }
                        }
                    } catch (EngineServiceEntityExploreException e) {
                        e.printStackTrace();
                    }

                    coreRealm.closeGlobalSession();
                    threadReturnDataMap.put(currentThreadName,successfulCount);
                }
            }
        }
    }

    public static Map<String,Object> batchAttachGeospatialScaleEvents(List<RelationshipEntityValue> relationshipEntityValueList, String eventComment, Map<String,Object> globalEventData,
                                                                      Geospatial.GeospatialScaleGrade geospatialScaleGrade, CPUUsageRate _CPUUsageRate){
        int degreeOfParallelism = calculateRuntimeCPUCoresByUsageRate(relationshipEntityValueList.size(),_CPUUsageRate);
        return batchAttachGeospatialScaleEvents(relationshipEntityValueList,eventComment,globalEventData,geospatialScaleGrade,degreeOfParallelism);
    }

    public static Map<String,Object> batchAttachGeospatialScaleEvents(List<RelationshipEntityValue> relationshipEntityValueList, String eventComment, Map<String,Object> globalEventData,
                                                                      Geospatial.GeospatialScaleGrade geospatialScaleGrade, int degreeOfParallelism){
        int singlePartitionSize = (relationshipEntityValueList.size()/degreeOfParallelism)+1;
        List<List<RelationshipEntityValue>> rsList = Lists.partition(relationshipEntityValueList, singlePartitionSize);
        Map<String,Object> threadReturnDataMap = new Hashtable<>();
        threadReturnDataMap.put("StartTime", LocalDateTime.now());
        ExecutorService executor = Executors.newFixedThreadPool(rsList.size());
        for(List<RelationshipEntityValue> currentRelationshipEntityValueList :rsList){
            LinkGeospatialScaleEventThread linkGeospatialScaleEventThread = new LinkGeospatialScaleEventThread(Constant._defaultGeospatialName,
                    eventComment,globalEventData,geospatialScaleGrade, currentRelationshipEntityValueList,threadReturnDataMap);
            executor.execute(linkGeospatialScaleEventThread);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        threadReturnDataMap.put("FinishTime", LocalDateTime.now());
        return threadReturnDataMap;
    }

    private static class LinkGeospatialScaleEventThread implements Runnable{
        private String geospatialRegionName;
        private String eventComment;
        private Map<String,Object> globalEventData;
        private Geospatial.GeospatialScaleGrade geospatialScaleGrade;
        private List<RelationshipEntityValue> relationshipEntityValueList;

        private Map<String,Object> threadReturnDataMap;
        public LinkGeospatialScaleEventThread(String geospatialRegionName,String eventComment,Map<String,Object> globalEventData,
                                              Geospatial.GeospatialScaleGrade geospatialScaleGrade,List<RelationshipEntityValue> relationshipEntityValueList,
                                              Map<String,Object> threadReturnDataMap){
            this.geospatialRegionName = geospatialRegionName;
            this.eventComment = eventComment;
            this.globalEventData = globalEventData;
            this.geospatialScaleGrade = geospatialScaleGrade;
            this.relationshipEntityValueList = relationshipEntityValueList;
            this.threadReturnDataMap = threadReturnDataMap;
        }

        @Override
        public void run() {
            String currentThreadName = Thread.currentThread().getName();
            long successfulCount = 0;
            GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
            try{
                for(RelationshipEntityValue currentRelationshipEntityValue : relationshipEntityValueList){
                    String conceptionEntityUID = currentRelationshipEntityValue.getFromEntityUID();
                    String geospatialScaleEntityUID = currentRelationshipEntityValue.getToEntityUID();
                    Map<String,Object> linkDataMap = currentRelationshipEntityValue.getEntityAttributesValue();
                    if(conceptionEntityUID != null && geospatialScaleEntityUID != null && linkDataMap != null){
                        Map<String, Object> propertiesMap = linkDataMap ;

                        if(linkDataMap.containsKey(Constant.GeospatialCodeProperty)){
                            CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
                            propertiesMap.put(Constant._GeospatialScaleEventReferLocation,linkDataMap.get(Constant.GeospatialCodeProperty));
                            propertiesMap.put(Constant._GeospatialScaleEventComment,this.eventComment);
                            propertiesMap.put(Constant._GeospatialScaleEventScaleGrade,this.geospatialScaleGrade.toString());
                            propertiesMap.put(Constant._GeospatialScaleEventGeospatial,this.geospatialRegionName);
                            if(this.globalEventData!=null){
                                propertiesMap.putAll(this.globalEventData);
                            }
                            propertiesMap.remove(Constant.GeospatialCodeProperty);
                            String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.GeospatialScaleEventClass}, propertiesMap);
                            logger.debug("Generated Cypher Statement: {}", createCql);
                            GetSingleEntityTransformer getSingleEntityTransformer =
                                    new GetSingleEntityTransformer(Constant.GeospatialScaleEventClass, graphOperationExecutor);
                            Object newEntityRes = graphOperationExecutor.executeWrite(getSingleEntityTransformer, createCql);
                            if(newEntityRes != null) {
                                Entity geospatialScaleEventEntity = (Entity) newEntityRes;
                                RelationshipEntity linkToEntityRelation = geospatialScaleEventEntity.attachToRelation(conceptionEntityUID, Constant.GeospatialScale_AttachToRelationClass, null, true);
                                RelationshipEntity linkToGeospatialScaleEntityRelation = geospatialScaleEventEntity.attachToRelation(geospatialScaleEntityUID, Constant.GeospatialScale_GeospatialReferToRelationClass, null, true);
                                if(linkToGeospatialScaleEntityRelation != null && linkToEntityRelation!= null){
                                    successfulCount++;
                                }
                            }
                        }
                    }
                }
            } catch (EngineServiceRuntimeException e) {
                e.printStackTrace();
            }finally {
                graphOperationExecutor.close();
            }
            threadReturnDataMap.put(currentThreadName,successfulCount);
        }
    }

    public static Map<String,Object> batchAttachGeospatialScaleEventsByGeospatialCode(Map<String,String> entityUIDAndGeospatialCodeMap,
                                                    String eventComment,Map<String,Object> globalEventData,Geospatial.GeospatialScaleGrade geospatialScaleGrade, CPUUsageRate _CPUUsageRate){
        Map<String,String> geospatialScaleEntityUIDAndCodeMap = new HashMap<>();
        GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
        try{
            QueryParameters geospatialScaleEntityQueryParameters = new QueryParameters();
            geospatialScaleEntityQueryParameters.setResultNumber(10000000);
            List<String> attributeNamesList = new ArrayList<>();
            attributeNamesList.add(Constant.GeospatialCodeProperty);
            String queryCql = CypherBuilder.matchAttributesWithQueryParameters(Constant.GeospatialScaleEntityClass,geospatialScaleEntityQueryParameters,attributeNamesList);
            DataTransformer geospatialCodeSearchDataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    while(result.hasNext()){
                        Record nodeRecord = result.next();
                        Map<String,Object> valueMap = nodeRecord.asMap();
                        String idKey = "id("+CypherBuilder.operationResultName+")";
                        Long uidValue = (Long)valueMap.get(idKey);
                        String geospatialScaleEntityUID = ""+uidValue.longValue();
                        String geospatialCodeKey = CypherBuilder.operationResultName+"."+ Constant.GeospatialCodeProperty;
                        String geospatialCodeValue = valueMap.get(geospatialCodeKey).toString();
                        geospatialScaleEntityUIDAndCodeMap.put(geospatialCodeValue,geospatialScaleEntityUID);
                    }
                    return null;
                }
            };
            graphOperationExecutor.executeRead(geospatialCodeSearchDataTransformer, queryCql);
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }finally {
            graphOperationExecutor.close();
        }
        List<RelationshipEntityValue> attachEntityMetaDataList = new ArrayList<>();
        Iterator<Map.Entry<String, String>> entries = entityUIDAndGeospatialCodeMap.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, String> entry = entries.next();
            String conceptionEntityUID = entry.getKey();
            String targetGeospatialCode = entry.getValue();

            if(geospatialScaleEntityUIDAndCodeMap.containsKey(targetGeospatialCode)) {
                RelationshipEntityValue relationshipEntityValue = new RelationshipEntityValue();
                relationshipEntityValue.setFromEntityUID(conceptionEntityUID);
                relationshipEntityValue.setToEntityUID(geospatialScaleEntityUIDAndCodeMap.get(targetGeospatialCode));
                Map<String,Object> geospatialCodePropertyDataMap = new HashMap<>();
                geospatialCodePropertyDataMap.put(Constant.GeospatialCodeProperty,targetGeospatialCode);
                relationshipEntityValue.setEntityAttributesValue(geospatialCodePropertyDataMap);
                attachEntityMetaDataList.add(relationshipEntityValue);
            }
        }
        return batchAttachGeospatialScaleEvents(attachEntityMetaDataList,eventComment,globalEventData,geospatialScaleGrade,_CPUUsageRate);
    }

    public static Map<String,Object> batchAttachGeospatialScaleEventsByChineseNames(Map<String,String> entityUIDAndGeospatialChinaNamesMap,
                                                                                      String eventComment,Map<String,Object> globalEventData,Geospatial.GeospatialScaleGrade geospatialScaleGrade, CPUUsageRate _CPUUsageRate){
        Map<String,String> geospatialScaleEntityUIDAndChinaNamesMap = new HashMap<>();
        Map<String,String> geospatialScaleEntityUIDAndCodeMap = new HashMap<>();
        GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
        try{
            QueryParameters geospatialScaleEntityQueryParameters = new QueryParameters();
            geospatialScaleEntityQueryParameters.setResultNumber(10000000);
            List<String> attributeNamesList = new ArrayList<>();
            attributeNamesList.add(Constant.GeospatialChineseNameProperty);
            attributeNamesList.add(Constant.GeospatialCodeProperty);

            String _GeospatialScaleEntityClassName = Constant.GeospatialScaleEntityClass;
            switch(geospatialScaleGrade){
                case PROVINCE: _GeospatialScaleEntityClassName = Constant.GeospatialScaleProvinceEntityClass;
                    break;
                case PREFECTURE:_GeospatialScaleEntityClassName = Constant.GeospatialScalePrefectureEntityClass;
                    attributeNamesList.add("ChinaProvinceName");
                    break;
                case COUNTY:_GeospatialScaleEntityClassName = Constant.GeospatialScaleCountyEntityClass;
                    attributeNamesList.add("ChinaProvinceName");
                    attributeNamesList.add("ChinaPrefectureName");
                    break;
                case TOWNSHIP:_GeospatialScaleEntityClassName = Constant.GeospatialScaleTownshipEntityClass;
                    attributeNamesList.add("ChinaProvinceName");
                    attributeNamesList.add("ChinaPrefectureName");
                    attributeNamesList.add("ChinaCountyName");
                    break;
                case VILLAGE:_GeospatialScaleEntityClassName = Constant.GeospatialScaleVillageEntityClass;
                    attributeNamesList.add("ChinaProvinceName");
                    attributeNamesList.add("ChinaPrefectureName");
                    attributeNamesList.add("ChinaCountyName");
                    attributeNamesList.add("ChinaTownshipName");
                    break;
            }

            String queryCql = CypherBuilder.matchAttributesWithQueryParameters(_GeospatialScaleEntityClassName,geospatialScaleEntityQueryParameters,attributeNamesList);
            DataTransformer geospatialCodeSearchDataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    while(result.hasNext()){
                        Record nodeRecord = result.next();
                        Map<String,Object> valueMap = nodeRecord.asMap();
                        String idKey = "id("+CypherBuilder.operationResultName+")";
                        Long uidValue = (Long)valueMap.get(idKey);
                        String geospatialScaleEntityUID = ""+uidValue.longValue();
                        String geospatialScaleEntityCode = CypherBuilder.operationResultName+"."+ Constant.GeospatialCodeProperty;
                        String geospatialChinaProvinceKey = CypherBuilder.operationResultName+"."+"ChinaProvinceName";
                        String geospatialChinaPrefectureKey = CypherBuilder.operationResultName+"."+"ChinaPrefectureName";
                        String geospatialChinaCountyKey = CypherBuilder.operationResultName+"."+"ChinaCountyName";
                        String geospatialChinaTownKey = CypherBuilder.operationResultName+"."+"ChinaTownshipName";
                        String geospatialEntitySelfKey = CypherBuilder.operationResultName+"."+ Constant.GeospatialChineseNameProperty;

                        String geospatialChinaNameValue = "";
                        switch(geospatialScaleGrade){
                            case PROVINCE:
                                geospatialChinaNameValue = valueMap.get(geospatialEntitySelfKey).toString();
                                break;
                            case PREFECTURE:
                                geospatialChinaNameValue = valueMap.get(geospatialChinaProvinceKey)+"-"+
                                        valueMap.get(geospatialEntitySelfKey).toString();
                                break;
                            case COUNTY:
                                geospatialChinaNameValue = valueMap.get(geospatialChinaProvinceKey)+"-"+
                                        valueMap.get(geospatialChinaPrefectureKey).toString()+"-"+
                                        valueMap.get(geospatialEntitySelfKey).toString();
                                break;
                            case TOWNSHIP:
                                geospatialChinaNameValue = valueMap.get(geospatialChinaProvinceKey)+"-"+
                                        valueMap.get(geospatialChinaPrefectureKey).toString()+"-"+
                                        valueMap.get(geospatialChinaCountyKey).toString()+"-"+
                                        valueMap.get(geospatialEntitySelfKey).toString();
                                break;
                            case VILLAGE:
                                geospatialChinaNameValue = valueMap.get(geospatialChinaProvinceKey)+"-"+
                                        valueMap.get(geospatialChinaPrefectureKey).toString()+"-"+
                                        valueMap.get(geospatialChinaCountyKey).toString()+"-"+
                                        valueMap.get(geospatialChinaTownKey).toString()+"-"+
                                        valueMap.get(geospatialEntitySelfKey).toString();
                                break;
                        }
                        geospatialScaleEntityUIDAndChinaNamesMap.put(geospatialChinaNameValue,geospatialScaleEntityUID);
                        String entityGeoCode =
                                valueMap.containsKey(geospatialScaleEntityCode) ? valueMap.get(geospatialScaleEntityCode).toString():"-";
                        geospatialScaleEntityUIDAndCodeMap.put(geospatialScaleEntityUID,entityGeoCode);
                    }
                    return null;
                }
            };
            graphOperationExecutor.executeRead(geospatialCodeSearchDataTransformer, queryCql);
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }finally {
            graphOperationExecutor.close();
        }
        List<RelationshipEntityValue> attachEntityMetaDataList = new ArrayList<>();
        Iterator<Map.Entry<String, String>> entries = entityUIDAndGeospatialChinaNamesMap.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<String, String> entry = entries.next();
            String conceptionEntityUID = entry.getKey();
            String targetGeospatialChinaNames = entry.getValue();

            if(geospatialScaleEntityUIDAndChinaNamesMap.containsKey(targetGeospatialChinaNames)) {
                RelationshipEntityValue relationshipEntityValue = new RelationshipEntityValue();
                relationshipEntityValue.setFromEntityUID(conceptionEntityUID);
                relationshipEntityValue.setToEntityUID(geospatialScaleEntityUIDAndChinaNamesMap.get(targetGeospatialChinaNames));
                Map<String,Object> geospatialCodePropertyDataMap = new HashMap<>();
                geospatialCodePropertyDataMap.put("GeospatialChinaNames",targetGeospatialChinaNames);
                geospatialCodePropertyDataMap.put(Constant.GeospatialCodeProperty,
                        geospatialScaleEntityUIDAndCodeMap.get(geospatialScaleEntityUIDAndChinaNamesMap.get(targetGeospatialChinaNames)));
                relationshipEntityValue.setEntityAttributesValue(geospatialCodePropertyDataMap);
                attachEntityMetaDataList.add(relationshipEntityValue);
            }
        }
        return batchAttachGeospatialScaleEvents(attachEntityMetaDataList,eventComment,globalEventData,geospatialScaleGrade,_CPUUsageRate);
    }

    public static int calculateRuntimeCPUCoresByUsageRate(int entityVolume,CPUUsageRate _CPUUsageRate){
        int availableCoreNumber = Runtime.getRuntime().availableProcessors();
        if(availableCoreNumber<=4){
            return 4;
        }else if(availableCoreNumber<=8){
            switch(_CPUUsageRate){
                case Low:
                    return 4;
                case Middle:
                    return 6;
                case High:
                    return 8;
            }
        }else if(availableCoreNumber<=16){
            switch(_CPUUsageRate){
                case Low:
                    return 4;
                case Middle:
                    return 8;
                case High:
                    return 16;
            }
        }else{
            int lowCoreNumber = availableCoreNumber/4;
            int middleCoreNumber = availableCoreNumber/2;
            int highCoreNumber = availableCoreNumber -4;
            switch(_CPUUsageRate){
                case Low:
                    return lowCoreNumber;
                case Middle:
                    return middleCoreNumber;
                case High:
                    return highCoreNumber;
            }
        }
        return 4;
    }

    public static boolean importConceptionEntitiesFromCSV(String csvLocation,String conceptionKind,Map<String,String> attributesMapping){
        if(csvLocation == null || conceptionKind == null || attributesMapping == null){
            return false;
        }else{
            if(attributesMapping.size()>0){
                String propertyInsertStr="";
                Set<String> attributeNames = attributesMapping.keySet();
                for(String currentAttributeName:attributeNames){
                    String propertyNameOfCSV = attributesMapping.get(currentAttributeName);
                    propertyInsertStr = propertyInsertStr+currentAttributeName+":row."+propertyNameOfCSV+",";
                }
                propertyInsertStr = propertyInsertStr.substring(0,propertyInsertStr.length()-1);

                String csvFileLocation="file:///"+csvLocation.replaceFirst("file:///","");
                String cql = "LOAD CSV WITH HEADERS FROM \""+csvFileLocation+"\" AS row CREATE (:"+conceptionKind+" {"+propertyInsertStr+"})";

                GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
                try{
                    DataTransformer<Boolean> geospatialCodeSearchDataTransformer = new DataTransformer() {
                        @Override
                        public Object transformResult(Result result) {
                            return true;
                        }
                    };
                    Object result = graphOperationExecutor.executeWrite(geospatialCodeSearchDataTransformer, cql);
                    if(result != null){
                        return (Boolean)result;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    graphOperationExecutor.close();
                }
            }
        }
        return false;
    }

    public static Map<String,String>  getAttributesMappingFromHeaderCSV(String csvLocation){
        if(csvLocation == null){
            return null;
        }else{
            try {
                BufferedReader reader = new BufferedReader(new FileReader(csvLocation));
                String header = reader.readLine();
                Map<String,String> attributesMapping=new HashMap<>();
                String[] attributesArray = header.split(",");
                for(String currentStr : attributesArray){
                    attributesMapping.put(currentStr.replaceAll("\"",""),currentStr.replaceAll("\"",""));
                }
                return attributesMapping;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public interface EntityAttributesProcess {
        void doEntityAttributesProcess(Map<String,Object> entityValueMap);
    }

    public static boolean importConceptionEntitiesFromExternalCSV(String csvLocation, String conceptionKind, EntityAttributesProcess conceptionEntityAttributesProcess){
        if(csvLocation == null || conceptionKind == null){
            return false;
        }else{
            try{
                List<EntityValue> _EntityValueList = Lists.newArrayList();
                BufferedReader reader = new BufferedReader(new FileReader(csvLocation));
                String header = reader.readLine();
                List<String> attributeNameList = new ArrayList<>();
                String[] attributesArray = header.split(",");
                for(String currentStr : attributesArray){
                    attributeNameList.add(currentStr.replaceAll("\"",""));
                }
                reader.close();
                File file = new File(csvLocation);
                reader = new BufferedReader(new FileReader(file));
                String tempStr;
                int lineCount = 0;

                while ((tempStr = reader.readLine()) != null) {
                    if(lineCount > 0){
                        Map<String,Object> newEntityValueMap = new HashMap<>();
                        String[] dataItems = tempStr.split(",");
                        if(dataItems.length == attributeNameList.size()) {
                            for (int i = 0; i < dataItems.length; i++) {
                                String attributeName = attributeNameList.get(i);
                                String attributeOriginalValue = dataItems[i];
                                newEntityValueMap.put(attributeName, attributeOriginalValue);
                            }
                            if(conceptionEntityAttributesProcess != null){
                                conceptionEntityAttributesProcess.doEntityAttributesProcess(newEntityValueMap);
                            }
                            EntityValue entityValue = new EntityValue(newEntityValueMap);
                            entityValue.setEntityAttributesValue(newEntityValueMap);
                            _EntityValueList.add(entityValue);
                        }
                    }
                    lineCount ++;
                }
                reader.close();

                BatchDataOperationUtil.batchAddNewEntities(conceptionKind, _EntityValueList, BatchDataOperationUtil.CPUUsageRate.High);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return true;
        }
    }
}
