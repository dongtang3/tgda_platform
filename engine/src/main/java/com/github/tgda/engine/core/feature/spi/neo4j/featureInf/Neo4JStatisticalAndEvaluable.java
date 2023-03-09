package com.github.tgda.engine.core.feature.spi.neo4j.featureInf;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.RelationKindMatchLogic;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.DataTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetListObjectValueTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleAttributeValueTransformer;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.term.Direction;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.RelationshipType;
import com.github.tgda.engine.core.term.Type;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.feature.StatisticalAndEvaluable;
import com.github.tgda.coreRealm.realmServiceCore.payload.*;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEntityImpl;
import com.github.tgda.engine.core.util.Constant;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public interface Neo4JStatisticalAndEvaluable extends StatisticalAndEvaluable,Neo4JKeyResourcesRetrievable{

    static Logger logger = LoggerFactory.getLogger(StatisticalAndEvaluable.class);

    default Map<String,Number> statisticNumericalAttributes(QueryParameters queryParameters, List<NumericalAttributeStatisticCondition> statisticConditions) throws EngineServiceEntityExploreException {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String checkCql = CypherBuilder.matchNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.getEntityUID()),new String[]{Constant._NameProperty});
                GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._NameProperty);
                Object resultRes = workingGraphOperationExecutor.executeRead(getSingleAttributeValueTransformer,checkCql);
                String statisticTargetLabel = ((AttributeValue)resultRes).getAttributeValue().toString();

                QueryParameters realQueryParameters = queryParameters != null ?queryParameters:new QueryParameters();
                String statisticCql = "";
                if(this instanceof Type){
                    statisticCql = CypherBuilder.statistNodesWithQueryParametersAndStatisticFunctions(statisticTargetLabel,realQueryParameters,statisticConditions,null);
                }
                if(this instanceof RelationshipType){
                    statisticCql = CypherBuilder.statistRelationsWithQueryParametersAndStatisticFunctions(statisticTargetLabel,realQueryParameters,statisticConditions,null);
                }

                DataTransformer resultHandleDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        if(result.hasNext()){
                            Map<String,Number> resultStatisticMap = new HashMap<>();
                            Record returnRecord = result.next();
                            Map<String,Object> returnValueMap = returnRecord.asMap();
                            Set<String> keySet = returnValueMap.keySet();
                            for(String currentKey : keySet){
                                String currentStatisticKey = currentKey.replace(CypherBuilder.operationResultName+".","");
                                Number currentStatisticValue = (Number)returnValueMap.get(currentKey);
                                resultStatisticMap.put(currentStatisticKey,currentStatisticValue);
                            }
                            return resultStatisticMap;
                        }
                        return null;
                    }
                };

                Object statisticCqlRes = workingGraphOperationExecutor.executeRead(resultHandleDataTransformer,statisticCql);
                if(statisticCqlRes != null){
                    return (Map<String,Number>)statisticCqlRes;
                }
            } finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default List<GroupNumericalAttributesStatisticResult> statisticNumericalAttributesByGroup(String groupByAttribute,
                                                                                              QueryParameters queryParameters,
                                                                                              List<NumericalAttributeStatisticCondition> statisticConditions)  throws EngineServiceEntityExploreException {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String checkCql = CypherBuilder.matchNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.getEntityUID()),new String[]{Constant._NameProperty});
                GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._NameProperty);
                Object resultRes = workingGraphOperationExecutor.executeRead(getSingleAttributeValueTransformer,checkCql);
                String statisticTargetLabel = ((AttributeValue)resultRes).getAttributeValue().toString();

                QueryParameters realQueryParameters = queryParameters != null ?queryParameters:new QueryParameters();
                String statisticCql = "";
                if(this instanceof Type){
                    statisticCql = CypherBuilder.statistNodesWithQueryParametersAndStatisticFunctions(statisticTargetLabel,realQueryParameters,statisticConditions,groupByAttribute);
                }
                if(this instanceof RelationshipType){
                    statisticCql = CypherBuilder.statistRelationsWithQueryParametersAndStatisticFunctions(statisticTargetLabel,realQueryParameters,statisticConditions,groupByAttribute);
                }

                DataTransformer resultHandleDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        List<GroupNumericalAttributesStatisticResult> resultList = new ArrayList<>();
                        while(result.hasNext()){
                            Object groupValueObject = null;
                            Map<String,Number> resultStatisticMap = new HashMap<>();
                            Record returnRecord = result.next();
                            Map<String,Object> returnValueMap = returnRecord.asMap();
                            Set<String> keySet = returnValueMap.keySet();

                            for(String currentKey : keySet){
                                if(currentKey.equals(CypherBuilder.operationResultName+"."+groupByAttribute)){
                                    groupValueObject = returnValueMap.get(currentKey);
                                }else{
                                    String currentStatisticKey = currentKey.replace(CypherBuilder.operationResultName+".","");
                                    Number currentStatisticValue = (Number)returnValueMap.get(currentKey);
                                    resultStatisticMap.put(currentStatisticKey,currentStatisticValue);
                                }
                            }
                            GroupNumericalAttributesStatisticResult groupNumericalAttributesStatisticResult
                                    = new GroupNumericalAttributesStatisticResult(groupValueObject,resultStatisticMap);
                            resultList.add(groupNumericalAttributesStatisticResult);
                        }
                        return resultList;
                    }
                };

                Object statisticCqlRes = workingGraphOperationExecutor.executeRead(resultHandleDataTransformer,statisticCql);
                if(statisticCqlRes != null){
                    return (List<GroupNumericalAttributesStatisticResult>)statisticCqlRes;
                }
            } finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public Map<String,List<Entity>> statisticRelatedClassifications(QueryParameters queryParameters, String relationKindName, Direction direction) throws EngineServiceEntityExploreException {
        if(this instanceof RelationshipType){
            logger.error("Method statisticRelatedClassifications can not used for RelationKind");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("Method statisticRelatedClassifications can not used for RelationKind");
            throw e;
        }
        if (this.getEntityUID() != null) {
            Map<String,List<Entity>> resultMap = new HashMap<>();
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String checkCql = CypherBuilder.matchNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.getEntityUID()),new String[]{Constant._NameProperty});
                GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._NameProperty);
                Object resultRes = workingGraphOperationExecutor.executeRead(getSingleAttributeValueTransformer,checkCql);
                String statisticTargetLabel = ((AttributeValue)resultRes).getAttributeValue().toString();

                QueryParameters inputQueryParameters;
                if(queryParameters != null){
                    inputQueryParameters = queryParameters;
                }else{
                    inputQueryParameters = new QueryParameters();
                    inputQueryParameters.setResultNumber(10000000);
                }
                String queryEntitiesIDCql = CypherBuilder.matchNodesWithQueryParameters(statisticTargetLabel,inputQueryParameters, CypherBuilder.CypherFunctionType.ID);
                GetListObjectValueTransformer<Long> longListValueTransformer = new GetListObjectValueTransformer<Long>("id");
                Object idList = workingGraphOperationExecutor.executeRead(longListValueTransformer,queryEntitiesIDCql);

                List<Long> resultEntitiesIDList = idList != null? (List<Long>)idList : null;

                if(resultEntitiesIDList != null){
                    String queryPairsCql = CypherBuilder.matchRelatedPairFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID,
                            CommonOperationUtil.formatListLiteralValue(resultEntitiesIDList), Constant.ClassificationClass,relationKindName, direction);
                    DataTransformer pairRelationDataTransformer = new DataTransformer() {
                        @Override
                        public Object transformResult(Result result) {
                            while(result.hasNext()){
                                Record record = result.next();
                                if(record.containsKey(CypherBuilder.operationResultName) && record.containsKey(CypherBuilder.sourceNodeName)){
                                    Node classificationNode = record.get(CypherBuilder.operationResultName).asNode();
                                    Node entityNode = record.get(CypherBuilder.sourceNodeName).asNode();
                                    List<String> classificationLabelNames = Lists.newArrayList(classificationNode.labels());
                                    List<String> entityLabelNames = Lists.newArrayList(entityNode.labels());

                                    if(classificationLabelNames.contains(Constant.ClassificationClass) && entityLabelNames.contains(statisticTargetLabel)){
                                        String classificationName = classificationNode.get(Constant._NameProperty).asString();
                                        if(!resultMap.containsKey(classificationName)){
                                            resultMap.put(classificationName,new ArrayList<Entity>());
                                        }
                                        List<Entity> classificationRelatedEntityList = resultMap.get(classificationName);

                                        long nodeUID = entityNode.id();
                                        String conceptionEntityUID = ""+nodeUID;
                                        String resultConceptionKindName = statisticTargetLabel != null ? statisticTargetLabel : entityLabelNames.get(0);
                                        Neo4JEntityImpl neo4jEntityImpl =
                                                new Neo4JEntityImpl(resultConceptionKindName,conceptionEntityUID);
                                        neo4jEntityImpl.setAllConceptionKindNames(entityLabelNames);
                                        neo4jEntityImpl.setGlobalGraphOperationExecutor(getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                                        classificationRelatedEntityList.add(neo4jEntityImpl);
                                    }
                                }
                            }
                            return null;
                        }
                    };
                    workingGraphOperationExecutor.executeRead(pairRelationDataTransformer,queryPairsCql);
                }
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            } finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
            return resultMap;
        }
        return null;
    }

    default List<KindEntityAttributeRuntimeStatistics> statisticEntityAttributesDistribution(long sampleCount){
        /*
        Example:
        https://neo4j.com/labs/apoc/4.1/overview/apoc.meta/apoc.meta.nodeTypeProperties/
        https://neo4j.com/labs/apoc/4.1/overview/apoc.meta/apoc.meta.relTypeProperties/
        */
        if (this.getEntityUID() != null) {
            List<KindEntityAttributeRuntimeStatistics> resultList = new ArrayList<>();
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String checkCql = CypherBuilder.matchNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.getEntityUID()),new String[]{Constant._NameProperty});
                GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._NameProperty);
                Object resultRes = workingGraphOperationExecutor.executeRead(getSingleAttributeValueTransformer,checkCql);
                String statisticTargetType = ((AttributeValue)resultRes).getAttributeValue().toString();

                long sampleCountRealValue = sampleCount > 1000 ? sampleCount : 1000;
                String evaluateCql = "";
                if(this instanceof Type){
                    evaluateCql = "CALL apoc.meta.nodeTypeProperties({labels: [\""+statisticTargetType+"\"],sample:"+sampleCountRealValue+"});";
                    logger.debug("Generated Cypher Statement: {}", evaluateCql);
                    DataTransformer nodeTypePropertiesDataTransformer = new DataTransformer() {
                        @Override
                        public Object transformResult(Result result) {

                            while(result.hasNext()){
                                Record nodeRecord = result.next();
                                String kindName = nodeRecord.get("nodeType").asString().replace(":","").replaceAll("`","");
                                String propertyName = nodeRecord.get("propertyName").asString();
                                String propertyTypes = nodeRecord.get("propertyTypes").asList().get(0).toString();
                                long propertyObservations = nodeRecord.get("propertyObservations").asLong();
                                long totalObservations = nodeRecord.get("totalObservations").asLong();
                                KindEntityAttributeRuntimeStatistics currentKindEntityAttributeRuntimeStatistics =
                                        new KindEntityAttributeRuntimeStatistics(kindName,propertyName,propertyTypes,totalObservations,propertyObservations);
                                resultList.add(currentKindEntityAttributeRuntimeStatistics);
                            }
                            return null;
                        }
                    };
                    workingGraphOperationExecutor.executeRead(nodeTypePropertiesDataTransformer,evaluateCql);
                }
                if(this instanceof RelationshipType){
                    evaluateCql = "CALL apoc.meta.relTypeProperties({rels: [\""+statisticTargetType+"\"],maxRels: "+sampleCountRealValue+"});";
                    logger.debug("Generated Cypher Statement: {}", evaluateCql);
                    DataTransformer relTypePropertiesDataTransformer = new DataTransformer() {
                        @Override
                        public Object transformResult(Result result) {
                            while(result.hasNext()){
                                Record nodeRecord = result.next();
                                String kindName = nodeRecord.get("relType").asString().replace(":","").replaceAll("`","");
                                String propertyName = nodeRecord.get("propertyName").asString();
                                String propertyTypes = nodeRecord.get("propertyTypes").asList().get(0).toString();
                                long propertyObservations = nodeRecord.get("propertyObservations").asLong();
                                long totalObservations = nodeRecord.get("totalObservations").asLong();
                                KindEntityAttributeRuntimeStatistics currentKindEntityAttributeRuntimeStatistics =
                                        new KindEntityAttributeRuntimeStatistics(kindName,propertyName,propertyTypes,totalObservations,propertyObservations);
                                resultList.add(currentKindEntityAttributeRuntimeStatistics);
                            }
                            return null;
                        }
                    };
                    workingGraphOperationExecutor.executeRead(relTypePropertiesDataTransformer,evaluateCql);
                }
                return resultList;
            } finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public Map<String,Long> statisticEntityRelationDegree(AttributesParameters queryParameters, List<RelationKindMatchLogic> relationKindMatchLogics,
                                                                  Direction defaultDirectionForNoneRelationKindMatch) throws EngineServiceEntityExploreException {
        if(this instanceof RelationshipType){
            logger.error("Method statisticEntityRelationDegree can not used for RelationKind");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("Method statisticEntityRelationDegree can not used for RelationKind");
            throw e;
        }
        /*
        Example:
        https://neo4j.com/labs/apoc/4.1/overview/apoc.node/apoc.node.degree/
        */
        if (this.getEntityUID() != null) {
            Map<String,Long> resultMap = new HashMap<>();
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String checkCql = CypherBuilder.matchNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.getEntityUID()),new String[]{Constant._NameProperty});
                GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._NameProperty);
                Object resultRes = workingGraphOperationExecutor.executeRead(getSingleAttributeValueTransformer,checkCql);
                String statisticTargetLabel = ((AttributeValue)resultRes).getAttributeValue().toString();

                String statisticCql = "MATCH (entity:"+statisticTargetLabel+")";
                String wherePartQuery = CypherBuilder.generateAttributesParametersQueryLogic(queryParameters,"entity");
                String relationMatchLogicFullString = CypherBuilder.generateRelationKindMatchLogicsQuery(relationKindMatchLogics,defaultDirectionForNoneRelationKindMatch);
                if(!wherePartQuery.equals("")){
                    statisticCql = statisticCql + " " + wherePartQuery;
                }
                statisticCql = statisticCql +"\n" + "RETURN id(entity) as uid,apoc.node.degree(entity,\""+relationMatchLogicFullString+"\") AS degree;";
                logger.debug("Generated Cypher Statement: {}", statisticCql);

                DataTransformer entityRelationDegreeDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        if(result.hasNext()){
                            while(result.hasNext()){
                                Record nodeRecord = result.next();
                                String entityUID = ""+nodeRecord.get("uid").asLong();
                                long entityDegree = nodeRecord.get("degree").asLong();
                                resultMap.put(entityUID,entityDegree);
                            }
                        }
                        return null;
                    }
                };
                workingGraphOperationExecutor.executeRead(entityRelationDegreeDataTransformer,statisticCql);
                return resultMap;
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            } finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public AttributeValueDistributionInfo statisticAttributeValueDistribution(AttributesParameters queryParameters, String attributeName) throws EngineServiceEntityExploreException {
        if(attributeName == null){
            logger.error("AttributeName is required");
            EngineServiceEntityExploreException e = new EngineServiceEntityExploreException();
            e.setCauseMessage("AttributeName is required");
            throw e;
        }
        /*
        Example:
        https://neo4j.com/labs/apoc/4.1/overview/apoc.agg/apoc.agg.statistics/
        */
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                String checkCql = CypherBuilder.matchNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.getEntityUID()),new String[]{Constant._NameProperty});
                GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._NameProperty);
                Object resultRes = workingGraphOperationExecutor.executeRead(getSingleAttributeValueTransformer,checkCql);
                String statisticTargetType = ((AttributeValue)resultRes).getAttributeValue().toString();
                String statisticCql = "";
                if(this instanceof Type){
                    statisticCql = "MATCH (entity:"+statisticTargetType+")";
                }
                if(this instanceof RelationshipType){
                    statisticCql = "MATCH ()-[entity:"+statisticTargetType+"]->()";
                }

                String wherePartQuery = CypherBuilder.generateAttributesParametersQueryLogic(queryParameters,"entity");
                if(!wherePartQuery.equals("")){
                    statisticCql = statisticCql + " " + wherePartQuery;
                }

                String fullQuery = statisticCql +"\n" +
                        "WITH apoc.agg.statistics(entity."+attributeName+", [0.1,0.25,0.4,0.5,0.75,0.90,0.95,0.99,0.999]) AS stats"+"\n" +
                        "RETURN stats";
                logger.debug("Generated Cypher Statement: {}", fullQuery);

                DataTransformer<AttributeValueDistributionInfo> attributeValueDistributionDataTransformer = new DataTransformer() {
                    @Override
                    public AttributeValueDistributionInfo transformResult(Result result) {
                        if(result.hasNext()){
                            while(result.hasNext()){
                                Record nodeRecord = result.next();
                                Map<String,Object> statsResultMap = nodeRecord.get("stats").asMap();

                                if(statsResultMap.size() == 6){
                                    /*value of attributeName noe exist in entities, will return below values:
                                        "total": 0,
                                        "min": null,
                                        "minNonZero": 9223372036854776000.0,
                                        "max": null,
                                        "mean": 0.0,
                                        "stdev": 0.0
                                    */
                                    return null;
                                }else {
                                    String kindName = statisticTargetType;
                                    String attributeNameValue = attributeName;
                                    long entityCount = ((Number) statsResultMap.get("total")).longValue();
                                    float p10 = ((Number) statsResultMap.get("0.1")).floatValue();
                                    float p25 = ((Number) statsResultMap.get("0.25")).floatValue();
                                    float p40 = ((Number) statsResultMap.get("0.4")).floatValue();
                                    float p50 = ((Number) statsResultMap.get("0.5")).floatValue();
                                    float p75 = ((Number) statsResultMap.get("0.75")).floatValue();
                                    float p90 = ((Number) statsResultMap.get("0.9")).floatValue();
                                    float p95 = ((Number) statsResultMap.get("0.95")).floatValue();
                                    float p99 = ((Number) statsResultMap.get("0.99")).floatValue();
                                    float p999 = ((Number) statsResultMap.get("0.999")).floatValue();
                                    float max = ((Number) statsResultMap.get("max")).floatValue();
                                    float min = ((Number) statsResultMap.get("min")).floatValue();
                                    float minNonZero = ((Number) statsResultMap.get("minNonZero")).floatValue();
                                    float mean = ((Number) statsResultMap.get("mean")).floatValue();
                                    float stDev = ((Number) statsResultMap.get("stdev")).floatValue();

                                    AttributeValueDistributionInfo targetAttributeValueDistributionInfo =
                                            new AttributeValueDistributionInfo(kindName, attributeNameValue, entityCount, p10,
                                                    p25, p40, p50, p75, p90, p95, p99, p999, max, min, minNonZero, mean, stDev);
                                    return targetAttributeValueDistributionInfo;
                                }
                            }
                        }
                        return null;
                    }
                };
                Object responseObject = workingGraphOperationExecutor.executeRead(attributeValueDistributionDataTransformer,fullQuery);
                return responseObject != null ? (AttributeValueDistributionInfo)responseObject : null;
            } catch (org.neo4j.driver.exceptions.ClientException e){
                logger.error("AttributeName value contains format error -> "+e.getMessage());
                EngineServiceEntityExploreException e1 = new EngineServiceEntityExploreException();
                e1.setCauseMessage("AttributeName value contains format error -> "+e.getMessage());
                throw e1;
            } catch (Exception e){
                e.printStackTrace();
            }
            finally{
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }
}
