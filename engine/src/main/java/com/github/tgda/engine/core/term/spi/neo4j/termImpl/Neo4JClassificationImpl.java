package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.feature.TypeCacheable;
import com.github.tgda.engine.core.feature.spi.neo4j.featureImpl.Neo4JAttributesMeasurableImpl;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.*;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.payload.AttributeValue;
import com.github.tgda.engine.core.structure.InheritanceTree;
import com.github.tgda.engine.core.structure.spi.common.structureImpl.CommonInheritanceTreeImpl;
import com.github.tgda.engine.core.term.*;
import com.github.tgda.engine.core.util.Constant;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JClassification;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4JClassificationImpl extends Neo4JAttributesMeasurableImpl implements Neo4JClassification {

    private static Logger logger = LoggerFactory.getLogger(Neo4JClassificationImpl.class);
    private String coreRealmName;
    private String classificationName;
    private String classificationDesc;
    private String classificationUID;

    public Neo4JClassificationImpl(String coreRealmName,String classificationName,String classificationDesc,String classificationUID){
        super(classificationUID);
        this.coreRealmName = coreRealmName;
        this.classificationName = classificationName;
        this.classificationDesc = classificationDesc;
        this.classificationUID = classificationUID;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    public String getClassificationUID() {
        return this.classificationUID;
    }

    public String getCoreRealmName() {
        return this.coreRealmName;
    }

    @Override
    public String getClassificationName() {
        return this.classificationName;
    }

    @Override
    public String getClassificationDesc() {
        return this.classificationDesc;
    }

    @Override
    public boolean updateClassificationDesc(String classificationDesc) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            Map<String,Object> attributeDataMap = new HashMap<>();
            attributeDataMap.put(Constant._DescProperty, classificationDesc);
            String updateCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.classificationUID),attributeDataMap);
            GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._DescProperty);
            Object updateResultRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeValueTransformer,updateCql);
            CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.classificationUID,false);
            AttributeValue resultAttributeValue =  updateResultRes != null ? (AttributeValue) updateResultRes : null;
            if(resultAttributeValue != null && resultAttributeValue.getAttributeValue().toString().equals(classificationDesc)){
                this.classificationDesc = classificationDesc;
                return true;
            }else{
                return false;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public boolean isRootClassification() {
        return getParentClassification() == null ? true : false;
    }

    @Override
    public Classification getParentClassification() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchRelatedNodesFromSpecialStartNodes(
                    CypherBuilder.CypherFunctionType.ID, Long.parseLong(classificationUID), Constant.ClassificationClass, Constant.Classification_ClassificationRelationClass, Direction.TO, null);
            GetSingleClassificationTransformer getSingleClassificationTransformer =
                    new GetSingleClassificationTransformer(coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object classificationRes = workingGraphOperationExecutor.executeWrite(getSingleClassificationTransformer,queryCql);
            return classificationRes != null?(Classification)classificationRes:null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<Classification> getChildClassifications() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchRelatedNodesFromSpecialStartNodes(
                    CypherBuilder.CypherFunctionType.ID, Long.parseLong(classificationUID), Constant.ClassificationClass, Constant.Classification_ClassificationRelationClass, Direction.FROM, null);
            GetListClassificationTransformer getListClassificationTransformer =
                    new GetListClassificationTransformer(coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object classificationListRes = workingGraphOperationExecutor.executeWrite(getListClassificationTransformer,queryCql);
            return classificationListRes != null ? (List<Classification>)classificationListRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public InheritanceTree<Classification> getOffspringClassifications() {
        Table<String,String,Classification> treeElementsTable = HashBasedTable.create();
        treeElementsTable.put(InheritanceTree.Virtual_ParentID_Of_Root_Node,this.classificationName,this);
        Map<String,String> classificationUID_NameMapping = new HashMap<>();
        classificationUID_NameMapping.put(this.classificationUID,this.classificationName);

        String currentCoreRealmName = this.coreRealmName;
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            /*
            MATCH (sourceNode)<-[relation:`TGDA_ParentClassificationIs`*]-(operationResult:`TGDA_Classification`) WHERE id(sourceNode) = 2324 RETURN operationResult,relation
            */
            String queryCql = CypherBuilder.matchRelatedNodesAndRelationsFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID, Long.parseLong(classificationUID),
                    Constant.ClassificationClass, Constant.Classification_ClassificationRelationClass, Direction.FROM,0,0, CypherBuilder.ReturnRelationableDataType.BOTH);
            DataTransformer offspringClassificationsDataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    List<Record> recordList = result.list();
                    if(recordList != null){
                        for(Record nodeRecord : recordList){
                            Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                            long nodeUID = resultNode.id();
                            String classificationName = resultNode.get(Constant._NameProperty).asString();
                            classificationUID_NameMapping.put(""+nodeUID,classificationName);
                        }
                    }
                    if(recordList != null){
                        for(Record nodeRecord : recordList){
                            Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                            long nodeUID = resultNode.id();
                            String coreRealmName = currentCoreRealmName;
                            String classificationName = resultNode.get(Constant._NameProperty).asString();
                            String classificationDesc = null;
                            if(resultNode.get(Constant._DescProperty) != null){
                                classificationDesc = resultNode.get(Constant._DescProperty).asString();
                            }
                            String classificationUID = ""+nodeUID;
                            Neo4JClassificationImpl neo4JClassificationImpl =
                                    new Neo4JClassificationImpl(coreRealmName,classificationName,classificationDesc,classificationUID);
                            neo4JClassificationImpl.setGlobalGraphOperationExecutor(graphOperationExecutorHelper.getGlobalGraphOperationExecutor());

                            List<Object> relationships = nodeRecord.get(CypherBuilder.relationResultName).asList();
                            String parentClassificationUID = null;
                            for(Object currentRelationship : relationships){
                                Relationship currentTargetRelationship = (Relationship)currentRelationship;
                                String startNodeUID = "" + currentTargetRelationship.startNodeId();
                                String endNodeUID = "" + currentTargetRelationship.endNodeId();
                                if(startNodeUID.equals(classificationUID)){
                                    parentClassificationUID = endNodeUID;
                                    break;
                                }
                            }
                            treeElementsTable.put(classificationUID_NameMapping.get(parentClassificationUID),
                                    classificationUID_NameMapping.get(classificationUID),neo4JClassificationImpl);

                        }
                    }
                    return null;
                }
            };
            workingGraphOperationExecutor.executeRead(offspringClassificationsDataTransformer,queryCql);
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        CommonInheritanceTreeImpl<Classification> resultInheritanceTree = new CommonInheritanceTreeImpl(this.classificationName,treeElementsTable);
        return resultInheritanceTree;
    }

    @Override
    public boolean attachChildClassification(String childClassificationName) throws EngineServiceRuntimeException {
        if(childClassificationName == null){
            return false;
        }else{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                Classification childClassification = getClassificationByName(workingGraphOperationExecutor,childClassificationName);
                if(childClassification == null){
                    logger.error("Classification with name {} does not exist.", childClassificationName);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("Classification with name "+childClassificationName+" does not exist.");
                    throw exception;
                }else{
                    String parentConceptionUID = this.getClassificationUID();
                    String childConceptionUID = ((Neo4JClassificationImpl)childClassification).getClassificationUID();

                    String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(childConceptionUID),Long.parseLong(parentConceptionUID),
                            Constant.Classification_ClassificationRelationClass);
                    GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                            (Constant.Classification_ClassificationRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                    Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer, queryRelationCql);
                    if(existingRelationshipEntityRes != null){
                        return true;
                    }

                    Map<String,Object> relationPropertiesMap = new HashMap<>();
                    CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesMap);
                    String createRelationCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(childConceptionUID),Long.parseLong(parentConceptionUID),
                            Constant.Classification_ClassificationRelationClass,relationPropertiesMap);

                    Object newRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, createRelationCql);
                    if(newRelationshipEntityRes == null){
                        logger.error("Set Classification {}'s parent to Classification {} fail.", childClassificationName,classificationName);
                        EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                        exception.setCauseMessage("Set Classification "+childClassificationName+"'s parent to Classification "+classificationName+" fail.");
                        throw exception;
                    }else{
                        return true;
                    }
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    @Override
    public boolean detachChildClassification(String childClassificationName) throws EngineServiceRuntimeException {
        if(childClassificationName == null){
            return false;
        }else{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                Classification childClassification = getClassificationByName(workingGraphOperationExecutor,childClassificationName);
                if(childClassification == null){
                    logger.error("Classification with name {} does not exist.", childClassificationName);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("Classification with name "+childClassificationName+" does not exist.");
                    throw exception;
                }else{
                    String parentConceptionUID = this.getClassificationUID();
                    String childConceptionUID = ((Neo4JClassificationImpl)childClassification).getClassificationUID();
                    String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(childConceptionUID),Long.parseLong(parentConceptionUID),
                            Constant.Classification_ClassificationRelationClass);
                    GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                            (Constant.Classification_ClassificationRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                    Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer, queryRelationCql);
                    if(existingRelationshipEntityRes == null){
                        logger.error("Classification {} is not parent of Classification {}.", getClassificationName(),classificationName);
                        EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                        exception.setCauseMessage("Classification "+getClassificationName()+" is not parent of Classification "+classificationName+".");
                        throw exception;
                    }else{
                        RelationshipEntity relationEntity = (RelationshipEntity)existingRelationshipEntityRes;
                        String deleteCql = CypherBuilder.deleteRelationWithSingleFunctionValueEqual(
                                CypherBuilder.CypherFunctionType.ID,Long.valueOf(relationEntity.getRelationshipEntityUID()),null,null);
                        getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                                (Constant.Classification_ClassificationRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                        Object deleteRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, deleteCql);
                        if(deleteRelationshipEntityRes == null){
                            logger.error("Internal error occurs during detach child classification {} from {}.",  childClassificationName,this.getClassificationName());
                            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                            exception.setCauseMessage("Internal error occurs during detach child classification "+childClassificationName+" from "+this.getClassificationName()+".");
                            throw exception;
                        }else{
                            return true;
                        }
                    }
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    @Override
    public Classification createChildClassification(String classificationName, String classificationDesc) throws EngineServiceRuntimeException {
        if(classificationName == null){
            return null;
        }else{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                Classification childClassification = getClassificationByName(workingGraphOperationExecutor,classificationName);
                if(childClassification != null){
                    logger.error("Classification with name {} already exist.", classificationName);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("Classification with name "+classificationName+" already exist.");
                    throw exception;
                }else{
                    GetSingleClassificationTransformer getSingleClassificationTransformer =
                            new GetSingleClassificationTransformer(coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());

                    Map<String,Object> propertiesMap = new HashMap<>();
                    propertiesMap.put(Constant._NameProperty,classificationName);
                    if(classificationDesc != null) {
                        propertiesMap.put(Constant._DescProperty, classificationDesc);
                    }
                    CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
                    String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.ClassificationClass},propertiesMap);
                    Object createClassificationRes = workingGraphOperationExecutor.executeWrite(getSingleClassificationTransformer,createCql);
                    Classification targetClassification = createClassificationRes != null ? (Classification)createClassificationRes : null;
                    if(targetClassification != null){
                        executeClassificationCacheOperation(targetClassification, TypeCacheable.CacheOperationType.INSERT);

                        String childConceptionUID = ((Neo4JClassificationImpl)targetClassification).getClassificationUID();
                        String parentConceptionUID = getClassificationUID();
                        Map<String,Object> relationPropertiesMap = new HashMap<>();
                        CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesMap);
                        String createRelationCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(childConceptionUID),Long.parseLong(parentConceptionUID),
                                Constant.Classification_ClassificationRelationClass,relationPropertiesMap);
                        GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                                (Constant.Classification_ClassificationRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                        Object newRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, createRelationCql);
                        if(newRelationshipEntityRes == null){
                            logger.error("Set Classification {}'s parent to Classification {} fail.", classificationName,getClassificationName());
                            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                            exception.setCauseMessage("Set Classification "+classificationName+"'s parent to Classification "+getClassificationName()+" fail.");
                            throw exception;
                        }
                        return targetClassification;
                    }else{
                        return null;
                    }
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    @Override
    public boolean removeChildClassification(String classificationName) throws EngineServiceRuntimeException {
        if(classificationName == null){
            return false;
        }else{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                Classification childClassification = getClassificationByName(workingGraphOperationExecutor,classificationName);
                if(childClassification == null){
                    logger.error("Classification with name {} does not exist.", classificationName);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("Classification with name "+ classificationName +" does not exist.");
                    throw exception;
                }else{
                    String childConceptionUID = ((Neo4JClassificationImpl)childClassification).getClassificationUID();
                    String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(childConceptionUID),Long.parseLong(getClassificationUID()),
                            Constant.Classification_ClassificationRelationClass);
                    GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                            (Constant.Classification_ClassificationRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                    Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer, queryRelationCql);
                    if(existingRelationshipEntityRes == null){
                        logger.error("Classification {} is not parent of Classification {}.", getClassificationName(),classificationName);
                        EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                        exception.setCauseMessage("Classification "+getClassificationName()+" is not parent of Classification "+classificationName+".");
                        throw exception;
                    }else{
                        String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.valueOf(childConceptionUID), null, null);
                        GetSingleClassificationTransformer getSingleClassificationTransformer =
                                new GetSingleClassificationTransformer(coreRealmName, this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                        Object deletedClassificationRes = workingGraphOperationExecutor.executeWrite(getSingleClassificationTransformer, deleteCql);
                        Classification resultClassification = deletedClassificationRes != null ? (Classification) deletedClassificationRes : null;
                        if (resultClassification == null) {
                            throw new EngineServiceRuntimeException();
                        } else {
                            executeClassificationCacheOperation(resultClassification, CacheOperationType.DELETE);
                            return true;
                        }
                    }
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    @Override
    public List<Type> getRelatedConceptionKind(String relationKindName, Direction relationDirection, boolean includeOffspringClassifications, int offspringLevel) throws EngineServiceRuntimeException {
        if(classificationName == null){
            return null;
        }else{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                List<Long> targetClassificationUIDsList = getTargetClassificationsUIDList(workingGraphOperationExecutor,includeOffspringClassifications,offspringLevel);
                String queryPairsCql = CypherBuilder.matchRelatedPairFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID,
                        CommonOperationUtil.formatListLiteralValue(targetClassificationUIDsList), Constant.TypeClass,relationKindName,relationDirection);
                GetListTypeTransformer getListConceptionKindTransformer = new GetListTypeTransformer(this.coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object queryRes = workingGraphOperationExecutor.executeRead(getListConceptionKindTransformer,queryPairsCql);
                List<Type> resultList = queryRes != null ? (List<Type>)queryRes: new ArrayList<>();
                return resultList;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    @Override
    public List<RelationshipType> getRelatedRelationKind(String relationKindName, Direction relationDirection, boolean includeOffspringClassifications, int offspringLevel) throws EngineServiceRuntimeException {
        if(classificationName == null){
            return null;
        }else{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                List<Long> targetClassificationUIDsList = getTargetClassificationsUIDList(workingGraphOperationExecutor,includeOffspringClassifications,offspringLevel);
                String queryPairsCql = CypherBuilder.matchRelatedPairFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID,
                        CommonOperationUtil.formatListLiteralValue(targetClassificationUIDsList), Constant.RelationKindClass,relationKindName,relationDirection);
                GetListRelationshipTypeTransformer getListRelationKindTransformer = new GetListRelationshipTypeTransformer(this.coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object queryRes = workingGraphOperationExecutor.executeRead(getListRelationKindTransformer,queryPairsCql);
                List<RelationshipType> resultList = queryRes != null ? (List<RelationshipType>)queryRes: new ArrayList<>();
                return resultList;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    @Override
    public List<Attribute> getRelatedAttributeKind(String relationKindName, Direction relationDirection, boolean includeOffspringClassifications, int offspringLevel) throws EngineServiceRuntimeException {
        if(classificationName == null){
            return null;
        }else{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                List<Long> targetClassificationUIDsList = getTargetClassificationsUIDList(workingGraphOperationExecutor,includeOffspringClassifications,offspringLevel);
                String queryPairsCql = CypherBuilder.matchRelatedPairFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID,
                        CommonOperationUtil.formatListLiteralValue(targetClassificationUIDsList), Constant.AttributeClass,relationKindName,relationDirection);
                GetListAttributeTransformer getListAttributeKindTransformer = new GetListAttributeTransformer(this.coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object queryRes = workingGraphOperationExecutor.executeRead(getListAttributeKindTransformer,queryPairsCql);
                List<Attribute> resultList = queryRes != null ? (List<Attribute>)queryRes: new ArrayList<>();
                return resultList;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    @Override
    public List<AttributesView> getRelatedAttributesViewKind(String relationKindName, Direction relationDirection, boolean includeOffspringClassifications, int offspringLevel) throws EngineServiceRuntimeException {
        if(classificationName == null){
            return null;
        }else{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                List<Long> targetClassificationUIDsList = getTargetClassificationsUIDList(workingGraphOperationExecutor,includeOffspringClassifications,offspringLevel);
                String queryPairsCql = CypherBuilder.matchRelatedPairFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID,
                        CommonOperationUtil.formatListLiteralValue(targetClassificationUIDsList), Constant.AttributesViewKindClass,relationKindName,relationDirection);
                GetListAttributesViewKindTransformer getListAttributesViewKindTransformer = new GetListAttributesViewKindTransformer(this.coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object queryRes = workingGraphOperationExecutor.executeRead((DataTransformer) getListAttributesViewKindTransformer,queryPairsCql);
                List<AttributesView> resultList = queryRes != null ? (List<AttributesView>)queryRes: new ArrayList<>();
                return resultList;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    @Override
    public List<Entity> getRelatedEntity(String relationKindName, Direction relationDirection, QueryParameters queryParameters, boolean includeOffspringClassifications, int offspringLevel) throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        if(classificationName == null){
            return null;
        }else{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                List<Entity> conceptionEntityList = new ArrayList<>();
                List<Long> targetClassificationUIDsList = getTargetClassificationsUIDList(workingGraphOperationExecutor,includeOffspringClassifications,offspringLevel);
                String queryPairsCql = CypherBuilder.matchRelatedNodesFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID,
                        CommonOperationUtil.formatListLiteralValue(targetClassificationUIDsList),queryParameters,relationKindName,relationDirection);
                DataTransformer offspringClassificationsDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        while(result.hasNext()){
                            Record record = result.next();
                            Node conceptionEntityNode = record.get(CypherBuilder.operationResultName).asNode();
                            //Node classificationNode = record.get(CypherBuilder.sourceNodeName).asNode();
                            String currentEntityKind = null;
                            List<String> allLabelNames = Lists.newArrayList(conceptionEntityNode.labels());
                            if(queryParameters != null && queryParameters.getEntityKind() != null){
                                boolean isMatchedKind = false;
                                if(allLabelNames.size()>0){
                                    isMatchedKind = allLabelNames.contains( queryParameters.getEntityKind());
                                }
                                if(isMatchedKind){
                                    currentEntityKind = queryParameters.getEntityKind();
                                }
                            }else{
                                currentEntityKind = allLabelNames.get(0);
                            }
                            if(currentEntityKind != null){
                                long nodeUID = conceptionEntityNode.id();
                                String conceptionEntityUID = ""+nodeUID;
                                Neo4JEntityImpl neo4jEntityImpl =
                                        new Neo4JEntityImpl(currentEntityKind,conceptionEntityUID);
                                neo4jEntityImpl.setAllConceptionKindNames(allLabelNames);
                                neo4jEntityImpl.setGlobalGraphOperationExecutor(graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                                conceptionEntityList.add(neo4jEntityImpl);
                            }
                        }
                        return null;
                    }
                };
                workingGraphOperationExecutor.executeRead(offspringClassificationsDataTransformer,queryPairsCql);
                return conceptionEntityList;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    private List<Long> getTargetClassificationsUIDList(GraphOperationExecutor workingGraphOperationExecutor,boolean includeOffspringClassifications, int offspringLevel) throws EngineServiceRuntimeException {
        if(includeOffspringClassifications & offspringLevel < 1){
            logger.error("Classification Offspring Level must great or equal 1, current value is {}.", offspringLevel);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("Classification Offspring Level must great or equal 1, current value is "+ offspringLevel +".");
            throw exception;
        }
        List<Long> classificationsUIDList = new ArrayList<>();
        classificationsUIDList.add(Long.parseLong(this.classificationUID));

        String queryCql = CypherBuilder.matchRelatedNodesAndRelationsFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID, Long.parseLong(classificationUID),
                Constant.ClassificationClass, Constant.Classification_ClassificationRelationClass, Direction.FROM,1,offspringLevel, CypherBuilder.ReturnRelationableDataType.NODE);

        DataTransformer offspringClassificationsDataTransformer = new DataTransformer() {
            @Override
            public Object transformResult(Result result) {
                while(result.hasNext()){
                    Record record = result.next();
                    Node classificationNode = record.get(CypherBuilder.operationResultName).asNode();
                    List<String> allLabelNames = Lists.newArrayList(classificationNode.labels());
                    boolean isMatchedKind = false;
                    if(allLabelNames.size()>0){
                        isMatchedKind = allLabelNames.contains(Constant.ClassificationClass);
                    }
                    if(isMatchedKind){
                        classificationsUIDList.add(classificationNode.id());
                    }
                }
                return null;
            }
        };
        workingGraphOperationExecutor.executeRead(offspringClassificationsDataTransformer,queryCql);
        return classificationsUIDList;
    }

    private Classification getClassificationByName(GraphOperationExecutor workingGraphOperationExecutor,String classificationName){
        String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.ClassificationClass, Constant._NameProperty,classificationName,1);
        GetSingleClassificationTransformer getSingleClassificationTransformer =
                new GetSingleClassificationTransformer(coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
        Object classificationRes = workingGraphOperationExecutor.executeRead(getSingleClassificationTransformer,queryCql);
        return classificationRes != null ? (Classification)classificationRes : null;
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        super.setGlobalGraphOperationExecutor(graphOperationExecutor);
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }

    @Override
    public String getEntityUID() {
        return classificationUID;
    }

    @Override
    public GraphOperationExecutorHelper getGraphOperationExecutorHelper() {
        return graphOperationExecutorHelper;
    }
}
