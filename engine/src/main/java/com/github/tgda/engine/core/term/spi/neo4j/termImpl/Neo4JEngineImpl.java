package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.EqualFilteringItem;
import com.github.tgda.engine.core.exception.EngineFunctionNotSupportedException;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.CheckResultExistenceTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleEntityTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleRelationshipEntityTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleTypeTransformer;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.operator.CrossKindDataOperator;
import com.github.tgda.engine.core.operator.DataScienceOperator;
import com.github.tgda.engine.core.operator.SystemMaintenanceOperator;
import com.github.tgda.engine.core.operator.spi.neo4j.operatorImpl.Neo4JCrossKindDataOperatorImpl;
import com.github.tgda.engine.core.operator.spi.neo4j.operatorImpl.Neo4JDataScienceOperatorImpl;
import com.github.tgda.engine.core.operator.spi.neo4j.operatorImpl.Neo4JSystemMaintenanceOperatorImpl;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonEntitiesOperationResultImpl;
import com.github.tgda.engine.core.term.*;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JEngine;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.Constant;
import com.github.tgda.engine.core.util.config.PropertiesHandler;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.*;

public class Neo4JEngineImpl implements Neo4JEngine {

    private static Logger logger = LoggerFactory.getLogger(Neo4JEngineImpl.class);
    private String engine = null;

    public Neo4JEngineImpl(){
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    public Neo4JEngineImpl(String engine){
        this.engine = engine;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    @Override
    public StorageImplTech getStorageImplTech() {
        return StorageImplTech.NEO4J;
    }

    @Override
    public String getEngineName() {
        return engine != null ? engine : PropertiesHandler.getPropertyValue(PropertiesHandler.DEFAULT_REALM_NAME);
    }

    @Override
    public Type getType(String name) {
        if(name == null){
            return null;
        }

        if(name.startsWith("TGDA_")){
            Neo4JTypeImpl neo4JTypeImpl =
                    new Neo4JTypeImpl(engine, name,null,"0");
            neo4JTypeImpl.setGlobalGraphOperationExecutor(this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            return neo4JTypeImpl;
        }

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.TypeClass, Constant._NameProperty, name,1);
            GetSingleTypeTransformer getSingleTypeTransformer =
                    new GetSingleTypeTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object createTypeRes = workingGraphOperationExecutor.executeWrite(getSingleTypeTransformer,queryCql);
            return createTypeRes != null?(Type) createTypeRes:null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Type createType(String typeName, String typeDesc) {
        if(typeName == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String checkCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.TypeClass, Constant._NameProperty, typeName,1);
            Object conceptionKindExistenceRes = workingGraphOperationExecutor.executeRead(new CheckResultExistenceTransformer(),checkCql);
            if(((Boolean)conceptionKindExistenceRes).booleanValue()){
                return null;
            }
            Map<String,Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constant._NameProperty, typeName);
            if(typeDesc != null) {
                propertiesMap.put(Constant._DescProperty, typeDesc);
            }
            CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
            String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.TypeClass},propertiesMap);
            GetSingleTypeTransformer getSingleTypeTransformer =
                    new GetSingleTypeTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object createTypeRes = workingGraphOperationExecutor.executeWrite(getSingleTypeTransformer,createCql);
            Type type = createTypeRes != null ? (Type)createTypeRes : null;
            executeTypeCacheOperation(type,CacheOperationType.INSERT);
            return type;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Type createType(String name, String desc, String parentName) throws EngineFunctionNotSupportedException {
        EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
        exception.setCauseMessage("Neo4J storage implements doesn't support this function");
        throw exception;
    }

    @Override
    public boolean removeType(String name, boolean deleteExistEntities) throws EngineServiceRuntimeException {
        if(name == null){
            return false;
        }
        Type targetType =this.getType(name);
        if(targetType == null){
            logger.error("CoreRealm does not contains Type with name {}.", name);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("CoreRealm does not contains Type with name " + name + ".");
            throw exception;
        }else{
            if(deleteExistEntities){
                targetType.purgeAllEntities();
            }
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String conceptionKindUID = ((Neo4JTypeImpl)targetType).getTypeUID();
                String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.valueOf(conceptionKindUID),null,null);
                GetSingleTypeTransformer getSingleTypeTransformer =
                        new GetSingleTypeTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object deletedTypeRes = workingGraphOperationExecutor.executeWrite(getSingleTypeTransformer,deleteCql);
                Type resultType = deletedTypeRes != null ? (Type)deletedTypeRes : null;
                if(resultType == null){
                    throw new EngineServiceRuntimeException();
                }else{
                    String conceptionKindId = ((Neo4JTypeImpl)resultType).getTypeUID();
                    Neo4JTypeImpl resultNeo4JTypeImplForCacheOperation = new Neo4JTypeImpl(engine, name,null,conceptionKindId);
                    executeTypeCacheOperation(resultNeo4JTypeImplForCacheOperation,CacheOperationType.DELETE);
                    return true;
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    @Override
    public AttributesView getAttributesView(String attributesViewKindUID) {
        if(attributesViewKindUID == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(attributesViewKindUID), null, null);
            GetSingleAttributesViewTransformer getSingleAttributesViewTransformer =
                    new GetSingleAttributesViewTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object createAttributesViewRes = workingGraphOperationExecutor.executeWrite(getSingleAttributesViewTransformer,queryCql);
            return createAttributesViewRes != null ? (AttributesView)createAttributesViewRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public AttributesView createAttributesView(String attributesViewKindName, String attributesViewKindDesc, AttributesView.AttributesViewDataForm attributesViewKindDataForm) {
        if(attributesViewKindName == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            Map<String,Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constant._NameProperty,attributesViewKindName);
            if(attributesViewKindDesc != null) {
                propertiesMap.put(Constant._DescProperty, attributesViewKindDesc);
            }
            if(attributesViewKindDataForm != null) {
                propertiesMap.put(Constant._viewKindDataForm, attributesViewKindDataForm.toString());
            }else{
                propertiesMap.put(Constant._viewKindDataForm, AttributesView.AttributesViewDataForm.SINGLE_VALUE.toString());
            }
            CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
            String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.AttributesViewClass},propertiesMap);

            GetSingleAttributesViewTransformer getSingleAttributesViewTransformer =
                    new GetSingleAttributesViewTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object createAttributesViewRes = workingGraphOperationExecutor.executeWrite(getSingleAttributesViewTransformer,createCql);
            AttributesView resultKind = createAttributesViewRes != null ? (AttributesView)createAttributesViewRes : null;
            executeAttributesViewCacheOperation(resultKind,CacheOperationType.INSERT);
            return resultKind;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public boolean removeAttributesView(String attributesViewKindUID) throws EngineServiceRuntimeException {
        if(attributesViewKindUID == null){
            return false;
        }
        AttributesView targetAttributesView = this.getAttributesView(attributesViewKindUID);
        if(targetAttributesView != null){
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.valueOf(attributesViewKindUID),null,null);
                GetSingleAttributesViewTransformer getSingleAttributesViewTransformer =
                        new GetSingleAttributesViewTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object deletedAttributesViewRes = workingGraphOperationExecutor.executeWrite(getSingleAttributesViewTransformer,deleteCql);
                AttributesView resultKind = deletedAttributesViewRes != null ? (AttributesView)deletedAttributesViewRes : null;
                if(resultKind == null){
                    throw new EngineServiceRuntimeException();
                }else{
                    executeAttributesViewCacheOperation(resultKind,CacheOperationType.DELETE);
                    return true;
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }else{
            logger.error("AttributesView does not contains entity with UID {}.", attributesViewKindUID);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("AttributesView does not contains entity with UID " + attributesViewKindUID + ".");
            throw exception;
        }
    }

    @Override
    public List<AttributesView> getAttributesViews(String attributesViewKindName, String attributesViewKindDesc, AttributesView.AttributesViewDataForm attributesViewKindDataForm) {
        boolean alreadyHaveDefaultFilteringItem = false;
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(1000000);
        if(attributesViewKindName != null){
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._NameProperty,attributesViewKindName));
            alreadyHaveDefaultFilteringItem = true;
        }
        if(attributesViewKindDesc != null){
            if(alreadyHaveDefaultFilteringItem){
                queryParameters.addFilteringItem(new EqualFilteringItem(Constant._DescProperty,attributesViewKindDesc), QueryParameters.FilteringLogic.AND);
            }else{
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._DescProperty,attributesViewKindDesc));
                alreadyHaveDefaultFilteringItem = true;
            }
        }
        if(attributesViewKindDataForm != null){
            if(alreadyHaveDefaultFilteringItem){
                queryParameters.addFilteringItem(new EqualFilteringItem(Constant._viewKindDataForm,attributesViewKindDataForm.toString()), QueryParameters.FilteringLogic.AND);
            }else{
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._viewKindDataForm,attributesViewKindDataForm.toString()));
            }
        }
        try {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.AttributesViewClass,queryParameters,null);
                GetListAttributesViewTransformer getListAttributesViewTransformer =
                        new GetListAttributesViewTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object attributesViewKindsRes = workingGraphOperationExecutor.executeWrite(getListAttributesViewTransformer,queryCql);
                return attributesViewKindsRes != null ? (List<AttributesView>) attributesViewKindsRes : null;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public AttributeKind getAttribute(String attributeKindUID) {
        if(attributeKindUID == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(attributeKindUID), null, null);
            GetSingleAttributeKindTransformer getSingleAttributeKindTransformer =
                    new GetSingleAttributeKindTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object getAttributeKindRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeKindTransformer,queryCql);
            return getAttributeKindRes != null ? (AttributeKind)getAttributeKindRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public AttributeKind createAttribute(String attributeKindName, String attributeKindDesc, AttributeDataType attributeDataType) {
        if(attributeKindName == null || attributeDataType == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            Map<String,Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constant._NameProperty,attributeKindName);
            if(attributeKindDesc != null) {
                propertiesMap.put(Constant._DescProperty, attributeKindDesc);
            }
            propertiesMap.put(Constant._attributeDataType, attributeDataType.toString());
            CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
            String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.AttributeClass},propertiesMap);

            GetSingleAttributeKindTransformer getSingleAttributeKindTransformer =
                    new GetSingleAttributeKindTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object createAttributesViewRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeKindTransformer,createCql);
            AttributeKind resultKind = createAttributesViewRes != null ? (AttributeKind)createAttributesViewRes : null;
            executeAttributeCacheOperation(resultKind,CacheOperationType.INSERT);
            return resultKind;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public boolean removeAttribute(String attributeKindUID) throws EngineServiceRuntimeException {
        if(attributeKindUID == null){
            return false;
        }
        AttributeKind targetAttributeKind = this.getAttribute(attributeKindUID);
        if(targetAttributeKind != null){
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.valueOf(attributeKindUID),null,null);
                GetSingleAttributeKindTransformer getSingleAttributeKindTransformer =
                        new GetSingleAttributeKindTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object deletedAttributeKindRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeKindTransformer,deleteCql);
                AttributeKind resultKind = deletedAttributeKindRes != null ? (AttributeKind)deletedAttributeKindRes : null;
                if(resultKind == null){
                    throw new EngineServiceRuntimeException();
                }else{
                    executeAttributeCacheOperation(resultKind,CacheOperationType.DELETE);
                    return true;
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }else{
            logger.error("AttributeKind does not contains entity with UID {}.", attributeKindUID);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("AttributeKind does not contains entity with UID " + attributeKindUID + ".");
            throw exception;
        }
    }

    @Override
    public List<AttributeKind> getAttribute(String attributeKindName, String attributeKindDesc, AttributeDataType attributeDataType) {
        boolean alreadyHaveDefaultFilteringItem = false;
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(1000000);
        if(attributeKindName != null){
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._NameProperty,attributeKindName));
            alreadyHaveDefaultFilteringItem = true;
        }
        if(attributeKindDesc != null){
            if(alreadyHaveDefaultFilteringItem){
                queryParameters.addFilteringItem(new EqualFilteringItem(Constant._DescProperty,attributeKindDesc), QueryParameters.FilteringLogic.AND);
            }else{
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._DescProperty,attributeKindDesc));
                alreadyHaveDefaultFilteringItem = true;
            }
        }
        if(attributeDataType != null){
            if(alreadyHaveDefaultFilteringItem){
                queryParameters.addFilteringItem(new EqualFilteringItem(Constant._attributeDataType,attributeDataType.toString()), QueryParameters.FilteringLogic.AND);
            }else{
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._attributeDataType,attributeDataType.toString()));
            }
        }
        try {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.AttributeClass,queryParameters,null);
                GetListAttributeKindTransformer getListAttributeKindTransformer = new GetListAttributeKindTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object attributeKindsRes = workingGraphOperationExecutor.executeWrite(getListAttributeKindTransformer,queryCql);
                return attributeKindsRes != null ? (List<AttributeKind>) attributeKindsRes : null;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public RelationKind getRelationshipType(String relationKindName) {
        if(relationKindName == null){
            return null;
        }

        if(relationKindName.startsWith("TGDA_")){
            Neo4JRelationshipImplType neo4JRelationKindImpl =
                    new Neo4JRelationshipImplType(engine,relationKindName,null,"0");
            neo4JRelationKindImpl.setGlobalGraphOperationExecutor(this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            return neo4JRelationKindImpl;
        }

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.RelationKindClass, Constant._NameProperty,relationKindName,1);
            GetSingleRelationKindTransformer getSingleRelationKindTransformer =
                    new GetSingleRelationKindTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object getRelationKindRes = workingGraphOperationExecutor.executeWrite(getSingleRelationKindTransformer,queryCql);
            return getRelationKindRes != null ? (RelationKind)getRelationKindRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public RelationKind createRelationshipType(String relationKindName, String relationKindDesc) {
        if(relationKindName == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String checkCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.RelationKindClass, Constant._NameProperty,relationKindName,1);
            Object relationKindExistenceRes = workingGraphOperationExecutor.executeRead(new CheckResultExistenceTransformer(),checkCql);
            if(((Boolean)relationKindExistenceRes).booleanValue()){
                return null;
            }
            Map<String,Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constant._NameProperty,relationKindName);
            if(relationKindDesc != null) {
                propertiesMap.put(Constant._DescProperty, relationKindDesc);
            }
            CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
            String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.RelationKindClass},propertiesMap);
            GetSingleRelationKindTransformer getSingleRelationKindTransformer =
                    new GetSingleRelationKindTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object createRelationKindRes = workingGraphOperationExecutor.executeWrite(getSingleRelationKindTransformer,createCql);
            RelationKind resultKind = createRelationKindRes != null ? (RelationKind)createRelationKindRes : null;
            executeRelationshipTyepCacheOperation(resultKind,CacheOperationType.INSERT);
            return resultKind;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public RelationKind createRelationshipType(String relationKindName, String relationKindDesc, String parentRelationKindName) throws EngineFunctionNotSupportedException {
        EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
        exception.setCauseMessage("Neo4J storage implements doesn't support this function");
        throw exception;
    }

    @Override
    public boolean removeRelationshipType(String relationKindName, boolean deleteExistEntities) throws EngineServiceRuntimeException {
        if(relationKindName == null){
            return false;
        }
        RelationKind targetRelationKind = this.getRelationshipType(relationKindName);
        if(targetRelationKind == null){
            logger.error("RelationKind does not contains entity with UID {}.", relationKindName);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("RelationKind does not contains entity with UID " + relationKindName + ".");
            throw exception;
        }else{
            if(deleteExistEntities){
                targetRelationKind.purgeAllRelationEntities();
            }
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String relationKindUID = ((Neo4JRelationshipImplType)targetRelationKind).getRelationKindUID();
                String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.valueOf(relationKindUID),null,null);
                GetSingleRelationKindTransformer getSingleRelationKindTransformer =
                        new GetSingleRelationKindTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object deletedRelationKindRes = workingGraphOperationExecutor.executeWrite(getSingleRelationKindTransformer,deleteCql);
                RelationKind resultKind = deletedRelationKindRes != null ? (RelationKind)deletedRelationKindRes : null;
                if(resultKind == null){
                    throw new EngineServiceRuntimeException();
                }else{
                    String resultRelationKindUID = ((Neo4JRelationshipImplType)resultKind).getRelationKindUID();
                    Neo4JRelationshipImplType resultNeo4JRelationKindImplForCacheOperation = new Neo4JRelationshipImplType(engine,relationKindName,null,resultRelationKindUID);
                    executeRelationshipTyepCacheOperation(resultNeo4JRelationKindImplForCacheOperation,CacheOperationType.DELETE);
                    return true;
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    @Override
    public List<RelationAttachKind> getRelationshipAttaches(String relationAttachKindName, String relationAttachKindDesc, String sourceTypeName, String targetTypeName, String relationKindName, Boolean allowRepeatableRelationKind) {
        boolean alreadyHaveDefaultFilteringItem = false;
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(1000000);
        if(relationAttachKindName != null){
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._NameProperty,relationAttachKindName));
            alreadyHaveDefaultFilteringItem = true;
        }
        if(relationAttachKindDesc != null){
            if(alreadyHaveDefaultFilteringItem){
                queryParameters.addFilteringItem(new EqualFilteringItem(Constant._DescProperty,relationAttachKindDesc), QueryParameters.FilteringLogic.AND);
            }else{
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._DescProperty,relationAttachKindDesc));
                alreadyHaveDefaultFilteringItem = true;
            }
        }
        if(sourceTypeName != null){
            if(alreadyHaveDefaultFilteringItem){
                queryParameters.addFilteringItem(new EqualFilteringItem(Constant._relationAttachSourceKind,sourceTypeName), QueryParameters.FilteringLogic.AND);
            }else{
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._relationAttachSourceKind,sourceTypeName));
                alreadyHaveDefaultFilteringItem = true;
            }
        }
        if(targetTypeName != null){
            if(alreadyHaveDefaultFilteringItem){
                queryParameters.addFilteringItem(new EqualFilteringItem(Constant._relationAttachTargetKind,targetTypeName), QueryParameters.FilteringLogic.AND);
            }else{
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._relationAttachTargetKind,targetTypeName));
                alreadyHaveDefaultFilteringItem = true;
            }
        }
        if(relationKindName != null){
            if(alreadyHaveDefaultFilteringItem){
                queryParameters.addFilteringItem(new EqualFilteringItem(Constant._relationAttachRelationKind,relationKindName), QueryParameters.FilteringLogic.AND);
            }else{
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._relationAttachRelationKind,relationKindName));
                alreadyHaveDefaultFilteringItem = true;
            }
        }

        if(allowRepeatableRelationKind != null){
            boolean allowRepeatableRelationKindValue = allowRepeatableRelationKind.booleanValue();
            if(alreadyHaveDefaultFilteringItem){
                queryParameters.addFilteringItem(new EqualFilteringItem(Constant._relationAttachRepeatableRelationKind,allowRepeatableRelationKindValue), QueryParameters.FilteringLogic.AND);
            }else{
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant._relationAttachRepeatableRelationKind,allowRepeatableRelationKindValue));
            }
        }

        try {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.RelationAttachKindClass,queryParameters,null);
                GetListRelationAttachKindTransformer getListRelationAttachKindTransformer = new GetListRelationAttachKindTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object relationAttachKindsRes = workingGraphOperationExecutor.executeWrite(getListRelationAttachKindTransformer,queryCql);
                return relationAttachKindsRes != null ? (List<RelationAttachKind>) relationAttachKindsRes : null;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public RelationAttachKind getRelationshipAttach(String relationAttachKindUID) {
        if(relationAttachKindUID == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(relationAttachKindUID), null, null);
            GetSingleRelationAttachKindTransformer getSingleRelationAttachKindTransformer =
                    new GetSingleRelationAttachKindTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object getRelationAttachKindRes = workingGraphOperationExecutor.executeWrite(getSingleRelationAttachKindTransformer,queryCql);
            return getRelationAttachKindRes != null ? (RelationAttachKind)getRelationAttachKindRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public RelationAttachKind createRelationshipAttach(String relationAttachKindName, String relationAttachKindDesc, String sourceTypeName, String targetTypeName, String relationKindName, boolean allowRepeatableRelationKind) throws EngineFunctionNotSupportedException {
        if(relationAttachKindName == null || sourceTypeName== null || targetTypeName == null || relationKindName == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String checkCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.RelationAttachKindClass, Constant._NameProperty,relationAttachKindName,1);
            Object relationAttachKindExistenceRes = workingGraphOperationExecutor.executeRead(new CheckResultExistenceTransformer(),checkCql);
            if(((Boolean)relationAttachKindExistenceRes).booleanValue()){
                return null;
            }

            Map<String,Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constant._NameProperty,relationAttachKindName);
            if(relationAttachKindDesc != null) {
                propertiesMap.put(Constant._DescProperty, relationAttachKindDesc);
            }
            propertiesMap.put(Constant._relationAttachSourceKind,sourceTypeName);
            propertiesMap.put(Constant._relationAttachTargetKind,targetTypeName);
            propertiesMap.put(Constant._relationAttachRelationKind,relationKindName);
            propertiesMap.put(Constant._relationAttachRepeatableRelationKind,allowRepeatableRelationKind);
            CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);

            String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.RelationAttachKindClass},propertiesMap);

            GetSingleRelationAttachKindTransformer getSingleRelationAttachKindTransformer =
                    new GetSingleRelationAttachKindTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object createRelationAttachKindRes = workingGraphOperationExecutor.executeWrite(getSingleRelationAttachKindTransformer,createCql);
            RelationAttachKind resultKind = createRelationAttachKindRes != null ? (RelationAttachKind)createRelationAttachKindRes : null;
            executeRelationshipAttachCacheOperation(resultKind,CacheOperationType.INSERT);
            return resultKind;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public boolean removeRelationAttach(String relationAttachKindUID) throws EngineServiceRuntimeException {
        if(relationAttachKindUID == null){
            return false;
        }
        RelationAttachKind targetRelationAttachKind = this.getRelationshipAttach(relationAttachKindUID);
        if(targetRelationAttachKind != null){
            List<RelationAttachLinkLogic> relationAttachLinkLogicList = targetRelationAttachKind.getRelationAttachLinkLogic();
            if(relationAttachLinkLogicList != null){
                for(RelationAttachLinkLogic currentRelationAttachLinkLogic:relationAttachLinkLogicList){
                    targetRelationAttachKind.removeRelationAttachLinkLogic(currentRelationAttachLinkLogic.getRelationAttachLinkLogicUID());
                }
            }
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.valueOf(relationAttachKindUID),null,null);
                GetSingleRelationAttachKindTransformer getSingleRelationAttachKindTransformer =
                        new GetSingleRelationAttachKindTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object deletedRelationAttachKindRes = workingGraphOperationExecutor.executeWrite(getSingleRelationAttachKindTransformer,deleteCql);
                RelationAttachKind resultKind = deletedRelationAttachKindRes != null ? (RelationAttachKind)deletedRelationAttachKindRes : null;
                if(resultKind == null){
                    throw new EngineServiceRuntimeException();
                }else{
                    executeRelationshipAttachCacheOperation(resultKind,CacheOperationType.DELETE);
                    return true;
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }else{
            logger.error("RelationAttachKind does not contains entity with UID {}.", relationAttachKindUID);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("RelationAttachKind does not contains entity with UID " + relationAttachKindUID + ".");
            throw exception;
        }
    }

    @Override
    public Classification getClassification(String classificationName) {
        if(classificationName == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.ClassificationClass, Constant._NameProperty,classificationName,1);
            GetSingleClassificationTransformer getSingleClassificationTransformer =
                    new GetSingleClassificationTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object classificationRes = workingGraphOperationExecutor.executeWrite(getSingleClassificationTransformer,queryCql);
            return classificationRes != null?(Classification)classificationRes:null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Classification createClassification(String classificationName, String classificationDesc) {
        if(classificationName == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String checkCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.ClassificationClass, Constant._NameProperty,classificationName,1);
            Object classificationExistenceRes = workingGraphOperationExecutor.executeRead(new CheckResultExistenceTransformer(),checkCql);
            if(((Boolean)classificationExistenceRes).booleanValue()){
                return null;
            }
            Map<String,Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constant._NameProperty,classificationName);
            if(classificationDesc != null) {
                propertiesMap.put(Constant._DescProperty, classificationDesc);
            }
            CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
            String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.ClassificationClass},propertiesMap);
            GetSingleClassificationTransformer getSingleClassificationTransformer =
                    new GetSingleClassificationTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object createClassificationRes = workingGraphOperationExecutor.executeWrite(getSingleClassificationTransformer,createCql);
            Classification targetClassification = createClassificationRes != null ? (Classification)createClassificationRes : null;
            executeClassificationCacheOperation(targetClassification,CacheOperationType.INSERT);
            return targetClassification;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Classification createClassification(String classificationName, String classificationDesc, String parentClassificationName) throws EngineServiceRuntimeException {
        if(classificationName == null || parentClassificationName == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            //check Parent Classification exist
            String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.ClassificationClass, Constant._NameProperty,parentClassificationName,1);
            GetSingleClassificationTransformer getSingleClassificationTransformer =
                    new GetSingleClassificationTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object parentClassificationRes = workingGraphOperationExecutor.executeWrite(getSingleClassificationTransformer,queryCql);
            if(parentClassificationRes == null){
                logger.error("Classification named {} doesn't exist.", parentClassificationName);
                EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                exception.setCauseMessage("Classification named "+parentClassificationName+" doesn't exist.");
                throw exception;
            }
            String checkCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.ClassificationClass, Constant._NameProperty,classificationName,1);
            Object classificationExistenceRes = workingGraphOperationExecutor.executeRead(new CheckResultExistenceTransformer(),checkCql);
            if(((Boolean)classificationExistenceRes).booleanValue()){
                return null;
            }
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
                executeClassificationCacheOperation(targetClassification,CacheOperationType.INSERT);

                String childConceptionUID = ((Neo4JClassificationImpl)targetClassification).getClassificationUID();
                String parentConceptionUID = ((Neo4JClassificationImpl)parentClassificationRes).getClassificationUID();
                Map<String,Object> relationPropertiesMap = new HashMap<>();
                CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesMap);
                String createRelationCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(childConceptionUID),Long.parseLong(parentConceptionUID),
                        Constant.Classification_ClassificationRelationClass,relationPropertiesMap);
                GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (Constant.Classification_ClassificationRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object newRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, createRelationCql);
                if(newRelationshipEntityRes == null){
                    logger.error("Set Classification {}'s parent to Classification {} fail.", classificationName,parentClassificationName);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("Set Classification "+classificationName+"'s parent to Classification "+parentClassificationName+" fail.");
                    throw exception;
                }
            }
            return targetClassification;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public boolean removeClassification(String classificationName) throws EngineServiceRuntimeException {
        return this.removeClassification(classificationName,false);
    }

    @Override
    public boolean removeClassificationWithOffspring(String classificationName) throws EngineServiceRuntimeException {
        return this.removeClassification(classificationName,true);
    }

    @Override
    public Entity newMultiEntity(String[] conceptionKindNames, EntityValue conceptionEntityValue, boolean addPerDefinedRelation) throws EngineServiceRuntimeException {
        if(conceptionKindNames == null || conceptionKindNames.length == 0){
            logger.error("At least one Conception Kind Name is required.");
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("At least one Conception Kind Name is required.");
            throw exception;
        }
        if (conceptionEntityValue != null) {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try {
                Map<String, Object> propertiesMap = conceptionEntityValue.getEntityAttributesValue() != null ?
                        conceptionEntityValue.getEntityAttributesValue() : new HashMap<>();
                CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
                String createCql = CypherBuilder.createLabeledNodeWithProperties(conceptionKindNames, propertiesMap);
                GetSingleEntityTransformer getSingleEntityTransformer =
                        new GetSingleEntityTransformer(conceptionKindNames[0], this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object newEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer, createCql);
                Entity resultEntity = newEntityRes != null ? (Entity) newEntityRes : null;
                if(addPerDefinedRelation && resultEntity != null){
                    List<String> uidList = new ArrayList<>();
                    uidList.add(resultEntity.getEntityUID());
                    for(String currentTypeName:conceptionKindNames){
                        CommonOperationUtil.attachEntities(currentTypeName,uidList,workingGraphOperationExecutor);
                    }
                }
                return resultEntity;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public Entity newMultiEntity(String[] conceptionKindNames, EntityValue conceptionEntityValue, List<RelationshipAttach> relationAttachKindList, RelationshipAttach.EntityRelateRole entityRelateRole) throws EngineServiceRuntimeException {
        Entity conceptionEntity = newMultiEntity(conceptionKindNames,conceptionEntityValue,false);
        if(relationAttachKindList != null){
            for(RelationshipAttach currentRelationAttachKind : relationAttachKindList){
                currentRelationAttachKind.newRelationEntities(conceptionEntity.getEntityUID(),entityRelateRole,null);
            }
        }
        return conceptionEntity;
    }

    @Override
    public EntitiesOperationResult newMultiConceptionEntities(String[] conceptionKindNames, List<EntityValue> conceptionEntityValues, boolean addPerDefinedRelation) throws EngineServiceRuntimeException {
        if(conceptionKindNames == null || conceptionKindNames.length == 0){
            logger.error("At least one Conception Kind Name is required.");
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("At least one Conception Kind Name is required.");
            throw exception;
        }

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
                String createCql = CypherBuilder.createMultiLabeledNodesWithProperties(conceptionKindNames, attributesValueMap);
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
                            setOperationSummary("newEntities operation for multi conceptionKind success.");
                }
                commonEntitiesOperationResultImpl.finishEntitiesOperation();
                if(addPerDefinedRelation && commonEntitiesOperationResultImpl.getSuccessEntityUIDs() != null){
                    for(String currentTypeName:conceptionKindNames){
                        CommonOperationUtil.attachEntities(currentTypeName,commonEntitiesOperationResultImpl.getSuccessEntityUIDs(),workingGraphOperationExecutor);
                    }
                }
                return commonEntitiesOperationResultImpl;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public EntitiesOperationResult newMultiConceptionEntities(String[] conceptionKindNames, List<EntityValue> conceptionEntityValues, List<RelationAttachKind> relationAttachKindList, RelationAttachKind.EntityRelateRole entityRelateRole) throws EngineServiceRuntimeException {
        EntitiesOperationResult entitiesOperationResult = newMultiConceptionEntities(conceptionKindNames,conceptionEntityValues,false);
        if(relationAttachKindList != null){
            for(RelationAttachKind currentRelationAttachKind : relationAttachKindList){
                currentRelationAttachKind.newRelationEntities(entitiesOperationResult.getSuccessEntityUIDs(),entityRelateRole,null);
            }
        }
        return entitiesOperationResult;
    }

    @Override
    public List<Map<String,Map<String,Object>>> executeCustomQuery(String customQuerySentence) throws EngineServiceRuntimeException {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            GetListMapTransformer getListMapTransformer = new GetListMapTransformer();
            Object newEntityRes = workingGraphOperationExecutor.executeWrite(getListMapTransformer, customQuerySentence);
            return newEntityRes != null ? (List<Map<String,Map<String,Object>>>)newEntityRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Map<String, Number> executeCustomStatistic(String customQuerySentence) throws EngineServiceRuntimeException {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
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
            Object statisticCqlRes = workingGraphOperationExecutor.executeRead(resultHandleDataTransformer,customQuerySentence);
            if(statisticCqlRes != null){
                return (Map<String,Number>)statisticCqlRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public TimeFlow getOrCreateTimeFlow() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.TimeFlowClass, Constant._NameProperty, Constant._defaultTimeFlowName,1);
            GetSingleTimeFlowTransformer getSingleTimeFlowTransformer =
                    new GetSingleTimeFlowTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object getDefaultTimeFlowRes = workingGraphOperationExecutor.executeRead(getSingleTimeFlowTransformer,queryCql);
            if(getDefaultTimeFlowRes == null){
                Map<String,Object> propertiesMap = new HashMap<>();
                propertiesMap.put(Constant._NameProperty, Constant._defaultTimeFlowName);
                CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
                String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.TimeFlowClass},propertiesMap);
                Object createTimeFlowRes = workingGraphOperationExecutor.executeWrite(getSingleTimeFlowTransformer,createCql);
                TimeFlow resultTimeFlow = createTimeFlowRes != null ? (TimeFlow)createTimeFlowRes : null;
                return resultTimeFlow;
            }
            return (TimeFlow)getDefaultTimeFlowRes;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public TimeFlow getOrCreateTimeFlow(String timeFlowName) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.TimeFlowClass, Constant._NameProperty,timeFlowName,1);
            GetSingleTimeFlowTransformer getSingleTimeFlowTransformer =
                    new GetSingleTimeFlowTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object getDefaultTimeFlowRes = workingGraphOperationExecutor.executeRead(getSingleTimeFlowTransformer,queryCql);
            if(getDefaultTimeFlowRes == null){
                Map<String,Object> propertiesMap = new HashMap<>();
                propertiesMap.put(Constant._NameProperty,timeFlowName);
                CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
                String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.TimeFlowClass},propertiesMap);
                Object createTimeFlowRes = workingGraphOperationExecutor.executeWrite(getSingleTimeFlowTransformer,createCql);
                TimeFlow resultTimeFlow = createTimeFlowRes != null ? (TimeFlow)createTimeFlowRes : null;
                return resultTimeFlow;
            }
            return (TimeFlow)getDefaultTimeFlowRes;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public long removeTimeFlowWithEntities() {
        return removeTimeFlowWithEntitiesLogic(Constant._defaultTimeFlowName);
    }

    @Override
    public long removeTimeFlowWithEntities(String timeFlowName) {
        return removeTimeFlowWithEntitiesLogic(timeFlowName);
    }

    @Override
    public List<TimeFlow> getTimeFlows() {
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(1000000);
        try {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.TimeFlowClass,queryParameters,null);
                GetListTimeFlowTransformer getListTimeFlowTransformer =
                        new GetListTimeFlowTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object timeFlowsRes = workingGraphOperationExecutor.executeRead(getListTimeFlowTransformer,queryCql);
                return timeFlowsRes != null ? (List<TimeFlow>) timeFlowsRes : null;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Geospatial getOrCreateGeospatial() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.GeospatialClass, Constant._NameProperty, Constant._defaultGeospatialName,1);
            GetSingleGeospatialTransformer getSingleGeospatialTransformer =
                    new GetSingleGeospatialTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object getDefaultGeospatialRes = workingGraphOperationExecutor.executeRead(getSingleGeospatialTransformer,queryCql);
            if(getDefaultGeospatialRes == null){
                Map<String,Object> propertiesMap = new HashMap<>();
                propertiesMap.put(Constant._NameProperty, Constant._defaultGeospatialName);
                CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
                String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.GeospatialClass},propertiesMap);
                Object createGeospatialRes = workingGraphOperationExecutor.executeWrite(getSingleGeospatialTransformer,createCql);
                Geospatial resultNeo4JGeospatialImpl = createGeospatialRes != null ? (Neo4JGeospatialImpl)createGeospatialRes : null;
                return resultNeo4JGeospatialImpl;
            }
            return (Geospatial)getDefaultGeospatialRes;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Geospatial getOrCreateGeospatial(String geospatialRegionName) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.GeospatialClass, Constant._NameProperty,geospatialRegionName,1);
            GetSingleGeospatialTransformer getSingleGeospatialTransformer =
                    new GetSingleGeospatialTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object getGeospatialRes = workingGraphOperationExecutor.executeRead(getSingleGeospatialTransformer,queryCql);
            if(getGeospatialRes == null){
                Map<String,Object> propertiesMap = new HashMap<>();
                propertiesMap.put(Constant._NameProperty,geospatialRegionName);
                CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
                String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.GeospatialClass},propertiesMap);
                Object createGeospatialRes = workingGraphOperationExecutor.executeWrite(getSingleGeospatialTransformer,createCql);
                Geospatial resultTimeFlow = createGeospatialRes != null ? (Neo4JGeospatialImpl)createGeospatialRes : null;
                return resultTimeFlow;
            }
            return (Geospatial)getGeospatialRes;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public long removeGeospatialWithEntities() {
        return removeGeospatialWithEntitiesLogic(Constant._defaultGeospatialName);
    }

    @Override
    public long removeGeospatialWithEntities(String geospatialRegionName) {
        return removeGeospatialWithEntitiesLogic(geospatialRegionName);
    }

    @Override
    public List<Geospatial> getGeospatials() {
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(1000000);
        try {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialClass,queryParameters,null);
                GetListGeospatialTransformer getListGeospatialTransformer =
                        new GetListGeospatialTransformer(engine,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object geospatialRegionsRes = workingGraphOperationExecutor.executeRead(getListGeospatialTransformer,queryCql);
                return geospatialRegionsRes != null ? (List<Geospatial>) geospatialRegionsRes : null;
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<EntityStatisticsInfo> getConceptionEntitiesStatistics() throws EngineServiceEntityExploreException {
        List<EntityStatisticsInfo> entityStatisticsInfoList = new ArrayList<>();
        String cypherProcedureString = "CALL db.labels()\n" +
            "YIELD label\n" +
            "CALL apoc.cypher.run(\"MATCH (:`\"+label+\"`) RETURN count(*) as count\", null)\n" +
            "YIELD value\n" +
            "RETURN label, value.count as count\n" +
            "ORDER BY label";
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            List<String> attributesNameList = new ArrayList<>();
            Map<String,HashMap<String,Object>> conceptionKindMetaDataMap = new HashMap<>();
            attributesNameList.add(Constant._NameProperty);
            attributesNameList.add(Constant._DescProperty);
            attributesNameList.add(Constant._createDateProperty);
            attributesNameList.add(Constant._lastModifyDateProperty);
            attributesNameList.add(Constant._creatorIdProperty);
            attributesNameList.add(Constant._dataOriginProperty);
            String queryCql = CypherBuilder.matchAttributesWithQueryParameters(Constant.TypeClass,null,attributesNameList);
            DataTransformer conceptionKindInfoDataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    while(result.hasNext()){
                        Record nodeRecord = result.next();
                        String conceptionKindName = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._NameProperty).asString();
                        String conceptionKindDesc = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._DescProperty).asString();
                        ZonedDateTime createDate = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._createDateProperty).asZonedDateTime();
                        ZonedDateTime lastModifyDate = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._lastModifyDateProperty).asZonedDateTime();
                        String dataOrigin = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._dataOriginProperty).asString();
                        long conceptionKindUID = nodeRecord.get("id("+CypherBuilder.operationResultName+")").asLong();
                        String creatorId = nodeRecord.containsKey(CypherBuilder.operationResultName+"."+ Constant._creatorIdProperty) ?
                                nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._creatorIdProperty).asString():null;

                        HashMap<String,Object> metaDataMap = new HashMap<>();
                        metaDataMap.put(Constant._NameProperty,conceptionKindName);
                        metaDataMap.put(Constant._DescProperty,conceptionKindDesc);
                        metaDataMap.put(Constant._createDateProperty,createDate);
                        metaDataMap.put(Constant._lastModifyDateProperty,lastModifyDate);
                        metaDataMap.put(Constant._creatorIdProperty,creatorId);
                        metaDataMap.put(Constant._dataOriginProperty,dataOrigin);
                        metaDataMap.put("TypeUID",""+conceptionKindUID);
                        conceptionKindMetaDataMap.put(conceptionKindName,metaDataMap);
                    }
                    return null;
                }
            };
            workingGraphOperationExecutor.executeRead(conceptionKindInfoDataTransformer,queryCql);

            DataTransformer queryResultDataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    List<String> conceptionKindsNameWithDataList = new ArrayList<>();
                    while(result.hasNext()){
                        Record currentRecord = result.next();
                        String entityKind = currentRecord.get("label").asString();
                        long entityCount = currentRecord.get("count").asLong();
                        conceptionKindsNameWithDataList.add(entityKind);
                        EntityStatisticsInfo currentEntityStatisticsInfo = null;
                        if(entityKind.startsWith("TGDA_")){
                            currentEntityStatisticsInfo = new EntityStatisticsInfo(
                                    entityKind, EntityStatisticsInfo.kindType.Type, true, entityCount);
                        }else{
                            if(conceptionKindMetaDataMap.containsKey(entityKind)){
                                currentEntityStatisticsInfo = new EntityStatisticsInfo(
                                        entityKind, EntityStatisticsInfo.kindType.Type, false, entityCount,
                                        conceptionKindMetaDataMap.get(entityKind).get(Constant._DescProperty).toString(),
                                        conceptionKindMetaDataMap.get(entityKind).get("TypeUID").toString(),
                                        (ZonedDateTime) (conceptionKindMetaDataMap.get(entityKind).get(Constant._createDateProperty)),
                                        (ZonedDateTime) (conceptionKindMetaDataMap.get(entityKind).get(Constant._lastModifyDateProperty)),
                                        conceptionKindMetaDataMap.get(entityKind).get(Constant._creatorIdProperty).toString(),
                                        conceptionKindMetaDataMap.get(entityKind).get(Constant._dataOriginProperty).toString()
                                );
                            }
                        }
                        if(currentEntityStatisticsInfo != null){
                            entityStatisticsInfoList.add(currentEntityStatisticsInfo);
                        }
                    }

                    //如果 Type 尚未有实体数据，则 CALL db.labels() 不会返回该 Kind 记录，需要从 Type 列表反向查找
                    Set<String> allTypeNameSet = conceptionKindMetaDataMap.keySet();
                    for(String currentKindName :allTypeNameSet ){
                        if(!conceptionKindsNameWithDataList.contains(currentKindName)){
                            //当前 Type 中无数据，需要手动添加信息
                            EntityStatisticsInfo currentEntityStatisticsInfo = new EntityStatisticsInfo(
                                    currentKindName, EntityStatisticsInfo.kindType.Type, false, 0,
                                    conceptionKindMetaDataMap.get(currentKindName).get(Constant._DescProperty).toString(),
                                    conceptionKindMetaDataMap.get(currentKindName).get("TypeUID").toString(),
                                    (ZonedDateTime) (conceptionKindMetaDataMap.get(currentKindName).get(Constant._createDateProperty)),
                                    (ZonedDateTime) (conceptionKindMetaDataMap.get(currentKindName).get(Constant._lastModifyDateProperty)),
                                    conceptionKindMetaDataMap.get(currentKindName).get(Constant._creatorIdProperty).toString(),
                                    conceptionKindMetaDataMap.get(currentKindName).get(Constant._dataOriginProperty).toString()
                            );
                            entityStatisticsInfoList.add(currentEntityStatisticsInfo);
                        }
                    }
                    return null;
                    }
                };
                workingGraphOperationExecutor.executeRead(queryResultDataTransformer,cypherProcedureString);
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return entityStatisticsInfoList;
    }

    @Override
    public List<EntityStatisticsInfo> getRelationEntitiesStatistics() {
        List<EntityStatisticsInfo> entityStatisticsInfoList = new ArrayList<>();
        String cypherProcedureString = "CALL db.relationshipTypes()\n" +
                "YIELD relationshipType\n" +
                "CALL apoc.cypher.run(\"MATCH ()-[:\" + `relationshipType` + \"]->()\n" +
                "RETURN count(*) as count\", null)\n" +
                "YIELD value\n" +
                "RETURN relationshipType, value.count AS count\n" +
                "ORDER BY relationshipType";
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            List<String> attributesNameList = new ArrayList<>();
            Map<String,HashMap<String,Object>> relationKindMetaDataMap = new HashMap<>();
            attributesNameList.add(Constant._NameProperty);
            attributesNameList.add(Constant._DescProperty);
            attributesNameList.add(Constant._createDateProperty);
            attributesNameList.add(Constant._lastModifyDateProperty);
            attributesNameList.add(Constant._creatorIdProperty);
            attributesNameList.add(Constant._dataOriginProperty);
            String queryCql = CypherBuilder.matchAttributesWithQueryParameters(Constant.RelationKindClass,null,attributesNameList);
            DataTransformer relationKindInfoDataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    while(result.hasNext()){
                        Record nodeRecord = result.next();
                        String relationKindName = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._NameProperty).asString();
                        String relationKindDesc = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._DescProperty).asString();
                        ZonedDateTime createDate = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._createDateProperty).asZonedDateTime();
                        ZonedDateTime lastModifyDate = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._lastModifyDateProperty).asZonedDateTime();
                        String dataOrigin = nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._dataOriginProperty).asString();
                        long conceptionKindUID = nodeRecord.get("id("+CypherBuilder.operationResultName+")").asLong();
                        String creatorId = nodeRecord.containsKey(CypherBuilder.operationResultName+"."+ Constant._creatorIdProperty) ?
                                nodeRecord.get(CypherBuilder.operationResultName+"."+ Constant._creatorIdProperty).asString():null;

                        HashMap<String,Object> metaDataMap = new HashMap<>();
                        metaDataMap.put(Constant._NameProperty,relationKindName);
                        metaDataMap.put(Constant._DescProperty,relationKindDesc);
                        metaDataMap.put(Constant._createDateProperty,createDate);
                        metaDataMap.put(Constant._lastModifyDateProperty,lastModifyDate);
                        metaDataMap.put(Constant._creatorIdProperty,creatorId);
                        metaDataMap.put(Constant._dataOriginProperty,dataOrigin);
                        metaDataMap.put("RelationKindUID",""+conceptionKindUID);
                        relationKindMetaDataMap.put(relationKindName,metaDataMap);
                    }
                    return null;
                }
            };
            workingGraphOperationExecutor.executeRead(relationKindInfoDataTransformer,queryCql);
            DataTransformer queryResultDataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    List<String> conceptionKindsNameWithDataList = new ArrayList<>();
                    while(result.hasNext()){
                        Record currentRecord = result.next();
                        String entityKind = currentRecord.get("relationshipType").asString();
                        long entityCount = currentRecord.get("count").asLong();
                        conceptionKindsNameWithDataList.add(entityKind);
                        EntityStatisticsInfo currentEntityStatisticsInfo = null;
                        if(entityKind.startsWith("TGDA_")){
                            currentEntityStatisticsInfo = new EntityStatisticsInfo(
                                    entityKind, EntityStatisticsInfo.kindType.RelationKind, true, entityCount);
                        }else{
                            if(relationKindMetaDataMap.containsKey(entityKind)){
                                currentEntityStatisticsInfo = new EntityStatisticsInfo(
                                        entityKind, EntityStatisticsInfo.kindType.RelationKind, false, entityCount,
                                        relationKindMetaDataMap.get(entityKind).get(Constant._DescProperty).toString(),
                                        relationKindMetaDataMap.get(entityKind).get("RelationKindUID").toString(),
                                        (ZonedDateTime) (relationKindMetaDataMap.get(entityKind).get(Constant._createDateProperty)),
                                        (ZonedDateTime) (relationKindMetaDataMap.get(entityKind).get(Constant._lastModifyDateProperty)),
                                        relationKindMetaDataMap.get(entityKind).get(Constant._creatorIdProperty).toString(),
                                        relationKindMetaDataMap.get(entityKind).get(Constant._dataOriginProperty).toString()
                                );
                            }
                        }
                        if(currentEntityStatisticsInfo != null){
                            entityStatisticsInfoList.add(currentEntityStatisticsInfo);
                        }
                    }
                    //如果 Type 尚未有实体数据，则 CALL db.relationshipTypes() 不会返回该 Kind 记录，需要从 Type 列表反向查找
                    Set<String> allTypeNameSet = relationKindMetaDataMap.keySet();
                    for(String currentKindName :allTypeNameSet ){
                        if(!conceptionKindsNameWithDataList.contains(currentKindName)){
                            //当前 Type 中无数据，需要手动添加信息
                            EntityStatisticsInfo currentEntityStatisticsInfo = new EntityStatisticsInfo(
                                    currentKindName, EntityStatisticsInfo.kindType.Type, false, 0,
                                    relationKindMetaDataMap.get(currentKindName).get(Constant._DescProperty).toString(),
                                    relationKindMetaDataMap.get(currentKindName).get("RelationKindUID").toString(),
                                    (ZonedDateTime) (relationKindMetaDataMap.get(currentKindName).get(Constant._createDateProperty)),
                                    (ZonedDateTime) (relationKindMetaDataMap.get(currentKindName).get(Constant._lastModifyDateProperty)),
                                    relationKindMetaDataMap.get(currentKindName).get(Constant._creatorIdProperty).toString(),
                                    relationKindMetaDataMap.get(currentKindName).get(Constant._dataOriginProperty).toString()
                            );
                            entityStatisticsInfoList.add(currentEntityStatisticsInfo);
                        }
                    }
                    return null;
                }
            };
            workingGraphOperationExecutor.executeRead(queryResultDataTransformer,cypherProcedureString);
        } catch (EngineServiceEntityExploreException e) {
            throw new RuntimeException(e);
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return entityStatisticsInfoList;
    }

    @Override
    public List<TypeCorrelationInfo> getTypesCorrelation() {
        List<TypeCorrelationInfo> conceptionKindCorrelationInfoList = new ArrayList<>();
        String cypherProcedureString = "CALL apoc.meta.graph";

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            DataTransformer queryResultDataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {

                    if(result.hasNext()){
                        Record currentRecord = result.next();
                        List nodesList = currentRecord.get("nodes").asList();

                        Map<String,String> conceptionKindMetaInfoMap = new HashMap<>();
                        for(Object currentNode:nodesList){
                            Node currentNeo4JNode = (Node)currentNode;
                            conceptionKindMetaInfoMap.put(""+currentNeo4JNode.id(),currentNeo4JNode.get("name").asString());
                        }
                        List relationList =  currentRecord.get("relationships").asList();
                        for(Object currenRelation:relationList){
                            Relationship currentNeo4JRelation = (Relationship)currenRelation;
                            String relationKindName = currentNeo4JRelation.type();
                            String startTypeName = conceptionKindMetaInfoMap.get(""+currentNeo4JRelation.startNodeId());
                            String targetTypeName = conceptionKindMetaInfoMap.get(""+currentNeo4JRelation.endNodeId());
                            int relationEntityCount = currentNeo4JRelation.get("count").asInt();
                            conceptionKindCorrelationInfoList.add(new TypeCorrelationInfo(startTypeName,
                                    targetTypeName,relationKindName,relationEntityCount)
                            );
                        }
                    }
                    return null;
                }
            };
            workingGraphOperationExecutor.executeRead(queryResultDataTransformer,cypherProcedureString);
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return conceptionKindCorrelationInfoList;
    }

    @Override
    public CrossKindDataOperator getCrossKindDataOperator() {
        Neo4JCrossKindDataOperatorImpl crossKindDataOperator= new Neo4JCrossKindDataOperatorImpl(this);
        crossKindDataOperator.setGlobalGraphOperationExecutor(this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
        return crossKindDataOperator;
    }

    @Override
    public SystemMaintenanceOperator getSystemMaintenanceOperator() {
        Neo4JSystemMaintenanceOperatorImpl systemMaintenanceOperatorImpl = new Neo4JSystemMaintenanceOperatorImpl(engine);
        systemMaintenanceOperatorImpl.setGlobalGraphOperationExecutor(this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
        return systemMaintenanceOperatorImpl;
    }

    @Override
    public DataScienceOperator getDataScienceOperator() {
        Neo4JDataScienceOperatorImpl neo4JDataScienceOperatorImpl = new Neo4JDataScienceOperatorImpl(engine);
        neo4JDataScienceOperatorImpl.setGlobalGraphOperationExecutor(this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
        return neo4JDataScienceOperatorImpl;
    }

    @Override
    public List<KindMetaInfo> getTypesMetaInfo() throws EngineServiceEntityExploreException {
        return getKindsMetaInfo(Constant.TypeClass);
    }

    @Override
    public List<KindMetaInfo> getRelationshipTypsMetaInfo() throws EngineServiceEntityExploreException {
        return getKindsMetaInfo(Constant.RelationKindClass);
    }

    @Override
    public List<KindMetaInfo> getAttributesMetaInfo() throws EngineServiceEntityExploreException {
        return getKindsMetaInfo(Constant.AttributeClass);
    }

    @Override
    public List<KindMetaInfo> getAttributesViewsMetaInfo() throws EngineServiceEntityExploreException {
        return getKindsMetaInfo(Constant.AttributesViewClass);
    }

    @Override
    public void openGlobalSession() {
        GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
        this.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }

    @Override
    public void closeGlobalSession() {
        if(this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor() != null){
            this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor().close();
            this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(null);
        }
    }

    private List<KindMetaInfo> getKindsMetaInfo(String kindClassName) throws EngineServiceEntityExploreException {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            List<String> attributesNameList = new ArrayList<>();
            attributesNameList.add(Constant._NameProperty);
            attributesNameList.add(Constant._DescProperty);
            attributesNameList.add(Constant._createDateProperty);
            attributesNameList.add(Constant._lastModifyDateProperty);
            attributesNameList.add(Constant._creatorIdProperty);
            attributesNameList.add(Constant._dataOriginProperty);
            String queryCql = CypherBuilder.matchAttributesWithQueryParameters(kindClassName,null,attributesNameList);

            GetListKindMetaInfoTransformer getListKindMetaInfoTransformer = new GetListKindMetaInfoTransformer();
            Object kindMetaInfoListRes = workingGraphOperationExecutor.executeRead(getListKindMetaInfoTransformer,queryCql);
            return kindMetaInfoListRes != null ? (List<KindMetaInfo>) kindMetaInfoListRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    private boolean removeClassification(String classificationName, boolean cascadingDeleteOffspring) throws EngineServiceRuntimeException {
        if(classificationName == null){
            return false;
        }
        Classification targetClassification = this.getClassification(classificationName);
        if(targetClassification == null){
            logger.error("CoreRealm does not contains Classification with name {}.", classificationName);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("CoreRealm does not contains Classification with name " + classificationName + ".");
            throw exception;
        }else{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                if(!cascadingDeleteOffspring) {
                    String classificationUID = ((Neo4JClassificationImpl) targetClassification).getClassificationUID();
                    String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.valueOf(classificationUID), null, null);
                    GetSingleClassificationTransformer getSingleClassificationTransformer =
                            new GetSingleClassificationTransformer(engine, this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                    Object deletedClassificationRes = workingGraphOperationExecutor.executeWrite(getSingleClassificationTransformer, deleteCql);
                    Classification resultClassification = deletedClassificationRes != null ? (Classification) deletedClassificationRes : null;
                    if (resultClassification == null) {
                        throw new EngineServiceRuntimeException();
                    } else {
                        String classificationId = ((Neo4JClassificationImpl) resultClassification).getClassificationUID();
                        Neo4JClassificationImpl resultNeo4JClassificationImplForCacheOperation = new Neo4JClassificationImpl(engine, classificationName, null, classificationId);
                        executeClassificationCacheOperation(resultNeo4JClassificationImplForCacheOperation, CacheOperationType.DELETE);
                        return true;
                    }
                }else{
                    String classificationUID = ((Neo4JClassificationImpl) targetClassification).getClassificationUID();
                    List<Object> withOffspringClassificationUIDList = new ArrayList<>();
                    String queryCql = CypherBuilder.matchRelatedNodesAndRelationsFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID, Long.parseLong(classificationUID),
                            Constant.ClassificationClass, Constant.Classification_ClassificationRelationClass, Direction.FROM,0,0, CypherBuilder.ReturnRelationableDataType.BOTH);
                    withOffspringClassificationUIDList.add(Long.parseLong(classificationUID));
                    DataTransformer offspringClassificationsDataTransformer = new DataTransformer() {
                        @Override
                        public Object transformResult(Result result) {
                            List<Record> recordList = result.list();
                            if(recordList != null){
                                for(Record nodeRecord : recordList){
                                    Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                                    long nodeUID = resultNode.id();
                                    withOffspringClassificationUIDList.add(nodeUID);
                                }
                            }
                            return null;
                        }
                    };
                    workingGraphOperationExecutor.executeRead(offspringClassificationsDataTransformer,queryCql);

                    List<Object> deletedClassificationUIDList = new ArrayList<>();
                    String deleteCql = CypherBuilder.deleteNodesWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, withOffspringClassificationUIDList);
                    DataTransformer deleteOffspringClassificationsDataTransformer = new DataTransformer() {
                        @Override
                        public Object transformResult(Result result) {
                            List<Record> recordList = result.list();
                            if(recordList != null){
                                for(Record nodeRecord : recordList){
                                    Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                                    long nodeUID = resultNode.id();
                                    deletedClassificationUIDList.add(nodeUID);
                                }
                            }
                            return null;
                        }
                    };
                    workingGraphOperationExecutor.executeWrite(deleteOffspringClassificationsDataTransformer,deleteCql);

                    if(deletedClassificationUIDList.size() == withOffspringClassificationUIDList.size()){
                        return true;
                    }else{
                        logger.error("Not all offspring classifications of Classification {} are successful removed.", classificationName);
                        EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                        exception.setCauseMessage("Not all offspring classifications of Classification "+classificationName+" are successful removed.");
                        throw exception;

                    }
                }
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
    }

    private long removeTimeFlowWithEntitiesLogic(String timeFlowName){
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            String deleteEntitiesCql = "CALL apoc.periodic.commit(\"MATCH (n:"+ Constant.TimeScaleEntityClass+") WHERE n.timeFlow='"+timeFlowName+"' WITH n LIMIT $limit DETACH DELETE n RETURN count(*)\",{limit: 10000}) YIELD updates, executions, runtime, batches RETURN updates, executions, runtime, batches";
            logger.debug("Generated Cypher Statement: {}", deleteEntitiesCql);

            DataTransformer<Long> deleteTransformer = new DataTransformer() {
                @Override
                public Long transformResult(Result result) {
                    while(result.hasNext()){
                        Record nodeRecord = result.next();
                        Long deletedTimeScaleEntitiesNumber =  nodeRecord.get("updates").asLong();
                        return deletedTimeScaleEntitiesNumber;
                    }
                    return null;
                }
            };
            Object deleteEntitiesRes = workingGraphOperationExecutor.executeWrite(deleteTransformer,deleteEntitiesCql);
            long currentDeletedEntitiesCount = deleteEntitiesRes != null ? ((Long)deleteEntitiesRes).longValue():0;

            String deleteTimeFlowCql = "MATCH (n:"+ Constant.TimeFlowClass+") WHERE n.name='"+timeFlowName+"' DETACH DELETE n RETURN COUNT(n) as "+CypherBuilder.operationResultName+"";
            logger.debug("Generated Cypher Statement: {}", deleteTimeFlowCql);
            GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer();
            deleteEntitiesRes = workingGraphOperationExecutor.executeWrite(getLongFormatAggregatedReturnValueTransformer,deleteTimeFlowCql);
            long currentDeletedFlowsCount = deleteEntitiesRes != null ? ((Long)deleteEntitiesRes).longValue():0;

            return currentDeletedEntitiesCount + currentDeletedFlowsCount;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    private long removeGeospatialWithEntitiesLogic(String geospatialRegionName){
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            String deleteEntitiesCql = "CALL apoc.periodic.commit(\"MATCH (n:"+ Constant.GeospatialScaleEntityClass+") WHERE n."+ Constant.GeospatialClass+"='"+geospatialRegionName+"' WITH n LIMIT $limit DETACH DELETE n RETURN count(*)\",{limit: 10000}) YIELD updates, executions, runtime, batches RETURN updates, executions, runtime, batches";
            logger.debug("Generated Cypher Statement: {}", deleteEntitiesCql);

            DataTransformer<Long> deleteTransformer = new DataTransformer() {
                @Override
                public Long transformResult(Result result) {
                    while(result.hasNext()){
                        Record nodeRecord = result.next();
                        Long deletedTimeScaleEntitiesNumber =  nodeRecord.get("updates").asLong();
                        return deletedTimeScaleEntitiesNumber;
                    }
                    return null;
                }
            };
            Object deleteEntitiesRes = workingGraphOperationExecutor.executeWrite(deleteTransformer,deleteEntitiesCql);
            long currentDeletedEntitiesCount = deleteEntitiesRes != null ? ((Long)deleteEntitiesRes).longValue():0;

            String deleteTimeFlowCql = "MATCH (n:"+ Constant.GeospatialClass+") WHERE n.name='"+geospatialRegionName+"' DETACH DELETE n RETURN COUNT(n) as "+CypherBuilder.operationResultName+"";
            logger.debug("Generated Cypher Statement: {}", deleteTimeFlowCql);
            GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer();
            deleteEntitiesRes = workingGraphOperationExecutor.executeWrite(getLongFormatAggregatedReturnValueTransformer,deleteTimeFlowCql);
            long currentDeletedFlowsCount = deleteEntitiesRes != null ? ((Long)deleteEntitiesRes).longValue():0;

            return currentDeletedEntitiesCount + currentDeletedFlowsCount;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }
}
