package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.*;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.payload.AttributeValue;
import com.github.tgda.engine.core.term.Direction;
import com.github.tgda.engine.core.term.RelationshipEntity;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JAttributesView;
import com.github.tgda.engine.core.util.Constant;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4JAttributesViewImpl implements Neo4JAttributesView {

    private static Logger logger = LoggerFactory.getLogger(Neo4JTypeImpl.class);
    private String coreRealmName;
    private String attributesViewKindName;
    private String attributesViewKindDesc;
    private String attributesViewKindUID;
    private AttributesViewKindDataForm attributesViewKindDataForm;

    public Neo4JAttributesViewImpl(String coreRealmName, String attributesViewKindName, String attributesViewKindDesc, AttributesViewKindDataForm attributesViewKindDataForm, String attributesViewKindUID){
        this.coreRealmName = coreRealmName;
        this.attributesViewKindName = attributesViewKindName;
        this.attributesViewKindDesc = attributesViewKindDesc;
        this.attributesViewKindUID = attributesViewKindUID;
        this.attributesViewKindDataForm = attributesViewKindDataForm != null ? attributesViewKindDataForm : AttributesViewKindDataForm.SINGLE_VALUE;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    @Override
    public String getAttributesViewKindUID() {
        return attributesViewKindUID;
    }

    @Override
    public String getAttributesViewKindName() {
        return attributesViewKindName;
    }

    @Override
    public String getAttributesViewKindDesc() {
        return attributesViewKindDesc;
    }

    @Override
    public boolean updateAttributesViewKindDesc(String kindDesc) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            Map<String,Object> attributeDataMap = new HashMap<>();
            attributeDataMap.put(Constant._DescProperty, kindDesc);
            String updateCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.attributesViewKindUID),attributeDataMap);
            GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._DescProperty);
            Object updateResultRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeValueTransformer,updateCql);
            CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.attributesViewKindUID,false);
            AttributeValue resultAttributeValue =  updateResultRes != null ? (AttributeValue) updateResultRes : null;
            if(resultAttributeValue != null && resultAttributeValue.getAttributeValue().toString().equals(kindDesc)){
                this.attributesViewKindDesc = kindDesc;
                return true;
            }else{
                return false;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public boolean isCollectionAttributesViewKind() {
        boolean isCollectionAttributesViewKind = false;
        switch (attributesViewKindDataForm){
            case SINGLE_VALUE: isCollectionAttributesViewKind = false;
                break;
            case LIST_VALUE: isCollectionAttributesViewKind = true;
                break;
            case RELATED_VALUE: isCollectionAttributesViewKind = true;
                break;
            case EXTERNAL_VALUE: isCollectionAttributesViewKind = true;
        }
        return isCollectionAttributesViewKind;
    }

    @Override
    public AttributesViewKindDataForm getAttributesViewKindDataForm() {
        return attributesViewKindDataForm;
    }

    @Override
    public boolean attachAttribute(String attributeKindUID) throws EngineServiceRuntimeException {
        return attachAttribute(attributeKindUID,null);
    }

    @Override
    public boolean attachAttribute(String attributeKindUID, Map<String, Object> properties) throws EngineServiceRuntimeException {
        if(attributeKindUID == null){
            return false;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(attributeKindUID), null, null);
            GetSingleAttributeTransformer getSingleAttributeTransformer = new GetSingleAttributeTransformer(coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object checkAttributeRes = workingGraphOperationExecutor.executeRead(getSingleAttributeTransformer,queryCql);
            if(checkAttributeRes != null){
                String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(attributesViewKindUID),Long.parseLong(attributeKindUID),
                        Constant.AttributesViewKind_AttributeRelationClass);

                GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (Constant.AttributesViewKind_AttributeRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, queryRelationCql);
                if(existingRelationshipEntityRes != null){
                    return true;
                }

                Map<String,Object> relationPropertiesMap = properties != null ? properties:new HashMap<>();
                CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesMap);
                String createCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(attributesViewKindUID),Long.parseLong(attributeKindUID),
                        Constant.AttributesViewKind_AttributeRelationClass,relationPropertiesMap);
                getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (Constant.AttributesViewKind_AttributeRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object newRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, createCql);
                if(newRelationshipEntityRes == null){
                    throw new EngineServiceRuntimeException();
                }else{
                    return true;
                }
            }else{
                logger.error("Attribute does not contains entity with UID {}.", attributeKindUID);
                EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                exception.setCauseMessage("Attribute does not contains entity with UID " + attributeKindUID + ".");
                throw exception;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<String> setAttributeAttachMetaInfo(String attributeKindUID, Map<String,Object> properties) {
        if(attributeKindUID == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(this.attributesViewKindUID),Long.parseLong(attributeKindUID), Constant.AttributesViewKind_AttributeRelationClass);
            GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer =
                    new GetSingleRelationshipEntityTransformer(Constant.AttributesViewKind_AttributeRelationClass,workingGraphOperationExecutor);
            Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer, queryRelationCql);
            if(existingRelationshipEntityRes == null){
                return null;
            }
           // TODO
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public boolean removeAttributeAttachMetaInfo(String attributeKindUID, String metaPropertyName) throws EngineServiceRuntimeException {
        if(attributeKindUID == null){
            return false;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(this.attributesViewKindUID),Long.parseLong(attributeKindUID), Constant.AttributesViewKind_AttributeRelationClass);
            GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer =
                    new GetSingleRelationshipEntityTransformer(Constant.AttributesViewKind_AttributeRelationClass,workingGraphOperationExecutor);
            Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer, queryRelationCql);
            if(existingRelationshipEntityRes == null){
                return false;
            }else{
                RelationshipEntity targetRelationshipEntity = (RelationshipEntity)existingRelationshipEntityRes;
                return targetRelationshipEntity.removeAttribute(metaPropertyName);
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Map<String, Object> getAttributesAttachMetaInfo(String metaPropertyName) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            Map<String, Object> resultValueMap = new HashMap<>();
            String queryRelationCql = CypherBuilder.matchRelatedNodeAndRelationPairsFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID,
                    Long.parseLong(this.attributesViewKindUID), Constant.AttributeClass, Constant.AttributesViewKind_AttributeRelationClass,Direction.TO);
            DataTransformer dataTransformer = new DataTransformer() {
                
                @Override
                public Object transformResult(Result result) {
                    while(result.hasNext()){
                        Record nodeRecord = result.next();
                        Node attributeKindNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                        Relationship attachMetaInfoRelation = nodeRecord.get(CypherBuilder.relationResultName).asRelationship();

                        long relationUID = attachMetaInfoRelation.id();
                        String relationEntityUID = ""+relationUID;
                        String fromEntityUID = ""+attachMetaInfoRelation.startNodeId();
                        String toEntityUID = ""+attachMetaInfoRelation.endNodeId();
                        Neo4JRelationshipEntityImpl neo4jRelationshipEntityImpl =
                                new Neo4JRelationshipEntityImpl(Constant.AttributesViewKind_AttributeRelationClass,relationEntityUID,fromEntityUID,toEntityUID);
                        neo4jRelationshipEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                        Object propertyValue = neo4jRelationshipEntityImpl.getAttribute(metaPropertyName).getAttributeValue();
                        resultValueMap.put(""+attributeKindNode.id(),propertyValue);
                    }
                    return null;
                }
            };
            workingGraphOperationExecutor.executeRead(dataTransformer,queryRelationCql);
            return resultValueMap;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public Object getAttributeAttachMetaInfo(String attributeKindUID, String metaPropertyName) {
        if(attributeKindUID == null){
            return null;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(this.attributesViewKindUID),Long.parseLong(attributeKindUID), Constant.AttributesViewKind_AttributeRelationClass);
            GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer =
                    new GetSingleRelationshipEntityTransformer(Constant.AttributesViewKind_AttributeRelationClass,workingGraphOperationExecutor);
            Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer, queryRelationCql);
            if(existingRelationshipEntityRes != null){
                RelationshipEntity targetRelationshipEntity = (RelationshipEntity)existingRelationshipEntityRes;
                if(targetRelationshipEntity.hasAttribute(metaPropertyName)){
                    return targetRelationshipEntity.getAttribute(metaPropertyName).getAttributeValue();
                }
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public boolean detachAttribute(String attributeKindUID) throws EngineServiceRuntimeException {
        if(attributeKindUID == null){
            return false;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(attributeKindUID), null, null);
            GetSingleAttributeTransformer getSingleAttributeTransformer = new GetSingleAttributeTransformer(coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object checkAttributeRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeTransformer,queryCql);
            if(checkAttributeRes != null){
                String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(attributesViewKindUID),Long.parseLong(attributeKindUID),
                        Constant.AttributesViewKind_AttributeRelationClass);

                GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (Constant.AttributesViewKind_AttributeRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, queryRelationCql);
                if(existingRelationshipEntityRes == null){
                    return false;
                }
                RelationshipEntity relationEntity = (RelationshipEntity)existingRelationshipEntityRes;

                String deleteCql = CypherBuilder.deleteRelationWithSingleFunctionValueEqual(
                        CypherBuilder.CypherFunctionType.ID,Long.valueOf(relationEntity.getRelationshipEntityUID()),null,null);

                getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (Constant.AttributesViewKind_AttributeKindRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
                Object deleteRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, deleteCql);
                if(deleteRelationshipEntityRes == null){
                    return false;
                }else{
                    return true;
                }
            }else{
                logger.error("Attribute does not contains entity with UID {}.", attributeKindUID);
                EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                exception.setCauseMessage("Attribute does not contains entity with UID " + attributeKindUID + ".");
                throw exception;
            }

        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<Attribute> getContainsAttributes() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchRelatedNodesFromSpecialStartNodes(
                    CypherBuilder.CypherFunctionType.ID, Long.parseLong(attributesViewKindUID),
                    Constant.AttributeClass, Constant.AttributesViewKind_AttributeRelationClass, Direction.TO, null);
            GetListAttributeTransformer getListAttributeTransformer = new GetListAttributeTransformer(Constant.AttributeClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object attributeKindsRes = workingGraphOperationExecutor.executeWrite(getListAttributeTransformer,queryCql);
            return attributeKindsRes != null ? (List<Attribute>) attributeKindsRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<Type> getContainerTypes() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchRelatedNodesFromSpecialStartNodes(
                    CypherBuilder.CypherFunctionType.ID, Long.parseLong(attributesViewKindUID),
                    Constant.TypeClass, Constant.Type_AttributesViewRelationClass, Direction.FROM, null);
            GetListTypeTransformer getListTypeTransformer = new GetListTypeTransformer(this.coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object conceptionKindsRes = workingGraphOperationExecutor.executeWrite(getListTypeTransformer,queryCql);
            return conceptionKindsRes != null ? (List<Type>) conceptionKindsRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }

    @Override
    public String getEntityUID() {
        return this.attributesViewKindUID;
    }

    @Override
    public GraphOperationExecutorHelper getGraphOperationExecutorHelper() {
        return this.graphOperationExecutorHelper;
    }
}
