package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.coreRealm.realmServiceCore.analysis.query.filteringItem.*;
import com.github.tgda.engine.core.analysis.query.filteringItem.*;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.coreRealm.realmServiceCore.internal.neo4j.dataTransformer.*;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.*;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.payload.AttributeValue;
import com.github.tgda.engine.core.payload.EntitiesOperationResult;
import com.github.tgda.engine.core.payload.RelationshipAttachLinkLogic;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonEntitiesOperationResultImpl;
import com.github.tgda.engine.core.term.Direction;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.RelationshipEntity;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JRelationshipAttach;
import com.github.tgda.engine.core.util.Constant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4JRelationshipAttachImpl implements Neo4JRelationshipAttach {

    private static Logger logger = LoggerFactory.getLogger(Neo4JRelationshipAttachImpl.class);

    private String coreRealmName;
    private String relationAttachKindName;
    private String relationAttachKindDesc;
    private String relationAttachKindUID;
    private String sourceConceptionKindName;
    private String targetConceptionKindName;
    private String relationKindName;
    private boolean allowRepeatableRelationKind;

    public Neo4JRelationshipAttachImpl(String coreRealmName, String relationAttachKindName, String relationAttachKindDesc, String relationAttachKindUID,
                                       String sourceConceptionKindName, String targetConceptionKindName, String relationKindName, boolean allowRepeatableRelationKind){
        this.coreRealmName = coreRealmName;
        this.relationAttachKindName = relationAttachKindName;
        this.relationAttachKindDesc = relationAttachKindDesc;
        this.relationAttachKindUID = relationAttachKindUID;
        this.sourceConceptionKindName = sourceConceptionKindName;
        this.targetConceptionKindName = targetConceptionKindName;
        this.relationKindName = relationKindName;
        this.allowRepeatableRelationKind = allowRepeatableRelationKind;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    @Override
    public String getRelationAttachKindUID() {
        return this.relationAttachKindUID;
    }

    @Override
    public String getSourceConceptionKindName() {
        return this.sourceConceptionKindName;
    }

    @Override
    public String getTargetConceptionKindName() {
        return this.targetConceptionKindName;
    }

    @Override
    public String getRelationKindName() {
        return this.relationKindName;
    }

    @Override
    public String getRelationAttachKindName() {
        return this.relationAttachKindName;
    }

    @Override
    public String getRelationAttachKindDesc() {
        return this.relationAttachKindDesc;
    }

    @Override
    public boolean updateRelationAttachKindDesc(String newDesc) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            Map<String,Object> attributeDataMap = new HashMap<>();
            attributeDataMap.put(Constant._DescProperty, newDesc);
            String updateCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.relationAttachKindUID),attributeDataMap);
            GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._DescProperty);
            Object updateResultRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeValueTransformer,updateCql);
            CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.relationAttachKindUID,false);
            AttributeValue resultAttributeValue =  updateResultRes != null ? (AttributeValue) updateResultRes : null;
            if(resultAttributeValue != null && resultAttributeValue.getAttributeValue().toString().equals(newDesc)){
                this.relationAttachKindDesc = newDesc;
                return true;
            }else{
                return false;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public List<RelationshipAttachLinkLogic> getRelationAttachLinkLogic() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchRelatedNodesFromSpecialStartNodes(
                    CypherBuilder.CypherFunctionType.ID, Long.parseLong(this.relationAttachKindUID),
                    Constant.RelationAttachLinkLogicClass, Constant.RelationAttachKind_RelationAttachLinkLogicRelationClass, Direction.TO, null);
            GetListRelationshipAttachLinkLogicTransformer getListRelationAttachLinkLogicTransformer = new GetListRelationshipAttachLinkLogicTransformer(coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object relationAttachLinkLogicsRes = workingGraphOperationExecutor.executeWrite(getListRelationAttachLinkLogicTransformer,queryCql);
            return relationAttachLinkLogicsRes != null ? (List<RelationshipAttachLinkLogic>) relationAttachLinkLogicsRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public RelationshipAttachLinkLogic createRelationAttachLinkLogic(RelationshipAttachLinkLogic relationshipAttachLinkLogic) throws EngineServiceRuntimeException {
        if(relationshipAttachLinkLogic.getLinkLogicType().equals(LinkLogicType.DEFAULT)){
            List<RelationshipAttachLinkLogic> relationshipAttachLinkLogicList = getRelationAttachLinkLogic();
            for(RelationshipAttachLinkLogic currentRelationshipAttachLinkLogic : relationshipAttachLinkLogicList){
                if(currentRelationshipAttachLinkLogic.getLinkLogicType().equals(LinkLogicType.DEFAULT)){
                    logger.error("RelationAttachKind {} already contains DEFAULT LinkLogicType.", this.relationAttachKindName);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("RelationAttachKind "+this.relationAttachKindName+" already contains DEFAULT LinkLogicType.");
                    throw exception;
                }
            }
        }

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            Map<String,Object> propertiesMap = new HashMap<>();
            propertiesMap.put(Constant._attachLinkLogicType, relationshipAttachLinkLogic.getLinkLogicType().toString());
            propertiesMap.put(Constant._attachLinkLogicCondition, relationshipAttachLinkLogic.getLinkLogicCondition().toString());
            propertiesMap.put(Constant._attachLinkLogicSourceAttribute, relationshipAttachLinkLogic.getSourceEntityLinkAttributeName());
            propertiesMap.put(Constant._attachLinkLogicTargetAttribute, relationshipAttachLinkLogic.getTargetEntitiesLinkAttributeName());
            CommonOperationUtil.generateEntityMetaAttributes(propertiesMap);
            String createCql = CypherBuilder.createLabeledNodeWithProperties(new String[]{Constant.RelationAttachLinkLogicClass},propertiesMap);
            GetSingleRelationshipAttachLinkLogicTransformer getSingleRelationAttachLinkLogicTransformer =
                    new GetSingleRelationshipAttachLinkLogicTransformer(coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object createLinkLogicRes = workingGraphOperationExecutor.executeWrite(getSingleRelationAttachLinkLogicTransformer,createCql);
            RelationshipAttachLinkLogic targetRelationshipAttachLinkLogic = createLinkLogicRes != null ? (RelationshipAttachLinkLogic)createLinkLogicRes : null;

            Map<String,Object> relationPropertiesMap = new HashMap<>();
            CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesMap);
            String linkCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(this.relationAttachKindUID),Long.parseLong(targetRelationshipAttachLinkLogic.getRelationAttachLinkLogicUID()),
                    Constant.RelationAttachKind_RelationAttachLinkLogicRelationClass,relationPropertiesMap);
            GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                    (Constant.RelationAttachKind_RelationAttachLinkLogicRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object newRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, linkCql);
            if(newRelationshipEntityRes == null){
                throw new EngineServiceRuntimeException();
            }
            return targetRelationshipAttachLinkLogic;
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public boolean removeRelationAttachLinkLogic(String relationAttachLinkLogicUID) throws EngineServiceRuntimeException {
        boolean isValidRelationAttachLinkLogic = false;
        List<RelationshipAttachLinkLogic> relationshipAttachLinkLogicList = getRelationAttachLinkLogic();
        for(RelationshipAttachLinkLogic currentRelationshipAttachLinkLogic : relationshipAttachLinkLogicList){
            if(currentRelationshipAttachLinkLogic.getRelationAttachLinkLogicUID().equals(relationAttachLinkLogicUID)){
                isValidRelationAttachLinkLogic = true;
                break;
            }
        }
        if(!isValidRelationAttachLinkLogic){
            logger.error("RelationAttachKind {} does not contain relationAttachLinkLogic with UID {}.", this.relationAttachKindName,relationAttachLinkLogicUID);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("RelationAttachKind "+this.relationAttachKindName+" does not contain relationAttachLinkLogic with UID "+relationAttachLinkLogicUID+".");
            throw exception;
        }
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String deleteCql = CypherBuilder.deleteNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.valueOf(relationAttachLinkLogicUID),null,null);
            GetSingleRelationshipAttachLinkLogicTransformer getSingleRelationAttachLinkLogicTransformer =
                    new GetSingleRelationshipAttachLinkLogicTransformer(coreRealmName,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object deletedAttributesViewKindRes = workingGraphOperationExecutor.executeWrite(getSingleRelationAttachLinkLogicTransformer,deleteCql);
            RelationshipAttachLinkLogic resultKind = deletedAttributesViewKindRes != null ? (RelationshipAttachLinkLogic)deletedAttributesViewKindRes : null;
            if(resultKind == null){
                throw new EngineServiceRuntimeException();
            }else{
                return true;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public long newRelationEntities(String conceptionEntityUID, EntityRelateRole entityRelateRole, Map<String,Object> relationData) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            return newRelationEntities(workingGraphOperationExecutor,conceptionEntityUID,entityRelateRole,relationData);
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public long newRelationEntities(List<String> conceptionEntityUIDs, EntityRelateRole entityRelateRole, Map<String, Object> relationData) {
        if(conceptionEntityUIDs != null && conceptionEntityUIDs.size()>0){
            long totalResultNumber = 0;
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            try{
                for(String currentEntityID : conceptionEntityUIDs){
                    long currentResultNumber = newRelationEntities(workingGraphOperationExecutor,currentEntityID,entityRelateRole,relationData);
                    totalResultNumber = totalResultNumber + currentResultNumber;
                }
                return totalResultNumber;
            } finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return 0;
    }

    @Override
    public EntitiesOperationResult newUniversalRelationEntities(Map<String,Object> relationData) {
        CommonEntitiesOperationResultImpl entitiesOperationResult = new CommonEntitiesOperationResultImpl();
        entitiesOperationResult.getOperationStatistics().setSuccessItemsCount(0);

        String sourceKind = this.sourceConceptionKindName;
        String targetKind = this.targetConceptionKindName;

        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            String queryCql = CypherBuilder.matchLabelWithSinglePropertyValueAndFunction(sourceKind, CypherBuilder.CypherFunctionType.COUNT, null, null);
            GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer("count");
            Object countConceptionEntitiesRes = workingGraphOperationExecutor.executeRead(getLongFormatAggregatedReturnValueTransformer, queryCql);
            long sourceKindDataNumber = countConceptionEntitiesRes != null ?((Long)countConceptionEntitiesRes).longValue() : 0 ;
            queryCql = CypherBuilder.matchLabelWithSinglePropertyValueAndFunction(targetKind, CypherBuilder.CypherFunctionType.COUNT, null, null);
            countConceptionEntitiesRes = workingGraphOperationExecutor.executeRead(getLongFormatAggregatedReturnValueTransformer, queryCql);
            long targetKindDataNumber = countConceptionEntitiesRes != null ?((Long)countConceptionEntitiesRes).longValue() : 0 ;
            if(sourceKindDataNumber != 0 && targetKindDataNumber != 0){
                long successItemsCount = 0;
                String cacheKindName = sourceKindDataNumber > targetKindDataNumber ? sourceKind : targetKind;
                EntityRelateRole entityRelateRole = sourceKindDataNumber > targetKindDataNumber ? EntityRelateRole.SOURCE : EntityRelateRole.TARGET;
                String queryCacheIDsCql = CypherBuilder.matchLabelWithSinglePropertyValueAndFunction(cacheKindName, CypherBuilder.CypherFunctionType.ID, null, null);
                GetListObjectValueTransformer<Long> getListObjectValueTransformer = new GetListObjectValueTransformer("id");
                Object idListRes = workingGraphOperationExecutor.executeRead(getListObjectValueTransformer, queryCacheIDsCql);
                List<Long> idList = idListRes != null ? (List<Long>)idListRes : null;
                if(idList != null){
                    for(Long currentEntityId:idList){
                        successItemsCount = successItemsCount + newRelationEntities(workingGraphOperationExecutor,""+currentEntityId.longValue(),entityRelateRole,relationData);
                    }
               }
                entitiesOperationResult.getOperationStatistics().setSuccessItemsCount(successItemsCount);
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        entitiesOperationResult.finishEntitiesOperation();
        return entitiesOperationResult;
    }

    @Override
    public boolean isRepeatableRelationKindAllow() {
        return this.allowRepeatableRelationKind;
    }

    @Override
    public boolean setAllowRepeatableRelationKind(boolean allowRepeatableRelationKind) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            Map<String,Object> attributeDataMap = new HashMap<>();
            attributeDataMap.put(Constant._relationAttachRepeatableRelationKind, allowRepeatableRelationKind);
            String updateCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.relationAttachKindUID),attributeDataMap);
            GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._relationAttachRepeatableRelationKind);
            Object updateResultRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeValueTransformer,updateCql);
            CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.relationAttachKindUID,false);
            AttributeValue resultAttributeValue = updateResultRes != null ? (AttributeValue) updateResultRes : null;
            if(resultAttributeValue != null ){
                Boolean currentValue = (Boolean)resultAttributeValue.getAttributeValue();
                this.allowRepeatableRelationKind = currentValue.booleanValue();
                return currentValue.booleanValue();
            }else{
                return false;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    private FilteringItem generateFilteringItem(LinkLogicCondition linkLogicCondition, String attributeName, Object attributeValue){
        FilteringItem filteringItem = null;
        switch(linkLogicCondition){
            case Equal:
                filteringItem = new EqualFilteringItem(attributeName,attributeValue);
                break;
            case GreaterThanEqual:
                filteringItem = new GreaterThanEqualFilteringItem(attributeName,attributeValue);
                break;
            case GreaterThan:
                filteringItem = new GreaterThanFilteringItem(attributeName,attributeValue);
                break;
            case LessThanEqual:
                filteringItem = new LessThanEqualFilteringItem(attributeName,attributeValue);
                break;
            case LessThan:
                filteringItem = new LessThanFilteringItem(attributeName,attributeValue);
                break;
            case NotEqual:
                filteringItem = new NotEqualFilteringItem(attributeName,attributeValue);
                break;
            case RegularMatch:
                filteringItem = new RegularMatchFilteringItem(attributeName,attributeValue.toString());
                break;
            case BeginWithSimilar:
                filteringItem = new SimilarFilteringItem(attributeName,attributeValue.toString(),SimilarFilteringItem.MatchingType.BeginWith);
                break;
            case EndWithSimilar:
                filteringItem = new SimilarFilteringItem(attributeName,attributeValue.toString(),SimilarFilteringItem.MatchingType.EndWith);
                break;
            case ContainSimilar:
                filteringItem = new SimilarFilteringItem(attributeName,attributeValue.toString(),SimilarFilteringItem.MatchingType.Contain);
        }
        return filteringItem;
    }

    private long newRelationEntities(GraphOperationExecutor workingGraphOperationExecutor, String conceptionEntityUID, EntityRelateRole entityRelateRole, Map<String,Object> relationData) {
        try{
            String queryCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(conceptionEntityUID), null, null);
            GetSingleEntityTransformer getSingleEntityTransformer =
                    new GetSingleEntityTransformer(null, workingGraphOperationExecutor);
            Object resEntityRes = workingGraphOperationExecutor.executeRead(getSingleEntityTransformer, queryCql);
            if(resEntityRes != null){
                Entity currentEntity = (Entity)resEntityRes;
                List<String> conceptionKindNames = currentEntity.getAllConceptionKindNames();
                String linkTargetConceptionKind = null;
                switch(entityRelateRole){
                    case SOURCE:
                        if(!conceptionKindNames.contains(this.sourceConceptionKindName)){
                            return 0;
                        }
                        linkTargetConceptionKind = this.targetConceptionKindName;
                        break;
                    case TARGET:
                        if(!conceptionKindNames.contains(this.targetConceptionKindName)){
                            return 0;
                        }
                        linkTargetConceptionKind = this.sourceConceptionKindName;
                }

                long resultRelationCount = 0;
                QueryParameters queryParameters = new QueryParameters();
                queryParameters.setDistinctMode(true);
                queryParameters.setResultNumber(100000000);
                List<RelationshipAttachLinkLogic> relationshipAttachLinkLogicList = this.getRelationAttachLinkLogic();

                boolean hasDefaultFilteringItem = false;
                for(RelationshipAttachLinkLogic currentRelationshipAttachLinkLogic : relationshipAttachLinkLogicList){
                    LinkLogicType linkLogicType = currentRelationshipAttachLinkLogic.getLinkLogicType();
                    switch(linkLogicType){
                        case DEFAULT:
                            hasDefaultFilteringItem = true;
                            break;
                    }
                }
                if(!hasDefaultFilteringItem){
                    logger.error("RelationAttachKind {} doesn't contains DEFAULT LinkLogicType.", this.relationAttachKindName);
                    return 0;
                }

                for(RelationshipAttachLinkLogic currentRelationshipAttachLinkLogic : relationshipAttachLinkLogicList){
                    LinkLogicType linkLogicType = currentRelationshipAttachLinkLogic.getLinkLogicType();
                    LinkLogicCondition linkLogicCondition = currentRelationshipAttachLinkLogic.getLinkLogicCondition();

                    String knownEntityLinkAttributeName = null;
                    String unKnownEntitiesLinkAttributeName = null;

                    switch(entityRelateRole){
                        case SOURCE:
                            knownEntityLinkAttributeName = currentRelationshipAttachLinkLogic.getSourceEntityLinkAttributeName();
                            unKnownEntitiesLinkAttributeName = currentRelationshipAttachLinkLogic.getTargetEntitiesLinkAttributeName();
                            break;
                        case TARGET:
                            knownEntityLinkAttributeName = currentRelationshipAttachLinkLogic.getTargetEntitiesLinkAttributeName();
                            unKnownEntitiesLinkAttributeName = currentRelationshipAttachLinkLogic.getSourceEntityLinkAttributeName();
                    }

                    if(currentEntity.getAttribute(knownEntityLinkAttributeName) != null){
                        FilteringItem filteringItem = generateFilteringItem(linkLogicCondition,unKnownEntitiesLinkAttributeName,
                                currentEntity.getAttribute(knownEntityLinkAttributeName).getAttributeValue());
                        switch(linkLogicType){
                            case DEFAULT:
                                queryParameters.setDefaultFilteringItem(filteringItem);
                                break;
                            case AND:
                                queryParameters.addFilteringItem(filteringItem, QueryParameters.FilteringLogic.AND);
                                break;
                            case OR:
                                queryParameters.addFilteringItem(filteringItem, QueryParameters.FilteringLogic.OR);
                        }
                    }
                }
                if(queryParameters.getDefaultFilteringItem() == null){
                    logger.error("QueryParameters doesn't contains Default FilteringItem.", this.relationAttachKindName);
                    return 0;
                }
                String queryLinkTargetEntitiesCql = CypherBuilder.matchNodesWithQueryParameters(linkTargetConceptionKind,queryParameters,null);
                GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(linkTargetConceptionKind,workingGraphOperationExecutor);
                Object queryRes = workingGraphOperationExecutor.executeRead(getListEntityTransformer,queryLinkTargetEntitiesCql);
                if(queryRes != null){
                    List<Entity> resultEntityList = (List<Entity>)queryRes;
                    RelationshipEntity relationshipEntity = null;
                    for(Entity currentUnknownEntity: resultEntityList){
                        switch(entityRelateRole){
                            case SOURCE:
                                relationshipEntity = currentEntity.attachFromRelation(currentUnknownEntity.getEntityUID(),
                                        this.relationKindName,relationData,this.isRepeatableRelationKindAllow());
                                if(relationshipEntity != null){
                                    resultRelationCount ++;
                                }
                                break;
                            case TARGET:
                                relationshipEntity = currentEntity.attachToRelation(currentUnknownEntity.getEntityUID(),
                                        this.relationKindName,relationData,this.isRepeatableRelationKindAllow());
                                if(relationshipEntity != null){
                                    resultRelationCount ++;
                                }
                        }
                    }
                }
                return resultRelationCount;
            }else{
                return 0;
            }
        } catch (EngineServiceEntityExploreException | EngineServiceRuntimeException e) {
            e.printStackTrace();
        }
        return 0;
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }

    @Override
    public String getEntityUID() {
        return this.relationAttachKindUID;
    }

    @Override
    public GraphOperationExecutorHelper getGraphOperationExecutorHelper() {
        return graphOperationExecutorHelper;
    }
}
