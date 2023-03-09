package com.github.tgda.engine.core.feature.spi.neo4j.featureInf;

import com.github.tgda.coreRealm.realmServiceCore.analysis.query.*;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.feature.EntityRelationable;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.coreRealm.realmServiceCore.internal.neo4j.dataTransformer.*;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.payload.EntitiesAttributesRetrieveResult;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.spi.common.payloadImpl.CommonEntitiesAttributesRetrieveResultImpl;
import com.github.tgda.engine.core.term.Direction;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.RelationshipEntity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JRelationshipEntityImpl;
import org.neo4j.cypherdsl.core.*;
import org.neo4j.cypherdsl.core.renderer.Renderer;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Set;

public interface Neo4JEntityRelationable extends EntityRelationable,Neo4JKeyResourcesRetrievable {

    static Logger logger = LoggerFactory.getLogger(Neo4JEntityRelationable.class);

    enum NeighborsSearchUsage {Count , Entity}

    default public Long countAllRelations(){
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,getEntityUID(),null,true,null, CypherBuilder.CypherFunctionType.COUNT);
                GetLongFormatAggregatedReturnValueTransformer GetLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer("count");
                Long countResult = (Long)workingGraphOperationExecutor.executeRead(GetLongFormatAggregatedReturnValueTransformer,queryCql);
                return countResult;
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public List<RelationshipEntity> getAllRelations()  {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,getEntityUID(),null,true,null, null);
                GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(null,workingGraphOperationExecutor,true);
                Object relationEntityList = workingGraphOperationExecutor.executeRead(getListRelationshipEntityTransformer,queryCql);
                return relationEntityList != null ? (List<RelationshipEntity>)relationEntityList : null;
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public List<RelationshipEntity> getAllSpecifiedRelations(String relationKind, Direction direction) throws EngineServiceRuntimeException {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                QueryParameters relationshipQueryParameters = new QueryParameters();
                relationshipQueryParameters.setEntityKind(relationKind);
                relationshipQueryParameters.setResultNumber(10000000);
                boolean ignoreDirection = true;
                String sourceNodeProperty = null;
                String targetNodeProperty = null;
                if(direction != null){
                    switch (direction){
                        case FROM:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = false;
                            break;
                        case TO:
                            sourceNodeProperty = null;
                            targetNodeProperty = getEntityUID();
                            ignoreDirection = false;
                            break;
                        case TWO_WAY:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = true;
                            break;
                    }
                }
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,sourceNodeProperty,targetNodeProperty,ignoreDirection,relationshipQueryParameters, null);
                GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(null,workingGraphOperationExecutor,relationshipQueryParameters.isDistinctMode());
                Object relationEntityList = workingGraphOperationExecutor.executeRead(getListRelationshipEntityTransformer,queryCql);
                return relationEntityList != null ? (List<RelationshipEntity>)relationEntityList : null;
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public Long countAllSpecifiedRelations(String relationType, Direction direction) throws EngineServiceRuntimeException {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                QueryParameters relationshipQueryParameters = new QueryParameters();
                relationshipQueryParameters.setEntityKind(relationType);
                relationshipQueryParameters.setResultNumber(10000000);
                boolean ignoreDirection = true;
                String sourceNodeProperty = null;
                String targetNodeProperty = null;
                if(direction != null){
                    switch (direction){
                        case FROM:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = false;
                            break;
                        case TO:
                            sourceNodeProperty = null;
                            targetNodeProperty = getEntityUID();
                            ignoreDirection = false;
                            break;
                        case TWO_WAY:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = true;
                            break;
                    }
                }
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,sourceNodeProperty,targetNodeProperty,ignoreDirection,relationshipQueryParameters, CypherBuilder.CypherFunctionType.COUNT);
                GetLongFormatAggregatedReturnValueTransformer GetLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer("count");
                Long countResult = (Long)workingGraphOperationExecutor.executeRead(GetLongFormatAggregatedReturnValueTransformer,queryCql);
                return countResult;
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public Long countSpecifiedRelations(QueryParameters exploreParameters, Direction direction)  throws EngineServiceRuntimeException {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                boolean ignoreDirection = true;
                String sourceNodeProperty = null;
                String targetNodeProperty = null;
                if(direction != null){
                    switch (direction){
                        case FROM:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = false;
                            break;
                        case TO:
                            sourceNodeProperty = null;
                            targetNodeProperty = getEntityUID();
                            ignoreDirection = false;
                            break;
                        case TWO_WAY:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = true;
                            break;
                    }
                }
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,sourceNodeProperty,targetNodeProperty,ignoreDirection,exploreParameters,CypherBuilder.CypherFunctionType.COUNT);

                boolean isDistinct = false;
                if(exploreParameters != null){
                    isDistinct = exploreParameters.isDistinctMode();
                }
                GetLongFormatAggregatedReturnValueTransformer GetLongFormatAggregatedReturnValueTransformer = isDistinct ?
                        new GetLongFormatAggregatedReturnValueTransformer("count","DISTINCT"):
                        new GetLongFormatAggregatedReturnValueTransformer("count");
                Long countResult = (Long)workingGraphOperationExecutor.executeRead(GetLongFormatAggregatedReturnValueTransformer,queryCql);
                return countResult;
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public List<RelationshipEntity> getSpecifiedRelations(QueryParameters exploreParameters, Direction direction) throws EngineServiceRuntimeException {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                boolean ignoreDirection = true;
                String sourceNodeProperty = null;
                String targetNodeProperty = null;
                if(direction != null){
                    switch (direction){
                        case FROM:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = false;
                            break;
                        case TO:
                            sourceNodeProperty = null;
                            targetNodeProperty = getEntityUID();
                            ignoreDirection = false;
                            break;
                        case TWO_WAY:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = true;
                            break;
                    }
                }
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,sourceNodeProperty,targetNodeProperty,ignoreDirection,exploreParameters,null);
                GetListRelationshipEntityTransformer getListRelationshipEntityTransformer = new GetListRelationshipEntityTransformer(null,workingGraphOperationExecutor,exploreParameters.isDistinctMode());
                Object relationEntityList = workingGraphOperationExecutor.executeRead(getListRelationshipEntityTransformer,queryCql);
                return relationEntityList != null ? (List<RelationshipEntity>)relationEntityList : null;
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
                logger.error(e.getMessage());
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public RelationshipEntity attachFromRelation(String targetRelationableUID, String relationKind, Map<String,Object> initRelationProperties, boolean repeatable) throws EngineServiceRuntimeException {
        if (this.getEntityUID() != null) {
            return attachRelation(getEntityUID(),targetRelationableUID,relationKind,initRelationProperties,repeatable);
        }
        return null;
    }

    default public RelationshipEntity attachToRelation(String targetRelationableUID, String relationKind, Map<String,Object> initRelationProperties, boolean repeatable) throws EngineServiceRuntimeException {
        if (this.getEntityUID() != null) {
            return attachRelation(targetRelationableUID,getEntityUID(),relationKind,initRelationProperties,repeatable);
        }
        return null;
    }

    default public List<RelationshipEntity> attachFromRelation(List<String> targetRelationableUIDs, String relationKind, Map<String,Object> initRelationProperties, boolean repeatable) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                List<RelationshipEntity> relationshipEntityList = new ArrayList<>();
                for(String targetRelationableUID:targetRelationableUIDs){
                    RelationshipEntity currentRelationshipEntity = attachRelation(workingGraphOperationExecutor,getEntityUID(),targetRelationableUID,relationKind,initRelationProperties,repeatable);
                    relationshipEntityList.add(currentRelationshipEntity);
                }
                return relationshipEntityList;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public List<RelationshipEntity> attachToRelation(List<String> targetRelationableUIDs, String relationKind, Map<String,Object> initRelationProperties, boolean repeatable) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                List<RelationshipEntity> relationshipEntityList = new ArrayList<>();
                for(String targetRelationableUID:targetRelationableUIDs){
                    RelationshipEntity currentRelationshipEntity = attachRelation(workingGraphOperationExecutor,targetRelationableUID,getEntityUID(),relationKind,initRelationProperties,repeatable);
                    relationshipEntityList.add(currentRelationshipEntity);
                }
                return relationshipEntityList;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public boolean detachRelation(String relationEntityUID) throws EngineServiceRuntimeException {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchRelationWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(relationEntityUID),null,null);
                GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer(null,workingGraphOperationExecutor);
                Object relationEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer,queryCql);
                RelationshipEntity relationshipEntity = relationEntityRes != null ? (RelationshipEntity)relationEntityRes : null;
                if(relationshipEntity != null){
                    if(this.getEntityUID().equals(relationshipEntity.getFromEntityUID()) || this.getEntityUID().equals(relationshipEntity.getToEntityUID())){
                        String removeCql = CypherBuilder.deleteRelationWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(relationEntityUID),null,null);
                        relationEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer,removeCql);
                        relationshipEntity = relationEntityRes != null ? (RelationshipEntity)relationEntityRes : null;
                        if(relationshipEntity != null & relationshipEntity.getRelationshipEntityUID().equals(relationEntityUID)){
                            return true;
                        }else{
                            logger.error("Internal error occurs during remove relation Entity with UID {}.",  relationEntityUID);
                            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                            exception.setCauseMessage("Internal error occurs during remove relation Entity with UID "+relationEntityUID+".");
                            throw exception;
                        }
                    }else{
                        logger.error("RelationshipEntity with UID {} doesn't related to Entity with UID {}.", relationEntityUID,this.getEntityUID());
                        return false;
                    }
                }else{
                    logger.error("RelationshipEntity with UID {} doesn't exist.", relationEntityUID);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("RelationshipEntity with UID "+relationEntityUID+" doesn't exist.");
                    throw exception;
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    default public List<String> detachAllRelations(){
        if (this.getEntityUID() != null) {
            String queryCql = null;
            try {
                queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID, getEntityUID(), null, true, null, null);
                return batchDetachRelations(queryCql);
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    default public List<String> detachAllSpecifiedRelations(String relationType, Direction direction) throws EngineServiceRuntimeException {
        if (this.getEntityUID() != null) {
            try {
                QueryParameters relationshipQueryParameters = new QueryParameters();
                relationshipQueryParameters.setEntityKind(relationType);
                relationshipQueryParameters.setResultNumber(10000000);
                boolean ignoreDirection = true;
                String sourceNodeProperty = null;
                String targetNodeProperty = null;
                if(direction != null){
                    switch (direction){
                        case FROM:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = false;
                            break;
                        case TO:
                            sourceNodeProperty = null;
                            targetNodeProperty = getEntityUID();
                            ignoreDirection = false;
                            break;
                        case TWO_WAY:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = true;
                            break;
                    }
                }
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,sourceNodeProperty,targetNodeProperty,ignoreDirection,relationshipQueryParameters, null);
                return batchDetachRelations(queryCql);
                //return null;
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    default public List<String> detachSpecifiedRelations(QueryParameters exploreParameters, Direction direction) throws EngineServiceRuntimeException {
        if (this.getEntityUID() != null) {
            try {
                boolean ignoreDirection = true;
                String sourceNodeProperty = null;
                String targetNodeProperty = null;
                if(direction != null){
                    switch (direction){
                        case FROM:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = false;
                            break;
                        case TO:
                            sourceNodeProperty = null;
                            targetNodeProperty = getEntityUID();
                            ignoreDirection = false;
                            break;
                        case TWO_WAY:
                            sourceNodeProperty = getEntityUID();
                            targetNodeProperty = null;
                            ignoreDirection = true;
                            break;
                    }
                }
                String queryCql = CypherBuilder.matchRelationshipsWithQueryParameters(CypherBuilder.CypherFunctionType.ID,sourceNodeProperty,targetNodeProperty,ignoreDirection,exploreParameters,null);
                return batchDetachRelations(queryCql);
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    default Long countRelatedConceptionEntities(String targetConceptionKind, String relationKind, Direction direction, int maxJump) {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchRelatedNodesAndRelationsFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID, Long.parseLong(getEntityUID()),
                        targetConceptionKind,relationKind, direction,1,maxJump, CypherBuilder.ReturnRelationableDataType.COUNT_NODE);
                GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer("count","DISTINCT");
                Object countResultResp = workingGraphOperationExecutor.executeRead(getLongFormatAggregatedReturnValueTransformer,queryCql);
                return countResultResp != null ? (Long)countResultResp : null;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default List<Entity> getRelatedConceptionEntities(String targetConceptionKind, String relationKind, Direction direction, int maxJump) {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchRelatedNodesAndRelationsFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID, Long.parseLong(getEntityUID()),
                        targetConceptionKind,relationKind, direction,1,maxJump, CypherBuilder.ReturnRelationableDataType.NODE);
                GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(targetConceptionKind,workingGraphOperationExecutor);
                Object relationEntityList = workingGraphOperationExecutor.executeRead(getListEntityTransformer,queryCql);
                return relationEntityList != null ? (List<Entity>)relationEntityList : null;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public EntitiesAttributesRetrieveResult getAttributesOfRelatedConceptionEntities(String targetConceptionKind, List<String> attributeNames, String relationKind, Direction direction, int maxJump){
        if(attributeNames != null && attributeNames.size()>0 && this.getEntityUID() != null){
            CommonEntitiesAttributesRetrieveResultImpl commonConceptionEntitiesAttributesRetrieveResultImpl
                    = new CommonEntitiesAttributesRetrieveResultImpl();
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchRelatedNodesAndRelationsFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID, Long.parseLong(getEntityUID()),
                        targetConceptionKind,relationKind, direction,1,maxJump, CypherBuilder.ReturnRelationableDataType.NODE);
                GetListEntityValueTransformer getListEntityValueTransformer = new GetListEntityValueTransformer(attributeNames);
                Object resEntityRes = workingGraphOperationExecutor.executeRead(getListEntityValueTransformer, queryCql);
                if(resEntityRes != null){
                    List<EntityValue> resultEntitiesValues = (List<EntityValue>)resEntityRes;
                    commonConceptionEntitiesAttributesRetrieveResultImpl.addConceptionEntitiesAttributes(resultEntitiesValues);
                    commonConceptionEntitiesAttributesRetrieveResultImpl.getOperationStatistics().setResultEntitiesCount(resultEntitiesValues.size());
                }
                commonConceptionEntitiesAttributesRetrieveResultImpl.finishEntitiesRetrieving();
                return commonConceptionEntitiesAttributesRetrieveResultImpl;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default Long countRelatedConceptionEntities(String targetConceptionKind, String relationKind, Direction direction, int maxJump,
                                                AttributesParameters relationAttributesParameters, AttributesParameters conceptionAttributesParameters, boolean isDistinctMode) throws EngineServiceEntityExploreException {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                ResultEntitiesParameters resultEntitiesParameters = new ResultEntitiesParameters();
                resultEntitiesParameters.setDistinctMode(isDistinctMode);
                String queryCql = CypherBuilder.matchRelatedNodesAndRelationsFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID, Long.parseLong(getEntityUID()),
                        targetConceptionKind,relationKind, direction,1,maxJump, relationAttributesParameters,conceptionAttributesParameters,resultEntitiesParameters,CypherBuilder.ReturnRelationableDataType.COUNT_NODE);
                GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer = isDistinctMode ?
                        new GetLongFormatAggregatedReturnValueTransformer("count","DISTINCT"):
                        new GetLongFormatAggregatedReturnValueTransformer("count");
                Object countResult = workingGraphOperationExecutor.executeRead(getLongFormatAggregatedReturnValueTransformer,queryCql);
                if(countResult != null){
                    return (Long)countResult;
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default List<Entity> getRelatedConceptionEntities(String targetConceptionKind, String relationKind, Direction direction, int maxJump,
                                                      AttributesParameters relationAttributesParameters, AttributesParameters conceptionAttributesParameters, ResultEntitiesParameters resultEntitiesParameters) throws EngineServiceEntityExploreException {
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchRelatedNodesAndRelationsFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID, Long.parseLong(getEntityUID()),
                        targetConceptionKind,relationKind, direction,1,maxJump, relationAttributesParameters,conceptionAttributesParameters,resultEntitiesParameters,CypherBuilder.ReturnRelationableDataType.NODE);
                GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(targetConceptionKind,workingGraphOperationExecutor);
                Object relationEntityList = workingGraphOperationExecutor.executeRead(getListEntityTransformer,queryCql);
                return relationEntityList != null ? (List<Entity>)relationEntityList : null;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public EntitiesAttributesRetrieveResult getAttributesOfRelatedConceptionEntities(String targetConceptionKind, List<String> attributeNames,
                                                                                             String relationKind, Direction direction, int maxJump,
                                                                                             AttributesParameters relationAttributesParameters, AttributesParameters conceptionAttributesParameters,
                                                                                             ResultEntitiesParameters resultEntitiesParameters) throws EngineServiceEntityExploreException {
        if(attributeNames != null && attributeNames.size()>0 && this.getEntityUID() != null){
            CommonEntitiesAttributesRetrieveResultImpl commonConceptionEntitiesAttributesRetrieveResultImpl
                    = new CommonEntitiesAttributesRetrieveResultImpl();
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String queryCql = CypherBuilder.matchRelatedNodesAndRelationsFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID, Long.parseLong(getEntityUID()),
                        targetConceptionKind,relationKind, direction,1,maxJump, relationAttributesParameters,conceptionAttributesParameters,resultEntitiesParameters,CypherBuilder.ReturnRelationableDataType.NODE);
                GetListEntityValueTransformer getListEntityValueTransformer = new GetListEntityValueTransformer(attributeNames);
                Object resEntityRes = workingGraphOperationExecutor.executeRead(getListEntityValueTransformer, queryCql);
                if(resEntityRes != null){
                    List<EntityValue> resultEntitiesValues = (List<EntityValue>)resEntityRes;
                    commonConceptionEntitiesAttributesRetrieveResultImpl.addConceptionEntitiesAttributes(resultEntitiesValues);
                    commonConceptionEntitiesAttributesRetrieveResultImpl.getOperationStatistics().setResultEntitiesCount(resultEntitiesValues.size());
                }
                commonConceptionEntitiesAttributesRetrieveResultImpl.finishEntitiesRetrieving();
                return commonConceptionEntitiesAttributesRetrieveResultImpl;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public List<Entity> getRelatedConceptionEntities(List<RelationKindMatchLogic> relationKindMatchLogics, Direction defaultDirectionForNoneRelationKindMatch, JumpStopLogic jumpStopLogic, int jumpNumber,
                                                             AttributesParameters conceptionAttributesParameters, ResultEntitiesParameters resultEntitiesParameters) throws EngineServiceEntityExploreException {
        String cypherProcedureString = generateApocNeighborsQuery(NeighborsSearchUsage.Entity,relationKindMatchLogics,defaultDirectionForNoneRelationKindMatch,
                jumpStopLogic,jumpNumber,conceptionAttributesParameters,resultEntitiesParameters);
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(null,workingGraphOperationExecutor);
            try {
                Object queryResponse = workingGraphOperationExecutor.executeRead(getListEntityTransformer,cypherProcedureString);
                return queryResponse != null ? (List<Entity>)queryResponse : null;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public Long countRelatedConceptionEntities(List<RelationKindMatchLogic> relationKindMatchLogics, Direction defaultDirectionForNoneRelationKindMatch, JumpStopLogic jumpStopLogic, int jumpNumber,
                                                       AttributesParameters conceptionAttributesParameters) throws EngineServiceEntityExploreException {
        String cypherProcedureString = generateApocNeighborsQuery(NeighborsSearchUsage.Count,relationKindMatchLogics,defaultDirectionForNoneRelationKindMatch,
                jumpStopLogic,jumpNumber,conceptionAttributesParameters,null);
        if(this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            GetLongFormatAggregatedReturnValueTransformer getLongFormatAggregatedReturnValueTransformer = new GetLongFormatAggregatedReturnValueTransformer();
            try {
                Object queryResponse = workingGraphOperationExecutor.executeRead(getLongFormatAggregatedReturnValueTransformer,cypherProcedureString);
                return queryResponse != null ? (Long)queryResponse : null;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public boolean isDense() {
        if(this.getEntityUID() != null) {
            String cypherProcedureString = "MATCH (targetNode) WHERE id(targetNode)= "+this.getEntityUID()+"\n" +
                    "RETURN apoc.nodes.isDense(targetNode) AS "+ CypherBuilder.operationResultName+";";
            logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            GetBooleanFormatReturnValueTransformer getBooleanFormatReturnValueTransformer = new GetBooleanFormatReturnValueTransformer();
            try {
                Object queryResponse = workingGraphOperationExecutor.executeRead(getBooleanFormatReturnValueTransformer,cypherProcedureString);
                return queryResponse != null? (Boolean)queryResponse: false;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    default public boolean isAttachedWith(String targetRelationableUID, List<RelationKindMatchLogic> relationKindMatchLogics) throws EngineServiceRuntimeException {
        /*
        Example:
        https://neo4j.com/labs/apoc/4.1/overview/apoc.nodes/apoc.nodes.connected/
        */
        if(this.getEntityUID() != null && targetRelationableUID != null) {
            String relationMatchLogicFullString = CypherBuilder.generateRelationKindMatchLogicsQuery(relationKindMatchLogics,null);
            String cypherProcedureString = "MATCH (sourceNode) WHERE id(sourceNode)= "+this.getEntityUID()+"\n" +
                    "MATCH (targetNode) WHERE id(targetNode)= "+targetRelationableUID+"\n" +
                    "RETURN apoc.nodes.connected(sourceNode, targetNode, \""+relationMatchLogicFullString+"\") AS "+CypherBuilder.operationResultName+";";
            logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            GetBooleanFormatReturnValueTransformer getBooleanFormatReturnValueTransformer = new GetBooleanFormatReturnValueTransformer();
            try {
                Object queryResponse = workingGraphOperationExecutor.executeRead(getBooleanFormatReturnValueTransformer,cypherProcedureString);
                return queryResponse != null? (Boolean)queryResponse: false;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    default public List<String> listAttachedRelationKinds(){
        /*
        Example:
        https://neo4j.com/labs/apoc/4.1/overview/apoc.node/apoc.node.relationship.types/
        */
        if(this.getEntityUID() != null) {
            String cypherProcedureString = "MATCH (sourceNode) WHERE id(sourceNode)= "+this.getEntityUID()+"\n" +
                    "RETURN apoc.node.relationship.types(sourceNode) AS "+CypherBuilder.operationResultName+";";
            logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

            List<String> relationKindsList = new ArrayList<>();
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                DataTransformer resultDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        if(result.hasNext()){
                            Record nodeRecord = result.next();
                            List<Object> kindList = nodeRecord.get(CypherBuilder.operationResultName).asList();
                            if(kindList != null){
                                for(Object currentKindName:kindList){
                                    relationKindsList.add(currentKindName.toString());
                                }
                            }
                        }
                        return null;
                    }
                };
                workingGraphOperationExecutor.executeRead(resultDataTransformer,cypherProcedureString);
                return relationKindsList;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public Map<RelationKindMatchLogic,Boolean> checkRelationKindAttachExistence(List<RelationKindMatchLogic> relationKindMatchLogics){
        /*
        Example:
        https://neo4j.com/labs/apoc/4.1/overview/apoc.node/apoc.node.relationships.exist/
        */
        if(this.getEntityUID() != null) {
            String relationMatchLogicFullString = CypherBuilder.generateRelationKindMatchLogicsQuery(relationKindMatchLogics,null);
            String cypherProcedureString = "MATCH (sourceNode) WHERE id(sourceNode)= "+this.getEntityUID()+"\n" +
                    "RETURN apoc.node.relationships.exist(sourceNode, \""+relationMatchLogicFullString+"\") AS "+CypherBuilder.operationResultName+";";
            logger.debug("Generated Cypher Statement: {}", cypherProcedureString);

            Map<RelationKindMatchLogic,Boolean> checkResultMap = new HashMap<>();
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                DataTransformer resultDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        if(result.hasNext()){
                            Record nodeRecord = result.next();
                            Map resultMap = nodeRecord.get(CypherBuilder.operationResultName).asMap();
                            if(resultMap != null){
                                Iterator keyIterator = resultMap.keySet().iterator();
                                while (keyIterator.hasNext()){
                                    String currentKey = keyIterator.next().toString();
                                    String currentRelationKind = null;
                                    Direction direction = null;
                                    if(currentKey.startsWith("<")){
                                        currentRelationKind = currentKey.replaceFirst("<","");
                                        direction = Direction.TO;
                                    }else if(currentKey.endsWith(">")){
                                        currentRelationKind = currentKey.replaceFirst(">","");
                                        direction = Direction.FROM;
                                    }else{
                                        currentRelationKind = currentKey;
                                        direction = Direction.TWO_WAY;
                                    }
                                    Boolean existenceValue = Boolean.valueOf(resultMap.get(currentKey).toString());
                                    checkResultMap.put(new RelationKindMatchLogic(currentRelationKind, direction),existenceValue);
                                }
                            }
                        }
                        return null;
                    }
                };
                workingGraphOperationExecutor.executeRead(resultDataTransformer,cypherProcedureString);
                return checkResultMap;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public Map<String,Long> countAttachedRelationKinds(){
        if(this.getEntityUID() != null) {
            String queryCql ="MATCH (sourceNode) - [operationResult] - (targetNode) WHERE id(sourceNode) = "+this.getEntityUID()+" RETURN count(operationResult), type(operationResult)";
            logger.debug("Generated Cypher Statement: {}", queryCql);
            Map<String,Long> resultMap = new HashMap<>();
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                DataTransformer resultDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        while(result.hasNext()){
                            Record nodeRecord = result.next();
                            String relationKindName = nodeRecord.get("type(operationResult)").asString();
                            Long relationCount = nodeRecord.get("count(operationResult)").asLong();
                            resultMap.put(relationKindName,relationCount);
                        }
                        return null;
                    }
                };
                workingGraphOperationExecutor.executeRead(resultDataTransformer,queryCql);
                return resultMap;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default public List<String> listAttachedConceptionKinds(){
        if(this.getEntityUID() != null) {
            String queryCql = "MATCH (targetNode) -[r]- (sourceNode) WHERE id(sourceNode)= "+this.getEntityUID()+" RETURN count(targetNode),apoc.node.labels(targetNode) AS operationResult";
            logger.debug("Generated Cypher Statement: {}", queryCql);
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                List<String> conceptionKindsList = new ArrayList<>();
                DataTransformer resultDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        while(result.hasNext()){
                            Record nodeRecord = result.next();
                            if(nodeRecord.containsKey("operationResult")){
                                List<Object> conceptionKindNameList = nodeRecord.get("operationResult").asList();
                                for(Object currentConceptionKindName : conceptionKindNameList){
                                    conceptionKindsList.add(currentConceptionKindName.toString());
                                }
                            }
                        }
                        return null;
                    }
                };
                workingGraphOperationExecutor.executeRead(resultDataTransformer,queryCql);
                return conceptionKindsList;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default Map<Set<String>,Long> countAttachedConceptionKinds(){
        if(this.getEntityUID() != null) {
            String queryCql = "MATCH (targetNode) -[r]- (sourceNode) WHERE id(sourceNode)= "+this.getEntityUID()+" RETURN count(DISTINCT targetNode),apoc.node.labels(targetNode) AS operationResult";
            logger.debug("Generated Cypher Statement: {}", queryCql);
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                Map<Set<String>,Long> resultMap = new HashMap<>();
                DataTransformer resultDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        while(result.hasNext()){
                            Record nodeRecord = result.next();
                            if(nodeRecord.containsKey("operationResult") && nodeRecord.containsKey("count(DISTINCT targetNode)")){
                                List<Object> conceptionKindNameList = nodeRecord.get("operationResult").asList();
                                Long conceptionEntityCount = nodeRecord.get("count(DISTINCT targetNode)").asLong();
                                Set<String> conceptionKindNamesSet = new HashSet<>();
                                for(Object currentConceptionKindName : conceptionKindNameList){
                                    conceptionKindNamesSet.add(currentConceptionKindName.toString());
                                }
                                resultMap.put(conceptionKindNamesSet,conceptionEntityCount);
                            }
                        }
                        return null;
                    }
                };
                workingGraphOperationExecutor.executeRead(resultDataTransformer,queryCql);
                return resultMap;
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    private String generateApocNeighborsQuery(NeighborsSearchUsage neighborsSearchUsage, List<RelationKindMatchLogic> relationKindMatchLogics, Direction defaultDirectionForNoneRelationKindMatch, JumpStopLogic jumpStopLogic, int jumpNumber,
                                              AttributesParameters conceptionAttributesParameters, ResultEntitiesParameters resultEntitiesParameters) throws EngineServiceEntityExploreException {
        if(relationKindMatchLogics != null && relationKindMatchLogics.size() == 0){
            logger.error("At lease one RelationKind must be provided.");
            EngineServiceEntityExploreException exception = new EngineServiceEntityExploreException();
            exception.setCauseMessage("At lease one RelationKind must be provided.");
            throw exception;
        }
        if(relationKindMatchLogics == null & defaultDirectionForNoneRelationKindMatch == null){
            logger.error("At lease one RelationKind or global relation direction must be provided.");
            EngineServiceEntityExploreException exception = new EngineServiceEntityExploreException();
            exception.setCauseMessage("At lease one RelationKind or global relation direction must be provided.");
            throw exception;
        }
        if(relationKindMatchLogics == null & defaultDirectionForNoneRelationKindMatch != null){
            switch(defaultDirectionForNoneRelationKindMatch){
                case TWO_WAY:
                    logger.error("Only FROM or TO direction options are allowed here.");
                    EngineServiceEntityExploreException exception = new EngineServiceEntityExploreException();
                    exception.setCauseMessage("Only FROM or TO direction options are allowed here.");
                    throw exception;
            }
        }

        String relationMatchLogicFullString = CypherBuilder.generateRelationKindMatchLogicsQuery(relationKindMatchLogics,defaultDirectionForNoneRelationKindMatch);
        int distanceNumber = jumpNumber >=1 ? jumpNumber : 1;
        String apocProcedure ="";
        switch(jumpStopLogic){
            case TO:
                apocProcedure = "apoc.neighbors.tohop";
                break;
            case AT:
                apocProcedure = "apoc.neighbors.athop";
        }

        String wherePartQuery = CypherBuilder.generateAttributesParametersQueryLogic(conceptionAttributesParameters,"node");
        String resultPartQuery = generateResultEntitiesParametersFilterLogic(resultEntitiesParameters,"node");
        String returnPartQuery = "";
        switch(neighborsSearchUsage){
            case Entity: returnPartQuery = "RETURN node AS operationResult\n" +resultPartQuery;
                break;
            case Count: returnPartQuery = "RETURN count(node) AS operationResult";
        }

        String wherePartQueryString = wherePartQuery.equals("") ? "":wherePartQuery +"\n";
        String cypherProcedureString = "MATCH (n) WHERE id(n)= "+this.getEntityUID()+"\n" +
                "CALL "+apocProcedure+"(n, \""+relationMatchLogicFullString+"\","+distanceNumber+")\n" +
                "YIELD node\n" +
                wherePartQueryString +
                returnPartQuery;
        logger.debug("Generated Cypher Statement: {}", cypherProcedureString);
        return cypherProcedureString;
    }

    private void checkEntityExistence(GraphOperationExecutor workingGraphOperationExecutor,String entityUID) throws EngineServiceRuntimeException {
        String checkCql = CypherBuilder.matchNodeWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, Long.parseLong(entityUID), null, null);
        Object targetEntityExistenceRes = workingGraphOperationExecutor.executeRead(new CheckResultExistenceTransformer(),checkCql);
        if(!((Boolean)targetEntityExistenceRes).booleanValue()){
            logger.error("Entity with UID {} doesn't exist.", entityUID);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("Entity with UID "+entityUID+" doesn't exist.");
            throw exception;
        }
    }

    private RelationshipEntity attachRelation(String sourceRelationableUID, String targetRelationableUID, String relationKind, Map<String,Object> initRelationProperties, boolean repeatable) throws EngineServiceRuntimeException {
        GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
        try{
            checkEntityExistence(workingGraphOperationExecutor,sourceRelationableUID);
            checkEntityExistence(workingGraphOperationExecutor,targetRelationableUID);
            if(!repeatable){
                String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(sourceRelationableUID),Long.parseLong(targetRelationableUID), relationKind);
                GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                        (relationKind,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer, queryRelationCql);
                if(existingRelationshipEntityRes != null){
                    logger.debug("Relation of Kind {} already exist between Entity with UID {} and {}.", relationKind,sourceRelationableUID,targetRelationableUID);
                    return null;
                }
            }
            Map<String,Object> relationPropertiesMap = initRelationProperties != null ? initRelationProperties : new HashMap<>();
            CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesMap);
            String createCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(sourceRelationableUID),Long.parseLong(targetRelationableUID),
                    relationKind,relationPropertiesMap);
            GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                    (relationKind,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
            Object newRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, createCql);
            if(newRelationshipEntityRes == null){
                logger.error("Internal error occurs during create relation {} between entity with UID {} and {}.",  relationKind,sourceRelationableUID,targetRelationableUID);
                EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                exception.setCauseMessage("Internal error occurs during create relation "+relationKind+" between entity with UID "+sourceRelationableUID+" and "+targetRelationableUID+".");
                throw exception;
            }else{
                return (Neo4JRelationshipEntityImpl)newRelationshipEntityRes;
            }
        }finally {
            getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
        }
    }

    private RelationshipEntity attachRelation(GraphOperationExecutor workingGraphOperationExecutor, String sourceRelationableUID, String targetRelationableUID, String relationKind, Map<String,Object> initRelationProperties, boolean repeatable) throws EngineServiceRuntimeException {
        checkEntityExistence(workingGraphOperationExecutor,sourceRelationableUID);
        checkEntityExistence(workingGraphOperationExecutor,targetRelationableUID);
        if(!repeatable){
            String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(sourceRelationableUID),Long.parseLong(targetRelationableUID), relationKind);
            GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                    (relationKind,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
            Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer, queryRelationCql);
            if(existingRelationshipEntityRes != null){
                logger.debug("Relation of Kind {} already exist between Entity with UID {} and {}.", relationKind,sourceRelationableUID,targetRelationableUID);
                return null;
            }
        }
        Map<String,Object> relationPropertiesMap = initRelationProperties != null ? initRelationProperties : new HashMap<>();
        CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesMap);
        String createCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(sourceRelationableUID),Long.parseLong(targetRelationableUID),
                relationKind,relationPropertiesMap);
        GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                (relationKind,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
        Object newRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, createCql);
        if(newRelationshipEntityRes == null){
            logger.error("Internal error occurs during create relation {} between entity with UID {} and {}.",  relationKind,sourceRelationableUID,targetRelationableUID);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("Internal error occurs during create relation "+relationKind+" between entity with UID "+sourceRelationableUID+" and "+targetRelationableUID+".");
            throw exception;
        }else{
                return (Neo4JRelationshipEntityImpl)newRelationshipEntityRes;
        }
    }

    private List<String> batchDetachRelations(String relationQueryCql){
        if (this.getEntityUID() != null) {
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try {
                String queryCql = relationQueryCql;
                List<Object> relationEntitiesUIDList = new ArrayList<>();
                DataTransformer queryRelationshipOperationDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        if (result.hasNext()) {
                            while (result.hasNext()) {
                                Record nodeRecord = result.next();
                                if (nodeRecord != null) {
                                    Relationship resultRelationship = nodeRecord.get(CypherBuilder.operationResultName).asRelationship();
                                    Long relationEntityUID = resultRelationship.id();
                                    relationEntitiesUIDList.add(relationEntityUID);
                                }
                            }
                        }
                        return null;
                    }
                };
                workingGraphOperationExecutor.executeRead(queryRelationshipOperationDataTransformer, queryCql);
                String detachCql = CypherBuilder.deleteRelationsWithSingleFunctionValueEqual(CypherBuilder.CypherFunctionType.ID, relationEntitiesUIDList);
                DataTransformer detachRelationshipOperationDataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        List<String> resultEntitiesUIDList = new ArrayList<>();
                        if (result.hasNext()) {
                            while (result.hasNext()) {
                                Record nodeRecord = result.next();
                                if (nodeRecord != null) {
                                    Relationship resultRelationship = nodeRecord.get(CypherBuilder.operationResultName).asRelationship();
                                    Long relationEntityUID = resultRelationship.id();
                                    resultEntitiesUIDList.add("" + relationEntityUID);
                                }
                            }
                        }
                        return resultEntitiesUIDList;
                    }
                };
                Object detachResult = workingGraphOperationExecutor.executeWrite(detachRelationshipOperationDataTransformer, detachCql);
                if (detachResult != null) {
                    return (List<String>) detachResult;
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    private String generateResultEntitiesParametersFilterLogic(ResultEntitiesParameters resultEntitiesParameters,String filterNodeName) throws EngineServiceEntityExploreException {
        if(resultEntitiesParameters != null){
            Node resultNodes = Cypher.anyNode().named(filterNodeName);

            int startPage = resultEntitiesParameters.getStartPage();
            int endPage = resultEntitiesParameters.getEndPage();
            int pageSize = resultEntitiesParameters.getPageSize();
            int resultNumber = resultEntitiesParameters.getResultNumber();
            int defaultReturnRecordNumber = 10000;
            int skipRecordNumber = 0;
            int limitRecordNumber = 0;
            List<SortingItem> sortingItemList = resultEntitiesParameters.getSortingItems();
            SortItem[] sortItemArray = null;

            if (sortingItemList!= null && (sortingItemList.size() > 0)){
                sortItemArray = new SortItem[sortingItemList.size()];
                for (int i = 0; i < sortingItemList.size(); i++) {
                    SortingItem currentSortingItem = sortingItemList.get(i);
                    String attributeName = currentSortingItem.getAttributeName();
                    QueryParameters.SortingLogic sortingLogic = currentSortingItem.getSortingLogic();
                    switch (sortingLogic) {
                        case ASC:
                            sortItemArray[i] = Cypher.sort(resultNodes.property(attributeName)).ascending();
                            break;
                        case DESC:
                            sortItemArray[i] = Cypher.sort(resultNodes.property(attributeName)).descending();
                    }
                }
            }

            if (startPage != 0) {
                if (startPage < 0) {
                    String exceptionMessage = "start page must great then zero";
                    EngineServiceEntityExploreException coreRealmServiceEntityExploreException = new EngineServiceEntityExploreException();
                    coreRealmServiceEntityExploreException.setCauseMessage(exceptionMessage);
                    throw coreRealmServiceEntityExploreException;
                }
                if (pageSize < 0) {
                    String exceptionMessage = "page size must great then zero";
                    EngineServiceEntityExploreException coreRealmServiceEntityExploreException = new EngineServiceEntityExploreException();
                    coreRealmServiceEntityExploreException.setCauseMessage(exceptionMessage);
                    throw coreRealmServiceEntityExploreException;
                }

                int runtimePageSize = pageSize != 0 ? pageSize : 50;
                int runtimeStartPage = startPage - 1;

                if (endPage != 0) {
                    //get data from start page to end page, each page has runtimePageSize number of record
                    if (endPage < 0 || endPage <= startPage) {
                        String exceptionMessage = "end page must great than start page";
                        EngineServiceEntityExploreException coreRealmServiceEntityExploreException = new EngineServiceEntityExploreException();
                        coreRealmServiceEntityExploreException.setCauseMessage(exceptionMessage);
                        throw coreRealmServiceEntityExploreException;
                    }
                    int runtimeEndPage = endPage - 1;

                    skipRecordNumber = runtimePageSize * runtimeStartPage;
                    limitRecordNumber = (runtimeEndPage - runtimeStartPage) * runtimePageSize;
                } else {
                    //filter the data before the start page
                    limitRecordNumber = runtimePageSize * runtimeStartPage;
                }
            } else {
                //if there is no page parameters,use resultNumber to control result information number
                if (resultNumber != 0) {
                    if (resultNumber < 0) {
                        String exceptionMessage = "result number must great then zero";
                        EngineServiceEntityExploreException coreRealmServiceEntityExploreException = new EngineServiceEntityExploreException();
                        coreRealmServiceEntityExploreException.setCauseMessage(exceptionMessage);
                        throw coreRealmServiceEntityExploreException;
                    }
                    limitRecordNumber = resultNumber;
                }
            }
            if (limitRecordNumber == 0) {
                limitRecordNumber = defaultReturnRecordNumber;
            }

            StatementBuilder.OngoingReadingWithoutWhere ongoingReadingWithoutWhere = Cypher.match(resultNodes);
            StatementBuilder.OngoingReadingAndReturn ongoingReadingAndReturn;
            ongoingReadingAndReturn = ongoingReadingWithoutWhere.returning(resultNodes);
            Statement statement;

            if (skipRecordNumber != 0){
                if(sortItemArray != null){
                    statement = ongoingReadingAndReturn.orderBy(sortItemArray).skip(skipRecordNumber).limit(limitRecordNumber).build();
                }else{
                    statement = ongoingReadingAndReturn.skip(skipRecordNumber).limit(limitRecordNumber).build();
                }
            }else{
                if(sortItemArray != null){
                    statement = ongoingReadingAndReturn.orderBy(sortItemArray).limit(limitRecordNumber).build();
                }else{
                    statement = ongoingReadingAndReturn.limit(limitRecordNumber).build();
                }
            }

            Renderer cypherRenderer = Renderer.getDefaultRenderer();
            String rel = cypherRenderer.render(statement);

            String tempStringToReplace = "MATCH ("+filterNodeName+") RETURN "+filterNodeName+" ";
            return rel.replace(tempStringToReplace,"");
        }
        return "";
    }
}
