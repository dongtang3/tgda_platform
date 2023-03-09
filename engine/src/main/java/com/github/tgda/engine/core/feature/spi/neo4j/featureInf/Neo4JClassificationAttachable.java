package com.github.tgda.engine.core.feature.spi.neo4j.featureInf;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.feature.ClassificationAttachable;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetListClassificationTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleClassificationTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleRelationshipEntityTransformer;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.payload.RelationshipAttachInfo;
import com.github.tgda.engine.core.term.Classification;
import com.github.tgda.engine.core.term.Direction;
import com.github.tgda.engine.core.term.RelationshipEntity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JClassificationImpl;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JRelationshipEntityImpl;
import com.github.tgda.engine.core.util.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public interface Neo4JClassificationAttachable extends ClassificationAttachable,Neo4JKeyResourcesRetrievable {

    static Logger logger = LoggerFactory.getLogger(Neo4JClassificationAttachable.class);

    default RelationshipEntity attachClassification(RelationshipAttachInfo relationshipAttachInfo, String classificationName) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null){
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                Classification targetClassification = getClassificationByName(workingGraphOperationExecutor,classificationName);
                Map<String, Object> relationData = relationshipAttachInfo.getRelationData();
                Direction direction = relationshipAttachInfo.getDirection();
                String relationKind = relationshipAttachInfo.getRelationKind();
                if(targetClassification != null){
                    Neo4JClassificationImpl neo4JClassificationImpl = (Neo4JClassificationImpl)targetClassification;
                    String sourceRelationableUID = null;
                    String targetRelationableUID = null;
                    switch(direction){
                        case FROM:
                            sourceRelationableUID = this.getEntityUID();
                            targetRelationableUID = neo4JClassificationImpl.getClassificationUID();
                            break;
                        case TO:
                            sourceRelationableUID = neo4JClassificationImpl.getClassificationUID();
                            targetRelationableUID = this.getEntityUID();
                            break;
                    }
                    String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(sourceRelationableUID),Long.parseLong(targetRelationableUID), relationKind);

                    GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                            (relationKind,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                    Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer, queryRelationCql);
                    if(existingRelationshipEntityRes != null){
                        logger.debug("Relation of Kind {} already exist between Entity with UID {} and {}.", relationKind,sourceRelationableUID,targetRelationableUID);
                        return null;
                    }else{
                        Map<String,Object> relationPropertiesMap = relationData != null ? relationData : new HashMap<>();
                        CommonOperationUtil.generateEntityMetaAttributes(relationPropertiesMap);
                        String createCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(sourceRelationableUID),Long.parseLong(targetRelationableUID),
                                relationKind,relationPropertiesMap);
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
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    default boolean detachClassification(String classificationName, String relationKindName, Direction direction) throws EngineServiceRuntimeException {
        if(this.getEntityUID() != null){
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                Classification targetClassification = getClassificationByName(workingGraphOperationExecutor,classificationName);
                if(targetClassification != null){
                    Neo4JClassificationImpl neo4JClassificationImpl = (Neo4JClassificationImpl)targetClassification;
                    String sourceRelationableUID = null;
                    String targetRelationableUID = null;
                    switch(direction){
                        case TWO_WAY:
                            logger.error("TWO_WAY Direction is not allowed in this operation.");
                            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                            exception.setCauseMessage("TWO_WAY Direction is not allowed in this operation.");
                            throw exception;
                        case FROM:
                            sourceRelationableUID = this.getEntityUID();
                            targetRelationableUID = neo4JClassificationImpl.getClassificationUID();
                            break;
                        case TO:
                            sourceRelationableUID = neo4JClassificationImpl.getClassificationUID();
                            targetRelationableUID = this.getEntityUID();
                            break;
                    }
                    String queryRelationCql = CypherBuilder.matchRelationshipsByBothNodesId(Long.parseLong(sourceRelationableUID),Long.parseLong(targetRelationableUID), relationKindName);

                    GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                            (relationKindName,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                    Object existingRelationshipEntityRes = workingGraphOperationExecutor.executeRead(getSingleRelationshipEntityTransformer, queryRelationCql);
                    if(existingRelationshipEntityRes == null){
                        logger.debug("Relation of Kind {} does not exist between Entity with UID {} and {}.", relationKindName,sourceRelationableUID,targetRelationableUID);
                        return false;
                    }else{
                        RelationshipEntity relationshipEntity = (RelationshipEntity)existingRelationshipEntityRes;
                        String deleteCql = CypherBuilder.deleteRelationWithSingleFunctionValueEqual(
                                CypherBuilder.CypherFunctionType.ID,Long.valueOf(relationshipEntity.getRelationshipEntityUID()),null,null);
                        getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                                (relationshipEntity.getRelationTypeName(),workingGraphOperationExecutor);
                        Object deleteRelationshipEntityRes = workingGraphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, deleteCql);
                        if(deleteRelationshipEntityRes == null){
                            logger.error("Internal error occurs during detach classification {} from entity {}.",  classificationName,this.getEntityUID());
                            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                            exception.setCauseMessage("Internal error occurs during detach classification "+classificationName+" from entity "+this.getEntityUID()+".");
                            throw exception;
                        }else{
                            return true;
                        }
                    }
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return false;
    }

    default List<Classification> getAttachedClassifications(String relationKindName, Direction direction){
        if(this.getEntityUID() != null){
            GraphOperationExecutor workingGraphOperationExecutor = getGraphOperationExecutorHelper().getWorkingGraphOperationExecutor();
            try{
                Direction realDirection = Direction.TWO_WAY;
                switch(direction){
                    case FROM: realDirection = Direction.TO;break;
                    case TO:
                        realDirection = Direction.FROM;break;
                }
                String queryCql = CypherBuilder.matchRelatedNodesAndRelationsFromSpecialStartNodes(CypherBuilder.CypherFunctionType.ID, Long.parseLong(this.getEntityUID()),
                        Constant.ClassificationClass,relationKindName, realDirection,0,0, CypherBuilder.ReturnRelationableDataType.BOTH);
                GetListClassificationTransformer getListClassificationTransformer = new GetListClassificationTransformer(null,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
                Object queryClassificationRes = workingGraphOperationExecutor.executeRead(getListClassificationTransformer,queryCql);

                if(queryClassificationRes != null){
                    return (List<Classification>)queryClassificationRes;
                }
            }finally {
                getGraphOperationExecutorHelper().closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    private Classification getClassificationByName(GraphOperationExecutor workingGraphOperationExecutor,String classificationName) throws EngineServiceRuntimeException {
        String checkCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.ClassificationClass, Constant._NameProperty,classificationName,1);
        GetSingleClassificationTransformer getSingleClassificationTransformer = new GetSingleClassificationTransformer(null,getGraphOperationExecutorHelper().getGlobalGraphOperationExecutor());
        Object existClassificationRes = workingGraphOperationExecutor.executeRead(getSingleClassificationTransformer,checkCql);
        if(existClassificationRes != null){
            return (Classification)existClassificationRes;
        }else{
            logger.error("Classification with Name {} doesn't exist.", classificationName);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("Classification with Name "+classificationName+" doesn't exist.");
            throw exception;
        }
    }
}
