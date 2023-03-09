package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.payload.RelationshipAttachLinkLogic;
import com.github.tgda.engine.core.term.RelationshipAttach;
import com.github.tgda.engine.core.util.Constant;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import static com.github.tgda.engine.core.term.RelationshipAttach.LinkLogicCondition.*;
import static com.github.tgda.engine.core.term.RelationshipAttach.LinkLogicType.*;

public class GetSingleRelationshipAttachLinkLogicTransformer implements DataTransformer<RelationshipAttachLinkLogic>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String currentCoreRealmName;

    public GetSingleRelationshipAttachLinkLogicTransformer(String currentCoreRealmName, GraphOperationExecutor workingGraphOperationExecutor){
        this.currentCoreRealmName= currentCoreRealmName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public RelationshipAttachLinkLogic transformResult(Result result) {
        if(result.hasNext()){
            Record nodeRecord = result.next();
            if(nodeRecord != null){
                Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                long nodeUID = resultNode.id();
                String attachLinkLogicType = resultNode.get(Constant._attachLinkLogicType).asString();
                String attachLinkLogicCondition = resultNode.get(Constant._attachLinkLogicCondition).asString();
                String attachLinkLogicSourceAttribute = resultNode.get(Constant._attachLinkLogicSourceAttribute).asString();
                String attachLinkLogicTargetAttribute = resultNode.get(Constant._attachLinkLogicTargetAttribute).asString();

                String relationAttachLinkLogicUID = ""+nodeUID;
                RelationshipAttachLinkLogic relationshipAttachLinkLogic =
                        new RelationshipAttachLinkLogic(getLinkLogicType(attachLinkLogicType),getLinkLogicCondition(attachLinkLogicCondition),
                                attachLinkLogicSourceAttribute,attachLinkLogicTargetAttribute,relationAttachLinkLogicUID);
                return relationshipAttachLinkLogic;
            }
        }
        return null;
    }

    private static RelationshipAttach.LinkLogicType getLinkLogicType(String linkLogicTypeStr){
        if(linkLogicTypeStr.equals("DEFAULT")){
            return DEFAULT;
        }
        if(linkLogicTypeStr.equals("AND")){
            return AND;
        }
        if(linkLogicTypeStr.equals("OR")){
            return OR;
        }
        return null;
    }

    private static RelationshipAttach.LinkLogicCondition getLinkLogicCondition(String linkLogicConditionStr){
        if(linkLogicConditionStr.equals("Equal")){
            return Equal;
        }
        if(linkLogicConditionStr.equals("GreaterThanEqual")){
            return GreaterThanEqual;
        }
        if(linkLogicConditionStr.equals("GreaterThan")){
            return GreaterThan;
        }
        if(linkLogicConditionStr.equals("LessThanEqual")){
            return LessThanEqual;
        }
        if(linkLogicConditionStr.equals("LessThan")){
            return LessThan;
        }
        if(linkLogicConditionStr.equals("NotEqual")){
            return NotEqual;
        }
        if(linkLogicConditionStr.equals("RegularMatch")){
            return RegularMatch;
        }
        if(linkLogicConditionStr.equals("BeginWithSimilar")){
            return BeginWithSimilar;
        }
        if(linkLogicConditionStr.equals("EndWithSimilar")){
            return EndWithSimilar;
        }
        if(linkLogicConditionStr.equals("ContainSimilar")){
            return ContainSimilar;
        }
        return null;
    }
}
