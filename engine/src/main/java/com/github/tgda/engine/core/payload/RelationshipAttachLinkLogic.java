package com.github.tgda.engine.core.payload;

import com.github.tgda.engine.core.term.RelationshipAttach;

public class RelationshipAttachLinkLogic {

    private RelationshipAttach.LinkLogicType linkLogicType;
    private RelationshipAttach.LinkLogicCondition linkLogicCondition;
    private String sourceEntityLinkAttributeName;
    private String targetEntitiesLinkAttributeName;
    private String relationAttachLinkLogicUID;

    public RelationshipAttachLinkLogic(){}

    public RelationshipAttachLinkLogic(RelationshipAttach.LinkLogicType linkLogicType, RelationshipAttach.LinkLogicCondition linkLogicCondition,
                                       String sourceEntityLinkAttributeName, String targetEntitiesLinkAttributeName){
        this.linkLogicType = linkLogicType;
        this.linkLogicCondition = linkLogicCondition;
        this.sourceEntityLinkAttributeName = sourceEntityLinkAttributeName;
        this.targetEntitiesLinkAttributeName = targetEntitiesLinkAttributeName;
    }

    public RelationshipAttachLinkLogic(RelationshipAttach.LinkLogicType linkLogicType, RelationshipAttach.LinkLogicCondition linkLogicCondition,
                                       String sourceEntityLinkAttributeName, String targetEntitiesLinkAttributeName, String relationAttachLinkLogicUID){
        this.linkLogicType = linkLogicType;
        this.linkLogicCondition = linkLogicCondition;
        this.sourceEntityLinkAttributeName = sourceEntityLinkAttributeName;
        this.targetEntitiesLinkAttributeName = targetEntitiesLinkAttributeName;
        this.relationAttachLinkLogicUID = relationAttachLinkLogicUID;
    }

    public RelationshipAttach.LinkLogicType getLinkLogicType() {
        return linkLogicType;
    }

    public void setLinkLogicType(RelationshipAttach.LinkLogicType linkLogicType) {
        this.linkLogicType = linkLogicType;
    }

    public RelationshipAttach.LinkLogicCondition getLinkLogicCondition() {
        return linkLogicCondition;
    }

    public void setLinkLogicCondition(RelationshipAttach.LinkLogicCondition linkLogicCondition) {
        this.linkLogicCondition = linkLogicCondition;
    }

    public String getSourceEntityLinkAttributeName() {
        return sourceEntityLinkAttributeName;
    }

    public void setSourceEntityLinkAttributeName(String sourceEntityLinkAttributeName) {
        this.sourceEntityLinkAttributeName = sourceEntityLinkAttributeName;
    }

    public String getTargetEntitiesLinkAttributeName() {
        return targetEntitiesLinkAttributeName;
    }

    public void setTargetEntitiesLinkAttributeName(String targetEntitiesLinkAttributeName) {
        this.targetEntitiesLinkAttributeName = targetEntitiesLinkAttributeName;
    }

    public String getRelationAttachLinkLogicUID() {
        return relationAttachLinkLogicUID;
    }

    public void setRelationAttachLinkLogicUID(String relationAttachLinkLogicUID) {
        this.relationAttachLinkLogicUID = relationAttachLinkLogicUID;
    }
}
