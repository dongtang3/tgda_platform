package com.github.tgda.engine.core.analysis.query;

import com.github.tgda.engine.core.term.Direction;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class TravelParameters {

    public enum TraversalMethod {BFS, DFS}

    private List<RelationKindMatchLogic> relationKindMatchLogics;
    private Direction defaultDirectionForNoneRelationKindMatch;
    private List<ConceptionKindMatchLogic> conceptionKindMatchLogics;
    private int minJump = -1;
    private int maxJump = -1;
    private int resultNumber = -1;
    private LinkedList<List<RelationKindMatchLogic>> relationKindFlowMatchLogics;
    private LinkedList<List<ConceptionKindMatchLogic>> conceptionKindFlowMatchLogics;
    private TraversalMethod traversalMethod = TraversalMethod.BFS;
    private List<String> mustHaveEntityUIDs;
    private List<String> notAllowEntityUIDs;
    private List<String> endWithEntityUIDs;
    private List<String> terminateAtEntityUIDs;
    private boolean matchStartEntity = false;
    private LinkedList<List<? extends EntityKindMatchLogic>> entityPathFlowMatchLogics;
    private boolean matchStartEntityForFlow = true;
    private PathEntityFilterParameters relationPathEntityFilterParameters;
    private PathEntityFilterParameters conceptionPathEntityFilterParameters;

    public TravelParameters(){
        this.relationKindMatchLogics = new ArrayList<>();
        this.conceptionKindMatchLogics = new ArrayList<>();
        this.relationKindFlowMatchLogics = new LinkedList<>();
        this.conceptionKindFlowMatchLogics = new LinkedList<>();
        this.entityPathFlowMatchLogics = new LinkedList<>();
    }

    public void addRelationKindMatchLogic(RelationKindMatchLogic relationKindMatchLogic){
        this.relationKindMatchLogics.add(relationKindMatchLogic);
    }

    public List<RelationKindMatchLogic> getRelationKindMatchLogics() {
        return relationKindMatchLogics;
    }

    public Direction getDefaultDirectionForNoneRelationKindMatch() {
        return defaultDirectionForNoneRelationKindMatch;
    }

    public void setDefaultDirectionForNoneRelationKindMatch(Direction defaultDirectionForNoneRelationKindMatch) {
        this.defaultDirectionForNoneRelationKindMatch = defaultDirectionForNoneRelationKindMatch;
    }

    public void addConceptionKindMatchLogic(ConceptionKindMatchLogic conceptionKindMatchLogic){
        this.conceptionKindMatchLogics.add(conceptionKindMatchLogic);
    }

    public List<ConceptionKindMatchLogic> getConceptionKindMatchLogics() {
        return conceptionKindMatchLogics;
    }

    public int getMinJump() {
        return minJump;
    }

    public void setMinJump(int minJump) {
        this.minJump = minJump;
    }

    public int getMaxJump() {
        return maxJump;
    }

    public void setMaxJump(int maxJump) {
        this.maxJump = maxJump;
    }

    public int getResultNumber() {
        return resultNumber;
    }

    public void setResultNumber(int resultNumber) {
        this.resultNumber = resultNumber;
    }

    public void addRelationKindFlowMatchLogic(List<RelationKindMatchLogic> relationKindFlowMatchLogic){
        this.relationKindFlowMatchLogics.add(relationKindFlowMatchLogic);
    }

    public LinkedList<List<RelationKindMatchLogic>> getRelationKindFlowMatchLogics() {
        return relationKindFlowMatchLogics;
    }

    public void addConceptionKindFlowMatchLogic(List<ConceptionKindMatchLogic> conceptionKindFlowMatchLogic){
        this.conceptionKindFlowMatchLogics.add(conceptionKindFlowMatchLogic);
    }

    public LinkedList<List<ConceptionKindMatchLogic>> getConceptionKindFlowMatchLogics() {
        return conceptionKindFlowMatchLogics;
    }

    public TraversalMethod getTraversalMethod() {
        return traversalMethod;
    }

    public void setTraversalMethod(TraversalMethod traversalMethod) {
        this.traversalMethod = traversalMethod;
    }

    public List<String> getMustHaveEntityUIDs() {
        return mustHaveEntityUIDs;
    }

    public void setMustHaveEntityUIDs(List<String> mustHaveEntityUIDs) {
        this.mustHaveEntityUIDs = mustHaveEntityUIDs;
    }

    public List<String> getNotAllowEntityUIDs() {
        return notAllowEntityUIDs;
    }

    public void setNotAllowEntityUIDs(List<String> notAllowEntityUIDs) {
        this.notAllowEntityUIDs = notAllowEntityUIDs;
    }

    public List<String> getEndWithEntityUIDs() {
        return endWithEntityUIDs;
    }

    public void setEndWithEntityUIDs(List<String> endWithEntityUIDs) {
        this.endWithEntityUIDs = endWithEntityUIDs;
    }

    public List<String> getTerminateAtEntityUIDs() {
        return terminateAtEntityUIDs;
    }

    public void setTerminateAtEntityUIDs(List<String> terminateAtEntityUIDs) {
        this.terminateAtEntityUIDs = terminateAtEntityUIDs;
    }

    public boolean isMatchStartEntity() {
        return matchStartEntity;
    }

    public void setMatchStartEntity(boolean matchStartEntity) {
        this.matchStartEntity = matchStartEntity;
    }

    public void addEntityPathFlowMatchLogic(List<ConceptionKindMatchLogic> entityPathFlowMatchLogic){
        this.entityPathFlowMatchLogics.add(entityPathFlowMatchLogic);
    }

    public void addRelationshipEntityPathFlowMatchLogic(List<RelationKindMatchLogic> entityPathFlowMatchLogic){
        this.entityPathFlowMatchLogics.add(entityPathFlowMatchLogic);
    }

    public LinkedList<List<? extends EntityKindMatchLogic>> getEntityPathFlowMatchLogics() {
        return entityPathFlowMatchLogics;
    }

    public boolean isMatchStartEntityForFlow() {
        return matchStartEntityForFlow;
    }

    public void setMatchStartEntityForFlow(boolean matchStartEntityForFlow) {
        this.matchStartEntityForFlow = matchStartEntityForFlow;
    }

    public PathEntityFilterParameters getRelationPathEntityFilterParameters() {
        return relationPathEntityFilterParameters;
    }

    public void setRelationPathEntityFilterParameters(PathEntityFilterParameters relationPathEntityFilterParameters) {
        this.relationPathEntityFilterParameters = relationPathEntityFilterParameters;
    }

    public PathEntityFilterParameters getConceptionPathEntityFilterParameters() {
        return conceptionPathEntityFilterParameters;
    }

    public void setConceptionPathEntityFilterParameters(PathEntityFilterParameters conceptionPathEntityFilterParameters) {
        this.conceptionPathEntityFilterParameters = conceptionPathEntityFilterParameters;
    }
}
