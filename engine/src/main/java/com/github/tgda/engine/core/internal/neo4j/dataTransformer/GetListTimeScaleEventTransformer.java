package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.google.common.collect.Lists;

import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.TimeFlow;
import com.github.tgda.engine.core.term.TimeScaleEvent;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JTimeScaleEventImpl;
import com.github.tgda.engine.core.util.Constant;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class GetListTimeScaleEventTransformer  implements DataTransformer<List<TimeScaleEvent>>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String timeFlowName;

    public GetListTimeScaleEventTransformer(String timeFlowName, GraphOperationExecutor workingGraphOperationExecutor){
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
        this.timeFlowName = timeFlowName;
    }

    @Override
    public List<TimeScaleEvent> transformResult(Result result) {
        List<TimeScaleEvent> timeScaleEventList = new ArrayList<>();
        while(result.hasNext()){
            Record nodeRecord = result.next();
            Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
            List<String> allConceptionKindNames = Lists.newArrayList(resultNode.labels());
            boolean isMatchedConceptionKind = false;
            if(allConceptionKindNames.size()>0){
                isMatchedConceptionKind = allConceptionKindNames.contains(Constant.TimeScaleEventClass);
            }
            if(isMatchedConceptionKind){
                long nodeUID = resultNode.id();
                String timeScaleEventUID = ""+nodeUID;
                String eventComment = resultNode.get(Constant._TimeScaleEventComment).asString();
                String timeScaleGrade = resultNode.get(Constant._TimeScaleEventScaleGrade).asString();
                LocalDateTime referTime = resultNode.get(Constant._TimeScaleEventReferTime).asLocalDateTime();
                Neo4JTimeScaleEventImpl neo4JTimeScaleEventImpl = new Neo4JTimeScaleEventImpl(timeFlowName,eventComment,referTime,getTimeScaleGrade(timeScaleGrade),timeScaleEventUID);
                neo4JTimeScaleEventImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                timeScaleEventList.add(neo4JTimeScaleEventImpl);
            }
        }
        return timeScaleEventList;
    }

    private TimeFlow.TimeScaleGrade getTimeScaleGrade(String timeScaleGradeValue){
        if(timeScaleGradeValue.equals("YEAR")){
            return TimeFlow.TimeScaleGrade.YEAR;
        }else if(timeScaleGradeValue.equals("MONTH")){
            return TimeFlow.TimeScaleGrade.MONTH;
        }else if(timeScaleGradeValue.equals("DAY")){
            return TimeFlow.TimeScaleGrade.DAY;
        }else if(timeScaleGradeValue.equals("HOUR")){
            return TimeFlow.TimeScaleGrade.HOUR;
        }else if(timeScaleGradeValue.equals("MINUTE")){
            return TimeFlow.TimeScaleGrade.MINUTE;
        }else if(timeScaleGradeValue.equals("SECOND")){
            return TimeFlow.TimeScaleGrade.SECOND;
        }
        return null;
    }
}
