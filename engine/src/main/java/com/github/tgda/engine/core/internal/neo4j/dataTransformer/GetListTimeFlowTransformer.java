package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.TimeFlow;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JTimeFlowImpl;
import com.github.tgda.engine.core.util.Constant;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.List;

public class GetListTimeFlowTransformer implements DataTransformer<List<TimeFlow>>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String currentCoreRealmName;

    public GetListTimeFlowTransformer(String currentCoreRealmName,GraphOperationExecutor workingGraphOperationExecutor){
        this.currentCoreRealmName= currentCoreRealmName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public List<TimeFlow> transformResult(Result result) {
        List<TimeFlow> timeFlowsList = new ArrayList<>();
        if(result.hasNext()){
            while(result.hasNext()){
                Record nodeRecord = result.next();
                if(nodeRecord != null) {
                    Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                    List<String> allLabelNames = Lists.newArrayList(resultNode.labels());
                    boolean isMatchedKind = true;
                    if (allLabelNames.size() > 0) {
                        isMatchedKind = allLabelNames.contains(Constant.TimeFlowClass);
                    }
                    if (isMatchedKind) {
                        String coreRealmName = this.currentCoreRealmName;
                        String timeFlowName = resultNode.get(Constant._NameProperty).asString();
                        long nodeUID = resultNode.id();
                        String timeFlowUID = "" + nodeUID;
                        Neo4JTimeFlowImpl neo4JTimeFlowImpl = new Neo4JTimeFlowImpl(coreRealmName, timeFlowName, timeFlowUID);
                        neo4JTimeFlowImpl.setGlobalGraphOperationExecutor(this.workingGraphOperationExecutor);
                        timeFlowsList.add(neo4JTimeFlowImpl);
                    }
                }
            }
        }
        return timeFlowsList;
    }
}
