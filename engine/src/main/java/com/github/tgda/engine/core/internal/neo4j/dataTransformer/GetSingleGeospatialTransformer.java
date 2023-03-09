package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.Geospatial;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JGeospatialImpl;
import com.github.tgda.engine.core.util.Constant;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.util.List;

public class GetSingleGeospatialTransformer implements DataTransformer<Geospatial>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String currentCoreRealmName;

    public GetSingleGeospatialTransformer(String currentCoreRealmName, GraphOperationExecutor workingGraphOperationExecutor){
        this.currentCoreRealmName= currentCoreRealmName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public Geospatial transformResult(Result result) {
        if(result.hasNext()){
            Record nodeRecord = result.next();
            if(nodeRecord != null){
                Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                List<String> allLabelNames = Lists.newArrayList(resultNode.labels());
                boolean isMatchedKind = true;
                if(allLabelNames.size()>0){
                    isMatchedKind = allLabelNames.contains(Constant.GeospatialClass);
                }
                if(isMatchedKind){
                    String coreRealmName = this.currentCoreRealmName;
                    String geospatialRegionName = resultNode.get(Constant._NameProperty).asString();
                    long nodeUID = resultNode.id();
                    String geospatialRegionNameUID = ""+nodeUID;
                    Neo4JGeospatialImpl neo4JGeospatialImpl = new Neo4JGeospatialImpl(coreRealmName,geospatialRegionName,geospatialRegionNameUID);
                    neo4JGeospatialImpl.setGlobalGraphOperationExecutor(this.workingGraphOperationExecutor);
                    return neo4JGeospatialImpl;
                }
            }
        }
        return null;
    }
}
