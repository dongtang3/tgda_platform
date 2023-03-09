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

import java.util.ArrayList;
import java.util.List;

public class GetListGeospatialTransformer implements DataTransformer<List<Geospatial>>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String currentCoreRealmName;

    public GetListGeospatialTransformer(String currentCoreRealmName, GraphOperationExecutor workingGraphOperationExecutor){
        this.currentCoreRealmName= currentCoreRealmName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public List<Geospatial> transformResult(Result result) {
        List<Geospatial> geospatialRegionsList = new ArrayList<>();
        if(result.hasNext()){
            while(result.hasNext()){
                Record nodeRecord = result.next();
                if(nodeRecord != null) {
                    Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                    List<String> allLabelNames = Lists.newArrayList(resultNode.labels());
                    boolean isMatchedKind = true;
                    if (allLabelNames.size() > 0) {
                        isMatchedKind = allLabelNames.contains(Constant.GeospatialClass);
                    }
                    if (isMatchedKind) {
                        String coreRealmName = this.currentCoreRealmName;
                        String geospatialRegionName = resultNode.get(Constant._NameProperty).asString();
                        long nodeUID = resultNode.id();
                        String geospatialRegionUID = "" + nodeUID;
                        Neo4JGeospatialImpl neo4JGeospatialImpl = new Neo4JGeospatialImpl(coreRealmName, geospatialRegionName, geospatialRegionUID);
                        neo4JGeospatialImpl.setGlobalGraphOperationExecutor(this.workingGraphOperationExecutor);
                        geospatialRegionsList.add(neo4JGeospatialImpl);
                    }
                }
            }
        }
        return geospatialRegionsList;
    }
}
