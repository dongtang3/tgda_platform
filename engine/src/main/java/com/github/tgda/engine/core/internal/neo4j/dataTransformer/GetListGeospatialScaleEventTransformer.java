package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.Geospatial;
import com.github.tgda.engine.core.term.GeospatialScaleEvent;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JGeospatialScaleEventImpl;
import com.github.tgda.engine.core.util.Constant;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.List;

public class GetListGeospatialScaleEventTransformer implements DataTransformer<List<GeospatialScaleEvent>>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String geospatialRegionName;

    public GetListGeospatialScaleEventTransformer(String geospatialRegionName, GraphOperationExecutor workingGraphOperationExecutor){
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
        this.geospatialRegionName = geospatialRegionName;
    }

    @Override
    public List<GeospatialScaleEvent> transformResult(Result result) {
        List<GeospatialScaleEvent> geospatialScaleEventList = new ArrayList<>();
        while(result.hasNext()){
            Record nodeRecord = result.next();
            Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
            List<String> allConceptionKindNames = Lists.newArrayList(resultNode.labels());
            boolean isMatchedConceptionKind = false;
            if(allConceptionKindNames.size()>0){
                isMatchedConceptionKind = allConceptionKindNames.contains(Constant.GeospatialScaleEventClass);
            }
            if(isMatchedConceptionKind){
                long nodeUID = resultNode.id();
                String geospatialScaleEventUID = ""+nodeUID;
                String eventComment = resultNode.get(Constant._GeospatialScaleEventComment).asString();
                String geospatialScaleGrade = resultNode.get(Constant._GeospatialScaleEventScaleGrade).asString();
                String referLocation = resultNode.get(Constant._GeospatialScaleEventReferLocation).asString();
                String geospatialRegion = resultNode.get(Constant._GeospatialScaleEventGeospatial).asString();

                Neo4JGeospatialScaleEventImpl neo4JGeospatialScaleEventImpl = new Neo4JGeospatialScaleEventImpl(geospatialRegion,eventComment,referLocation,getGeospatialScaleGrade(geospatialScaleGrade.trim()),geospatialScaleEventUID);
                neo4JGeospatialScaleEventImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                geospatialScaleEventList.add(neo4JGeospatialScaleEventImpl);
            }
        }
        return geospatialScaleEventList;
    }

    private Geospatial.GeospatialScaleGrade getGeospatialScaleGrade(String geospatialScaleGradeValue){
        if(geospatialScaleGradeValue.equals("CONTINENT")){
            return Geospatial.GeospatialScaleGrade.CONTINENT;
        }else if(geospatialScaleGradeValue.equals("COUNTRY_REGION")){
            return Geospatial.GeospatialScaleGrade.COUNTRY_REGION;
        }else if(geospatialScaleGradeValue.equals("PROVINCE")){
            return Geospatial.GeospatialScaleGrade.PROVINCE;
        }else if(geospatialScaleGradeValue.equals("PREFECTURE")){
            return Geospatial.GeospatialScaleGrade.PREFECTURE;
        }else if(geospatialScaleGradeValue.equals("COUNTY")){
            return Geospatial.GeospatialScaleGrade.COUNTY;
        }else if(geospatialScaleGradeValue.equals("TOWNSHIP")){
            return Geospatial.GeospatialScaleGrade.TOWNSHIP;
        }else if(geospatialScaleGradeValue.equals("VILLAGE")){
            return Geospatial.GeospatialScaleGrade.VILLAGE;
        }
        return null;
    }
}
