package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.Geospatial;
import com.github.tgda.engine.core.term.GeospatialScaleEntity;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JGeospatialScaleEntityImpl;
import com.github.tgda.engine.core.util.Constant;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.List;

public class GetListGeospatialScaleEntityTransformer implements DataTransformer<List<GeospatialScaleEntity>>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String coreRealmName;
    private String geospatialRegionName;

    public GetListGeospatialScaleEntityTransformer(String coreRealmName,String geospatialRegionName,GraphOperationExecutor workingGraphOperationExecutor){
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
        this.coreRealmName = coreRealmName;
        this.geospatialRegionName = geospatialRegionName;
    }

    @Override
    public List<GeospatialScaleEntity> transformResult(Result result) {
        String targetConceptionKindName = Constant.GeospatialScaleEntityClass;
        List<GeospatialScaleEntity> geospatialScaleEntityList = new ArrayList<>();
        while(result.hasNext()){
            Record nodeRecord = result.next();
            Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
            List<String> allConceptionKindNames = Lists.newArrayList(resultNode.labels());
            boolean isMatchedConceptionKind = true;
            if(allConceptionKindNames.size()>0){
                if(targetConceptionKindName != null){
                    isMatchedConceptionKind = allConceptionKindNames.contains(targetConceptionKindName);
                }else{
                    isMatchedConceptionKind = true;
                }
            }
            if(isMatchedConceptionKind){
                long nodeUID = resultNode.id();
                String conceptionEntityUID = ""+nodeUID;
                String targetGeospatialCode = resultNode.get(Constant.GeospatialCodeProperty).asString();
                String targetGeospatialScaleGradeString = resultNode.get(Constant.GeospatialScaleGradeProperty).asString();
                String _ChineseName = null;
                String _EnglishName = null;
                if(resultNode.containsKey(Constant.GeospatialChineseNameProperty)){
                    _ChineseName = resultNode.get(Constant.GeospatialChineseNameProperty).asString();
                }
                if(resultNode.containsKey(Constant.GeospatialEnglishNameProperty)){
                    _EnglishName = resultNode.get(Constant.GeospatialEnglishNameProperty).asString();
                }
                String currentGeospatialName = this.geospatialRegionName != null ? this.geospatialRegionName:
                        resultNode.get(Constant.GeospatialProperty).asString();

                Geospatial.GeospatialScaleGrade geospatialScaleGrade = null;
                switch (targetGeospatialScaleGradeString){
                    case "CONTINENT":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.CONTINENT;break;
                    case "COUNTRY_REGION":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.COUNTRY_REGION;break;
                    case "PROVINCE":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.PROVINCE;break;
                    case "PREFECTURE":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.PREFECTURE;break;
                    case "COUNTY":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.COUNTY;break;
                    case "TOWNSHIP":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.TOWNSHIP;break;
                    case "VILLAGE":geospatialScaleGrade = Geospatial.GeospatialScaleGrade.VILLAGE;break;
                }
                Neo4JGeospatialScaleEntityImpl neo4JGeospatialScaleEntityImpl =
                        new Neo4JGeospatialScaleEntityImpl(this.coreRealmName,currentGeospatialName,conceptionEntityUID,geospatialScaleGrade,targetGeospatialCode,_ChineseName,_EnglishName);
                neo4JGeospatialScaleEntityImpl.setGlobalGraphOperationExecutor(workingGraphOperationExecutor);
                geospatialScaleEntityList.add(neo4JGeospatialScaleEntityImpl);
            }
        }
        return geospatialScaleEntityList;
    }
}
