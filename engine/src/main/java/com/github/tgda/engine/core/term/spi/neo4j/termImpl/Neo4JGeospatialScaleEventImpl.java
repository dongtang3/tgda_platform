package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.feature.spi.neo4j.featureImpl.Neo4JAttributesMeasurableImpl;
import com.github.tgda.engine.core.feature.spi.neo4j.featureInf.Neo4JClassificationAttachable;
import com.github.tgda.engine.core.feature.spi.neo4j.featureInf.Neo4JMultiConceptionKindsSupportable;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.DataTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleEntityTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleGeospatialScaleEntityTransformer;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.util.Constant;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.term.Geospatial;
import com.github.tgda.engine.core.term.GeospatialScaleEntity;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JGeospatialScaleEvent;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Neo4JGeospatialScaleEventImpl extends Neo4JAttributesMeasurableImpl implements Neo4JClassificationAttachable, Neo4JGeospatialScaleEvent, Neo4JMultiConceptionKindsSupportable {

    private static Logger logger = LoggerFactory.getLogger(Neo4JGeospatialScaleEventImpl.class);
    private String geospatialRegionName;
    private String eventComment;
    private String referLocation;
    private Geospatial.GeospatialScaleGrade geospatialScaleGrade;
    private String geospatialScaleEventUID;

    public Neo4JGeospatialScaleEventImpl(String geospatialRegionName, String eventComment, String referLocation, Geospatial.GeospatialScaleGrade geospatialScaleGrade,
                                         String geospatialScaleEventUID) {
        super(geospatialScaleEventUID);
        this.geospatialRegionName = geospatialRegionName;
        this.eventComment = eventComment;
        this.referLocation = referLocation;
        this.geospatialScaleGrade = geospatialScaleGrade;
        this.geospatialScaleEventUID = geospatialScaleEventUID;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    @Override
    public String getGeospatialName() {
        return this.geospatialRegionName;
    }

    @Override
    public String getReferLocation() {
        return this.referLocation;
    }

    @Override
    public Geospatial.GeospatialScaleGrade getGeospatialScaleGrade() {
        return this.geospatialScaleGrade;
    }

    @Override
    public String getGeospatialScaleEventUID() {
        return this.geospatialScaleEventUID;
    }

    @Override
    public String getEventComment() {
        return this.eventComment;
    }

    @Override
    public GeospatialScaleEntity getReferGeospatialScaleEntity() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = "MATCH(currentEntity:"+ Constant.GeospatialScaleEventClass+")<-[:"+ Constant.GeospatialScale_GeospatialReferToRelationClass+"]-(geospatialScaleEntity:"+ Constant.GeospatialScaleEntityClass+") WHERE id(currentEntity) = "+ this.geospatialScaleEventUID +" RETURN geospatialScaleEntity as operationResult";
            logger.debug("Generated Cypher Statement: {}", queryCql);
            GetSingleGeospatialScaleEntityTransformer getSingleGeospatialScaleEntityTransformer =
                    new GetSingleGeospatialScaleEntityTransformer(null,this.geospatialRegionName,graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object queryRes = workingGraphOperationExecutor.executeRead(getSingleGeospatialScaleEntityTransformer,queryCql);
            if(queryRes != null){
                return (GeospatialScaleEntity)queryRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public Entity getAttachEntity() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = "MATCH(currentEntity:"+ Constant.GeospatialScaleEventClass+")<-[:"+ Constant.GeospatialScale_AttachToRelationClass+"]-(conceptionEntity) WHERE id(currentEntity) = "+ this.geospatialScaleEventUID +" RETURN conceptionEntity as operationResult";
            logger.debug("Generated Cypher Statement: {}", queryCql);
            GetSingleEntityTransformer getSingleEntityTransformer =
                    new GetSingleEntityTransformer(null,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object queryRes = workingGraphOperationExecutor.executeRead(getSingleEntityTransformer,queryCql);
            if(queryRes != null){
                return (Entity)queryRes;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public List<String> getAliasConceptionKindNames() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String cypherProcedureString = "MATCH (targetNodes) WHERE id(targetNodes) = " + this.geospatialScaleEventUID+"\n"+
                    "RETURN DISTINCT targetNodes as operationResult";
            DataTransformer<List<String>> dataTransfer = new DataTransformer<List<String>>() {
                @Override
                public List<String> transformResult(Result result) {
                    if(result.hasNext()){
                        Record nodeRecord = result.next();
                        Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                        List<String> allConceptionKindNames = Lists.newArrayList(resultNode.labels());
                        allConceptionKindNames.remove(Constant.GeospatialScaleEventClass);
                        return allConceptionKindNames;
                    }
                    return null;
                }
            };

            Object conceptionEntityNameList = workingGraphOperationExecutor.executeRead(dataTransfer,cypherProcedureString);
            if(conceptionEntityNameList != null){
                return (List<String>)conceptionEntityNameList;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        super.setGlobalGraphOperationExecutor(graphOperationExecutor);
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }

    @Override
    public String getEntityUID() {
        return this.geospatialScaleEventUID;
    }

    @Override
    public GraphOperationExecutorHelper getGraphOperationExecutorHelper() {
        return graphOperationExecutorHelper;
    }
}
