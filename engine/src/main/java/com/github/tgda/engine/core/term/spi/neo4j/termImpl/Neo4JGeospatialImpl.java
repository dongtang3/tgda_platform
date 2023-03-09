package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.EqualFilteringItem;
import com.github.tgda.engine.core.analysis.query.filteringItem.SimilarFilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.DataTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetListGeospatialScaleEntityTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleGeospatialScaleEntityTransformer;
import com.github.tgda.engine.core.internal.neo4j.util.GeospatialScaleOperationUtil;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.term.GeospatialScaleEntity;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JGeospatial;
import com.github.tgda.engine.core.util.Constant;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Neo4JGeospatialImpl implements Neo4JGeospatial {

    private static Logger logger = LoggerFactory.getLogger(Neo4JGeospatialImpl.class);
    private String coreRealmName;
    private String geospatialRegionName;
    private String geospatialRegionUID;

    public Neo4JGeospatialImpl(String coreRealmName, String geospatialRegionName, String geospatialRegionUID){
        this.coreRealmName = coreRealmName;
        this.geospatialRegionName = geospatialRegionName;
        this.geospatialRegionUID = geospatialRegionUID;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    @Override
    public String getGeospatialName() {
        return this.geospatialRegionName;
    }

    @Override
    public boolean createGeospatialScaleEntities() {
        try{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            return GeospatialScaleOperationUtil.generateGeospatialScaleEntities(workingGraphOperationExecutor,this.geospatialRegionName);
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public GeospatialScaleEntity getEntityByGeospatialCode(String geospatialCode) {
        if(geospatialCode != null){
            try {
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
                QueryParameters queryParameters = new QueryParameters();
                queryParameters.setResultNumber(1);
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName));
                queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialCodeProperty,geospatialCode), QueryParameters.FilteringLogic.AND);
                queryParameters.setDistinctMode(true);
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleEntityClass,queryParameters,null);
                GetSingleGeospatialScaleEntityTransformer getSingleGeospatialScaleEntityTransformer = new GetSingleGeospatialScaleEntityTransformer(this.coreRealmName,this.geospatialRegionName,workingGraphOperationExecutor);
                Object resultEntityRes = workingGraphOperationExecutor.executeRead(getSingleGeospatialScaleEntityTransformer,queryCql);
                if(resultEntityRes != null){
                    return (GeospatialScaleEntity)resultEntityRes;
                }
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public List<GeospatialScaleEntity> listContinentEntities() {
        try{
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.GeospatialScaleContinentEntityClass,
                    Constant.GeospatialProperty,geospatialRegionName,100);
            GetListGeospatialScaleEntityTransformer getListGeospatialScaleEntityTransformer =
                    new GetListGeospatialScaleEntityTransformer(this.coreRealmName,this.geospatialRegionName,workingGraphOperationExecutor);
            Object resultEntityList = workingGraphOperationExecutor.executeRead(getListGeospatialScaleEntityTransformer,queryCql);
            if(resultEntityList != null){
                return (List<GeospatialScaleEntity>)resultEntityList;
            }
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return new ArrayList<>();
    }

    @Override
    public GeospatialScaleEntity getContinentEntity(GeospatialProperty geospatialProperty, String continentValue) {
        if(continentValue != null){
            try {
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
                QueryParameters queryParameters = new QueryParameters();
                queryParameters.setResultNumber(1);
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName));
                switch(geospatialProperty){
                    case GeospatialCode:
                        queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialCodeProperty,continentValue),QueryParameters.FilteringLogic.AND);
                        break;
                    case ChineseName:
                        queryParameters.addFilteringItem(new SimilarFilteringItem(Constant.GeospatialChineseNameProperty,continentValue,
                                SimilarFilteringItem.MatchingType.BeginWith), QueryParameters.FilteringLogic.AND);
                        break;
                    case EnglishName:
                        queryParameters.addFilteringItem(new SimilarFilteringItem(Constant.GeospatialEnglishNameProperty,continentValue,
                                SimilarFilteringItem.MatchingType.BeginWith), QueryParameters.FilteringLogic.AND);
                        break;
                }
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleContinentEntityClass,queryParameters,null);
                GetSingleGeospatialScaleEntityTransformer getSingleGeospatialScaleEntityTransformer =
                        new GetSingleGeospatialScaleEntityTransformer(this.coreRealmName,this.geospatialRegionName,workingGraphOperationExecutor);
                Object resultEntityRes = workingGraphOperationExecutor.executeRead(getSingleGeospatialScaleEntityTransformer,queryCql);
                if(resultEntityRes != null){
                    return (GeospatialScaleEntity)resultEntityRes;
                }
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public List<GeospatialScaleEntity> listCountryRegionEntities(GeospatialProperty geospatialProperty, String countryValue) {
        try {
            GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
            QueryParameters queryParameters = new QueryParameters();
            queryParameters.setResultNumber(500);
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty, geospatialRegionName));
            if(countryValue != null) {
                switch (geospatialProperty) {
                    case GeospatialCode:
                        queryParameters.addFilteringItem(new SimilarFilteringItem(Constant.GeospatialCodeProperty, countryValue,
                                SimilarFilteringItem.MatchingType.BeginWith), QueryParameters.FilteringLogic.AND);
                        break;
                    case ChineseName:
                        queryParameters.addFilteringItem(new SimilarFilteringItem(Constant.GeospatialChineseNameProperty, countryValue,
                                SimilarFilteringItem.MatchingType.BeginWith), QueryParameters.FilteringLogic.AND);
                        break;
                    case EnglishName:
                        queryParameters.addFilteringItem(new SimilarFilteringItem(Constant.GeospatialEnglishNameProperty, countryValue,
                                SimilarFilteringItem.MatchingType.BeginWith), QueryParameters.FilteringLogic.AND);
                        break;
                }
            }

            String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleCountryRegionEntityClass, queryParameters, null);
            GetListGeospatialScaleEntityTransformer getListGeospatialScaleEntityTransformer =
                    new GetListGeospatialScaleEntityTransformer(this.coreRealmName, this.geospatialRegionName, workingGraphOperationExecutor);
            Object resultEntityList = workingGraphOperationExecutor.executeRead(getListGeospatialScaleEntityTransformer, queryCql);
            if (resultEntityList != null) {
                return (List<GeospatialScaleEntity>) resultEntityList;
            }

        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
        return null;
    }

    @Override
    public GeospatialScaleEntity getCountryRegionEntity(GeospatialProperty geospatialProperty, String countryValue) {
        if(countryValue != null){
            try {
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
                QueryParameters queryParameters = new QueryParameters();
                queryParameters.setResultNumber(1);
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName));
                switch(geospatialProperty){
                    case GeospatialCode:
                        queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialCodeProperty,countryValue),QueryParameters.FilteringLogic.AND);
                        break;
                    case ChineseName:
                        queryParameters.addFilteringItem(new SimilarFilteringItem(Constant.GeospatialChineseNameProperty,countryValue,
                                SimilarFilteringItem.MatchingType.BeginWith), QueryParameters.FilteringLogic.AND);
                        break;
                    case EnglishName:
                        queryParameters.addFilteringItem(new SimilarFilteringItem(Constant.GeospatialEnglishNameProperty,countryValue,
                                SimilarFilteringItem.MatchingType.BeginWith), QueryParameters.FilteringLogic.AND);
                        break;
                }
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleCountryRegionEntityClass,queryParameters,null);
                GetSingleGeospatialScaleEntityTransformer getSingleGeospatialScaleEntityTransformer = new GetSingleGeospatialScaleEntityTransformer(this.coreRealmName,this.geospatialRegionName,workingGraphOperationExecutor);
                Object resultEntityRes = workingGraphOperationExecutor.executeRead(getSingleGeospatialScaleEntityTransformer,queryCql);
                if(resultEntityRes != null){
                    return (GeospatialScaleEntity)resultEntityRes;
                }
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public List<GeospatialScaleEntity> listProvinceEntities(GeospatialProperty geospatialProperty, String countryValue, String provinceValue) throws EngineServiceRuntimeException {
        if(countryValue != null){
            try {
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
                QueryParameters queryParameters = new QueryParameters();
                queryParameters.setResultNumber(1);
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName));
                switch(geospatialProperty){
                    case GeospatialCode:
                        queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialCodeProperty,countryValue),QueryParameters.FilteringLogic.AND);
                        break;
                    case ChineseName:
                        queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialChineseNameProperty,countryValue),QueryParameters.FilteringLogic.AND);
                        break;
                    case EnglishName:
                        queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialEnglishNameProperty,countryValue),QueryParameters.FilteringLogic.AND);
                        break;
                }
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleCountryRegionEntityClass,queryParameters,null);
                GetSingleGeospatialScaleEntityTransformer getSingleGeospatialScaleEntityTransformer = new GetSingleGeospatialScaleEntityTransformer(this.coreRealmName,this.geospatialRegionName,workingGraphOperationExecutor);
                Object resultEntityRes = workingGraphOperationExecutor.executeRead(getSingleGeospatialScaleEntityTransformer,queryCql);
                if(resultEntityRes == null){
                    logger.error("COUNTRY_REGION GeospatialScaleEntity with {} = {} doesn't exist.", ""+geospatialProperty, countryValue);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("COUNTRY_REGION GeospatialScaleEntity with "+geospatialProperty+" = "+countryValue+" doesn't exist.");
                    throw exception;
                }else{
                    GeospatialScaleEntity targetCountryRegionEntity = (GeospatialScaleEntity)resultEntityRes;
                    List<GeospatialScaleEntity> allChildrenGeospatialScaleEntities = targetCountryRegionEntity.getChildEntities();
                    return getFullMatchedEntitiesList(allChildrenGeospatialScaleEntities,geospatialProperty,provinceValue);
                }
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public GeospatialScaleEntity getProvinceEntity(GeospatialProperty geospatialProperty, String countryValue, String provinceValue) throws EngineServiceRuntimeException {
        if(countryValue != null && provinceValue != null){
            try {
                GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
                QueryParameters queryParameters = new QueryParameters();
                queryParameters.setResultNumber(1);
                queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName));
                switch(geospatialProperty){
                    case GeospatialCode:
                        queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialCodeProperty,countryValue),QueryParameters.FilteringLogic.AND);
                        break;
                    case ChineseName:
                        queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialChineseNameProperty,countryValue),QueryParameters.FilteringLogic.AND);
                        break;
                    case EnglishName:
                        queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialEnglishNameProperty,countryValue),QueryParameters.FilteringLogic.AND);
                        break;
                }
                String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleCountryRegionEntityClass,queryParameters,null);
                GetSingleGeospatialScaleEntityTransformer getSingleGeospatialScaleEntityTransformer = new GetSingleGeospatialScaleEntityTransformer(this.coreRealmName,this.geospatialRegionName,workingGraphOperationExecutor);
                Object resultEntityRes = workingGraphOperationExecutor.executeRead(getSingleGeospatialScaleEntityTransformer,queryCql);
                if(resultEntityRes == null){
                    logger.error("COUNTRY_REGION GeospatialScaleEntity with {} = {} doesn't exist.", ""+geospatialProperty, countryValue);
                    EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
                    exception.setCauseMessage("COUNTRY_REGION GeospatialScaleEntity with "+geospatialProperty+" = "+countryValue+" doesn't exist.");
                    throw exception;
                }else{
                    GeospatialScaleEntity targetCountryRegionEntity = (GeospatialScaleEntity)resultEntityRes;
                    List<GeospatialScaleEntity> allChildrenGeospatialScaleEntities = targetCountryRegionEntity.getChildEntities();
                    return getFullMatchedEntity(allChildrenGeospatialScaleEntities,geospatialProperty,provinceValue);
                }
            } catch (EngineServiceEntityExploreException e) {
                e.printStackTrace();
            }finally {
                this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
            }
        }
        return null;
    }

    @Override
    public List<GeospatialScaleEntity> listPrefectureEntities(GeospatialProperty geospatialProperty, String countryValue, String provinceValue, String prefectureValue) throws EngineServiceRuntimeException {
        GeospatialScaleEntity provinceGeospatialScaleEntity = getProvinceEntity(geospatialProperty,countryValue,provinceValue);
        if(provinceGeospatialScaleEntity == null){
            logger.error("PROVINCE GeospatialScaleEntity with {} = {} , doesn't exist.", ""+geospatialProperty, countryValue+","+provinceValue);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("PROVINCE GeospatialScaleEntity with "+geospatialProperty+" = "+countryValue+","+provinceValue+" doesn't exist.");
            throw exception;
        }else{
            List<GeospatialScaleEntity> allChildrenGeospatialScaleEntities = provinceGeospatialScaleEntity.getChildEntities();
            return getFullMatchedEntitiesList(allChildrenGeospatialScaleEntities,geospatialProperty,prefectureValue);
        }
    }

    @Override
    public GeospatialScaleEntity getPrefectureEntity(GeospatialProperty geospatialProperty, String countryValue, String provinceValue, String prefectureValue) throws EngineServiceRuntimeException {
        GeospatialScaleEntity provinceGeospatialScaleEntity = getProvinceEntity(geospatialProperty,countryValue,provinceValue);
        if(provinceGeospatialScaleEntity == null){
            logger.error("PROVINCE GeospatialScaleEntity with {} = {} , doesn't exist.", ""+geospatialProperty, countryValue+","+provinceValue);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("PROVINCE GeospatialScaleEntity with "+geospatialProperty+" = "+countryValue+","+provinceValue+" doesn't exist.");
            throw exception;
        }else{
            List<GeospatialScaleEntity> allChildrenGeospatialScaleEntities = provinceGeospatialScaleEntity.getChildEntities();
            return getFullMatchedEntity(allChildrenGeospatialScaleEntities,geospatialProperty,prefectureValue);
        }
    }

    @Override
    public List<GeospatialScaleEntity> listCountyEntities(GeospatialProperty geospatialProperty, String countryValue, String provinceValue, String prefectureValue, String countyValue) throws EngineServiceRuntimeException {
        GeospatialScaleEntity provinceGeospatialScaleEntity = getPrefectureEntity(geospatialProperty,countryValue,provinceValue,prefectureValue);
        if(provinceGeospatialScaleEntity == null){
            logger.error("PREFECTURE GeospatialScaleEntity with {} = {} , doesn't exist.", ""+geospatialProperty, countryValue+","+provinceValue+","+prefectureValue);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("PREFECTURE GeospatialScaleEntity with "+geospatialProperty+" = "+countryValue+","+provinceValue+","+prefectureValue+" doesn't exist.");
            throw exception;
        }else{
            List<GeospatialScaleEntity> allChildrenGeospatialScaleEntities = provinceGeospatialScaleEntity.getChildEntities();
            return getFullMatchedEntitiesList(allChildrenGeospatialScaleEntities,geospatialProperty,countyValue);
        }
    }

    @Override
    public GeospatialScaleEntity getCountyEntity(GeospatialProperty geospatialProperty, String countryValue, String provinceValue, String prefectureValue, String countyValue) throws EngineServiceRuntimeException {
        GeospatialScaleEntity provinceGeospatialScaleEntity = getPrefectureEntity(geospatialProperty,countryValue,provinceValue,prefectureValue);
        if(provinceGeospatialScaleEntity == null){
            logger.error("PREFECTURE GeospatialScaleEntity with {} = {} , doesn't exist.", ""+geospatialProperty, countryValue+","+provinceValue+","+prefectureValue);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("PREFECTURE GeospatialScaleEntity with "+geospatialProperty+" = "+countryValue+","+provinceValue+","+prefectureValue+" doesn't exist.");
            throw exception;
        }else{
            List<GeospatialScaleEntity> allChildrenGeospatialScaleEntities = provinceGeospatialScaleEntity.getChildEntities();
            return getFullMatchedEntity(allChildrenGeospatialScaleEntities,geospatialProperty,countyValue);
        }
    }

    @Override
    public List<GeospatialScaleEntity> listTownshipEntities(GeospatialProperty geospatialProperty, String countryValue, String provinceValue, String prefectureValue, String countyValue, String townshipValue) throws EngineServiceRuntimeException {
        GeospatialScaleEntity provinceGeospatialScaleEntity = getCountyEntity(geospatialProperty,countryValue,provinceValue,prefectureValue,countyValue);
        if(provinceGeospatialScaleEntity == null){
            logger.error("COUNTY GeospatialScaleEntity with {} = {} , doesn't exist.", ""+geospatialProperty, countryValue+","+provinceValue+","+prefectureValue+","+countyValue);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("COUNTY GeospatialScaleEntity with "+geospatialProperty+" = "+countryValue+","+provinceValue+","+prefectureValue+","+countyValue+" doesn't exist.");
            throw exception;
        }else{
            List<GeospatialScaleEntity> allChildrenGeospatialScaleEntities = provinceGeospatialScaleEntity.getChildEntities();
            return getFullMatchedEntitiesList(allChildrenGeospatialScaleEntities,geospatialProperty,townshipValue);
        }
    }

    @Override
    public GeospatialScaleEntity getTownshipEntity(GeospatialProperty geospatialProperty, String countryValue, String provinceValue, String prefectureValue, String countyValue, String townshipValue) throws EngineServiceRuntimeException {
        GeospatialScaleEntity provinceGeospatialScaleEntity = getCountyEntity(geospatialProperty,countryValue,provinceValue,prefectureValue,countyValue);
        if(provinceGeospatialScaleEntity == null){
            logger.error("COUNTY GeospatialScaleEntity with {} = {} , doesn't exist.", ""+geospatialProperty, countryValue+","+provinceValue+","+prefectureValue+","+countyValue);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("COUNTY GeospatialScaleEntity with "+geospatialProperty+" = "+countryValue+","+provinceValue+","+prefectureValue+","+countyValue+" doesn't exist.");
            throw exception;
        }else{
            List<GeospatialScaleEntity> allChildrenGeospatialScaleEntities = provinceGeospatialScaleEntity.getChildEntities();
            return getFullMatchedEntity(allChildrenGeospatialScaleEntities,geospatialProperty,townshipValue);
        }
    }

    @Override
    public List<GeospatialScaleEntity> listVillageEntities(GeospatialProperty geospatialProperty, String countryValue, String provinceValue, String prefectureValue, String countyValue, String townshipValue, String villageValue) throws EngineServiceRuntimeException {
        GeospatialScaleEntity provinceGeospatialScaleEntity = getTownshipEntity(geospatialProperty,countryValue,provinceValue,prefectureValue,countyValue,townshipValue);
        if(provinceGeospatialScaleEntity == null){
            logger.error("TOWNSHIP GeospatialScaleEntity with {} = {} , doesn't exist.", ""+geospatialProperty, countryValue+","+provinceValue+","+prefectureValue+","+countyValue+","+townshipValue);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("TOWNSHIP GeospatialScaleEntity with "+geospatialProperty+" = "+countryValue+","+provinceValue+","+prefectureValue+","+countyValue+","+townshipValue+" doesn't exist.");
            throw exception;
        }else{
            List<GeospatialScaleEntity> allChildrenGeospatialScaleEntities = provinceGeospatialScaleEntity.getChildEntities();
            return getFullMatchedEntitiesList(allChildrenGeospatialScaleEntities,geospatialProperty,villageValue);
        }
    }

    @Override
    public GeospatialScaleEntity getVillageEntity(GeospatialProperty geospatialProperty, String countryValue, String provinceValue, String prefectureValue, String countyValue, String townshipValue, String villageValue) throws EngineServiceRuntimeException {
        GeospatialScaleEntity provinceGeospatialScaleEntity = getTownshipEntity(geospatialProperty,countryValue,provinceValue,prefectureValue,countyValue,townshipValue);
        if(provinceGeospatialScaleEntity == null){
            logger.error("TOWNSHIP GeospatialScaleEntity with {} = {} , doesn't exist.", ""+geospatialProperty, countryValue+","+provinceValue+","+prefectureValue+","+countyValue+","+townshipValue);
            EngineServiceRuntimeException exception = new EngineServiceRuntimeException();
            exception.setCauseMessage("TOWNSHIP GeospatialScaleEntity with "+geospatialProperty+" = "+countryValue+","+provinceValue+","+prefectureValue+","+countyValue+","+townshipValue+" doesn't exist.");
            throw exception;
        }else{
            List<GeospatialScaleEntity> allChildrenGeospatialScaleEntities = provinceGeospatialScaleEntity.getChildEntities();
            return getFullMatchedEntity(allChildrenGeospatialScaleEntities,geospatialProperty,villageValue);
        }
    }

    @Override
    public long removeRefersGeospatialScaleEvents() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            String deleteEntitiesCql = "CALL apoc.periodic.commit(\"MATCH (n:"+ Constant.GeospatialScaleEventClass+") WHERE n."+ Constant._GeospatialScaleEventGeospatial+"='"+this.geospatialRegionName+"' WITH n LIMIT $limit DETACH DELETE n RETURN count(*)\",{limit: 10000}) YIELD updates, executions, runtime, batches RETURN updates, executions, runtime, batches";
            logger.debug("Generated Cypher Statement: {}", deleteEntitiesCql);
            DataTransformer<Long> deleteTransformer = new DataTransformer() {
                @Override
                public Long transformResult(Result result) {
                    while(result.hasNext()){
                        Record nodeRecord = result.next();
                        Long deletedTimeScaleEntitiesNumber =  nodeRecord.get("updates").asLong();
                        return deletedTimeScaleEntitiesNumber;
                    }
                    return null;
                }
            };
            Object deleteEntitiesRes = workingGraphOperationExecutor.executeWrite(deleteTransformer,deleteEntitiesCql);
            long currentDeletedEntitiesCount = deleteEntitiesRes != null ? ((Long)deleteEntitiesRes).longValue():0;
            return currentDeletedEntitiesCount;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    private GeospatialScaleEntity getFullMatchedEntity(List<GeospatialScaleEntity> geospatialScaleEntitiesList,
                                                       GeospatialProperty geospatialProperty,String propertyValue){
        for(GeospatialScaleEntity currentChildEntity:geospatialScaleEntitiesList){
            switch(geospatialProperty){
                case GeospatialCode:
                    if(currentChildEntity.getGeospatialCode().equals(propertyValue)){
                        return currentChildEntity;
                    }
                    break;
                case ChineseName:
                    if(currentChildEntity.getChineseName() != null && currentChildEntity.getChineseName().equals(propertyValue)){
                        return currentChildEntity;
                    }
                    break;
                case EnglishName:
                    if(currentChildEntity.getEnglishName() != null && currentChildEntity.getEnglishName().equals(propertyValue)){
                        return currentChildEntity;
                    }
                    break;
            }
        }
        return null;
    }

    private List<GeospatialScaleEntity> getFullMatchedEntitiesList(List<GeospatialScaleEntity> geospatialScaleEntitiesList,
                                                       GeospatialProperty geospatialProperty,String propertyValue){
        if(propertyValue == null){
            return geospatialScaleEntitiesList;
        }
        List<GeospatialScaleEntity> matchedProvinceEntities = new ArrayList<>();
        for(GeospatialScaleEntity currentChildEntity:geospatialScaleEntitiesList){
            switch(geospatialProperty){
                case GeospatialCode:
                    if(currentChildEntity.getGeospatialCode().startsWith(propertyValue)){
                        matchedProvinceEntities.add(currentChildEntity);
                    }
                    break;
                case ChineseName:
                    if(currentChildEntity.getChineseName() != null && currentChildEntity.getChineseName().startsWith(propertyValue)){
                        matchedProvinceEntities.add(currentChildEntity);
                    }
                    break;
                case EnglishName:
                    if(currentChildEntity.getEnglishName() != null && currentChildEntity.getEnglishName().startsWith(propertyValue)){
                        matchedProvinceEntities.add(currentChildEntity);
                    }
                    break;
            }
        }
        return matchedProvinceEntities;
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }
}
