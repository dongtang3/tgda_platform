package com.github.tgda.engine.core.internal.neo4j.util;

import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetListEntityTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetListEntityValueTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleEntityTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleRelationshipEntityTransformer;
import com.github.tgda.engine.core.payload.EntityValue;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.EqualFilteringItem;
import com.github.tgda.engine.core.feature.GeospatialScaleFeatureSupportable;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.coreRealm.realmServiceCore.term.*;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JAttributeImpl;
import com.github.tgda.engine.core.util.Constant;
import com.github.tgda.engine.core.util.config.PropertiesHandler;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GeospatialScaleOperationUtil {

    private static Logger logger = LoggerFactory.getLogger(GeospatialScaleOperationUtil.class);
    private static final String GEOSPATIAL_DATA_FOLDER = "geospatialData";

    public static boolean generateGeospatialScaleEntities(GraphOperationExecutor workingGraphOperationExecutor, String geospatialRegionName){
        generateGeospatialScaleEntities_Continent(workingGraphOperationExecutor,geospatialRegionName);
        generateGeospatialScaleEntities_CountryRegion(workingGraphOperationExecutor,geospatialRegionName);
        updateCountryRegionEntities_GeospatialScaleInfo(workingGraphOperationExecutor,geospatialRegionName);
        generateGeospatialScaleEntities_ProvinceOfWorld(workingGraphOperationExecutor,geospatialRegionName);
        generateGeospatialScaleEntities_ProvinceOfChina(workingGraphOperationExecutor,geospatialRegionName);
        generateGeospatialScaleEntities_PrefectureAndLaterOfChina(workingGraphOperationExecutor,geospatialRegionName);
        linkGeospatialScaleEntitiesOfChina(workingGraphOperationExecutor,geospatialRegionName);
        linkSpecialAdministrativeRegionEntitiesOfChina(workingGraphOperationExecutor,geospatialRegionName);
        return true;
    }

    private static void generateGeospatialScaleEntities_Continent(GraphOperationExecutor workingGraphOperationExecutor, String geospatialRegionName){
        GetSingleEntityTransformer getSingleEntityTransformer =
                new GetSingleEntityTransformer(Constant.GeospatialClass,workingGraphOperationExecutor);
        String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.GeospatialClass, Constant._NameProperty,geospatialRegionName,1);
        Object getGeospatialRes = workingGraphOperationExecutor.executeRead(getSingleEntityTransformer,queryCql);
        String geospatialRegionUID = null;

        if(getGeospatialRes != null){
            Entity geospatialRegionEntity = (Entity) getGeospatialRes;
            geospatialRegionUID = geospatialRegionEntity.getEntityUID();
        }

        getSingleEntityTransformer =
                new GetSingleEntityTransformer(Constant.GeospatialScaleContinentEntityClass,workingGraphOperationExecutor);

        String[] conceptionTypeNameArray = new String[2];
        conceptionTypeNameArray[0] = Constant.GeospatialScaleEntityClass;
        conceptionTypeNameArray[1] = Constant.GeospatialScaleContinentEntityClass;

        File file = new File(PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/"+"ContinentsData.txt");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                String currentLine = !tempStr.startsWith("# ISO Code")? tempStr : null;
                if(currentLine != null){
                    String[] dataItems = currentLine.split("\t");
                    String _ISOCode = dataItems[0];
                    String _EngName = dataItems[1];
                    String _ChnName = dataItems[2];
                    String _ChnFullName = dataItems[3];

                    Map<String,Object> propertiesMap = new HashMap<>();
                    propertiesMap.put("ISO_Code",_ISOCode);
                    propertiesMap.put(Constant.GeospatialChineseNameProperty,_ChnName);
                    propertiesMap.put(Constant.GeospatialEnglishNameProperty,_EngName);
                    propertiesMap.put("ChineseFullName",_ChnFullName);
                    propertiesMap.put(Constant.GeospatialCodeProperty,_EngName);
                    propertiesMap.put(Constant.GeospatialProperty,geospatialRegionName);
                    propertiesMap.put(Constant.GeospatialScaleGradeProperty, ""+Geospatial.GeospatialScaleGrade.CONTINENT);

                    String createGeospatialScaleEntitiesCql = CypherBuilder.createLabeledNodeWithProperties(conceptionTypeNameArray,propertiesMap);
                    Object newEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,createGeospatialScaleEntitiesCql);

                    if(newEntityRes != null && geospatialRegionUID != null){
                        Entity geospatialScaleEntity = (Entity) newEntityRes;
                        geospatialScaleEntity.attachToRelation(geospatialRegionUID, Constant.GeospatialScale_SpatialContainsRelationClass, null, true);
                    }
                }
            }
            reader.close();
        } catch (
                IOException | EngineServiceRuntimeException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    private static void generateGeospatialScaleEntities_CountryRegion(GraphOperationExecutor workingGraphOperationExecutor, String geospatialRegionName){
        Map<String,String> ContinentCode_EntityUIDMap = new HashMap<>();
        String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.GeospatialScaleContinentEntityClass, Constant.GeospatialProperty,geospatialRegionName,100000);

        GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(Constant.GeospatialScaleContinentEntityClass,workingGraphOperationExecutor);
        Object resultEntityList = workingGraphOperationExecutor.executeRead(getListEntityTransformer,queryCql);
        if(resultEntityList != null){
            List<Entity> resultContinentList =  (List<Entity>)resultEntityList;
            for(Entity currentEntity : resultContinentList){
                ContinentCode_EntityUIDMap.put(
                    currentEntity.getAttribute(Constant.GeospatialCodeProperty).getAttributeValue().toString(),
                        currentEntity.getEntityUID());
            }
        }

        String[] conceptionTypeNameArray = new String[2];
        conceptionTypeNameArray[0] = Constant.GeospatialScaleEntityClass;
        conceptionTypeNameArray[1] = Constant.GeospatialScaleCountryRegionEntityClass;

        File file = new File(PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/"+"CountriesAndRegionsData(ISO_3166_1).txt");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
                if(currentLine != null){
                    String[] countriesAndRegionInfoValueArray = currentLine.split("\\|");

                    Map<String,Object> propertiesMap = new HashMap<>();
                    String continentEntityUID = null;
                    String _2bitCode = countriesAndRegionInfoValueArray[0].trim();
                    String _3bitCode = countriesAndRegionInfoValueArray[1].trim();
                    String _number = countriesAndRegionInfoValueArray[2].trim();
                    String _ISO3122_2Code = countriesAndRegionInfoValueArray[3].trim();
                    String _EnglishName = countriesAndRegionInfoValueArray[4].trim();
                    String _ChineseName = countriesAndRegionInfoValueArray[5].trim();

                    propertiesMap.put("Alpha_2Code",_2bitCode);
                    propertiesMap.put("Alpha_3Code",_3bitCode);
                    propertiesMap.put("NumericCode",_number);
                    propertiesMap.put("ISO3166_2Code",_ISO3122_2Code);
                    propertiesMap.put(Constant.GeospatialEnglishNameProperty,_EnglishName);
                    propertiesMap.put(Constant.GeospatialChineseNameProperty,_ChineseName);
                    propertiesMap.put("Standard","ISO 3166-1:2013");
                    propertiesMap.put("StandardStatus","Officially assigned");

                    if(!"-".equals(countriesAndRegionInfoValueArray[6].trim())){
                        String _belongedContinent = countriesAndRegionInfoValueArray[6].trim();
                        propertiesMap.put("belongedContinent",_belongedContinent);
                        continentEntityUID = ContinentCode_EntityUIDMap.get(_belongedContinent);
                    }
                    if(!"-".equals(countriesAndRegionInfoValueArray[7].trim())){
                        String _capitalChineseName = countriesAndRegionInfoValueArray[7].trim();
                        propertiesMap.put("capitalChineseName",_capitalChineseName);
                    }
                    if(!"-".equals(countriesAndRegionInfoValueArray[8].trim())){
                        String _capitalEnglishName = countriesAndRegionInfoValueArray[8].trim();
                        propertiesMap.put("capitalEnglishName",_capitalEnglishName);
                    }

                    propertiesMap.put(Constant.GeospatialCodeProperty,_2bitCode);
                    propertiesMap.put(Constant.GeospatialProperty,geospatialRegionName);
                    propertiesMap.put(Constant.GeospatialScaleGradeProperty, ""+Geospatial.GeospatialScaleGrade.COUNTRY_REGION);

                    GetSingleEntityTransformer getSingleEntityTransformer =
                            new GetSingleEntityTransformer(Constant.GeospatialScaleCountryRegionEntityClass,workingGraphOperationExecutor);

                    String createGeospatialScaleEntitiesCql = CypherBuilder.createLabeledNodeWithProperties(conceptionTypeNameArray,propertiesMap);
                    Object newEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,createGeospatialScaleEntitiesCql);

                    if(newEntityRes != null && continentEntityUID != null){
                        Entity geospatialEntity = (Entity) newEntityRes;
                        geospatialEntity.attachToRelation(continentEntityUID, Constant.GeospatialScale_SpatialContainsRelationClass, null, true);
                    }
                }
            }
            reader.close();
        } catch (IOException | EngineServiceRuntimeException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    private static void updateCountryRegionEntities_GeospatialScaleInfo(GraphOperationExecutor workingGraphOperationExecutor, String geospatialRegionName){
        Map<String,Map<String,Object>> _CountriesDataMap = generateNE_10m_CountriesDataMap();
        String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.GeospatialScaleCountryRegionEntityClass, Constant.GeospatialProperty,geospatialRegionName,100000);
        GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(Constant.GeospatialScaleCountryRegionEntityClass,workingGraphOperationExecutor);
        Object resultEntityList = workingGraphOperationExecutor.executeRead(getListEntityTransformer,queryCql);
        if(resultEntityList != null){
            List<Entity> resultContinentList =  (List<Entity>)resultEntityList;
            for(Entity currentEntity : resultContinentList){
                String _CountryRegionAlpha_2Code = currentEntity.getAttribute("Alpha_2Code").getAttributeValue().toString();
                Map<String,Object> _CountriesData = _CountriesDataMap.get(_CountryRegionAlpha_2Code);
                if(_CountriesData != null && _CountriesData.get("the_geom")!= null){
                    String geomWKT = _CountriesData.get("the_geom").toString();
                    currentEntity.addOrUpdateGeometryType(GeospatialScaleFeatureSupportable.WKTGeometryType.MULTIPOLYGON);
                    currentEntity.addOrUpdateGlobalCRSAID("EPSG:4326"); // CRS EPSG:4326 - WGS 84 - Geographic
                    currentEntity.addOrUpdateGLGeometryContent(geomWKT);
                }
            }
        }
    }

    private static void generateGeospatialScaleEntities_ProvinceOfWorld(GraphOperationExecutor workingGraphOperationExecutor, String geospatialRegionName){
        Map<String,Map<String,Object>> _ProvincesISO_3166_2DataMap = generateNE_10m_admin_states_provincesDataMap();
        String queryCql = CypherBuilder.matchLabelWithSinglePropertyValue(Constant.GeospatialScaleCountryRegionEntityClass, Constant.GeospatialProperty,geospatialRegionName,100000);
        GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(Constant.GeospatialScaleCountryRegionEntityClass,workingGraphOperationExecutor);
        Object resultEntityList = workingGraphOperationExecutor.executeRead(getListEntityTransformer,queryCql);
        if(resultEntityList != null){
            List<Entity> resultContinentList =  (List<Entity>)resultEntityList;
            for(Entity currentEntity : resultContinentList){
                generateProvinceLevelEntitiesOfWorld(currentEntity,_ProvincesISO_3166_2DataMap,geospatialRegionName,workingGraphOperationExecutor);
            }
        }
    }

    private static void generateProvinceLevelEntitiesOfWorld(Entity _CountryRegionEntity,Map<String,Map<String,Object>> _ProvincesDataMap,String geospatialRegionName,GraphOperationExecutor workingGraphOperationExecutor){
        String _CountryRegionAlpha_2Code = _CountryRegionEntity.getAttribute("Alpha_2Code").getAttributeValue().toString();
        String _CountryRegionEntityUID = _CountryRegionEntity.getEntityUID();
        if(!_CountryRegionAlpha_2Code.equals("CN")&&!_CountryRegionAlpha_2Code.equals("TW")&&
                !_CountryRegionAlpha_2Code.equals("HK")&&!_CountryRegionAlpha_2Code.equals("MO")){
            String[] conceptionTypeNameArray = new String[2];
            conceptionTypeNameArray[0] = Constant.GeospatialScaleEntityClass;
            conceptionTypeNameArray[1] = Constant.GeospatialScaleProvinceEntityClass;
            String filePath =
                    PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/statesAndProvinces/states_provinces(ISO_3166_2)/"+_CountryRegionAlpha_2Code+".csv";
            File file = new File(filePath);
            List<String> operatedItem = new ArrayList<>();
            if (file.exists()){
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new FileReader(file));
                    String tempStr;
                    while ((tempStr = reader.readLine()) != null) {
                        String[] iso_3166_2DataArray = tempStr.split("\\|");
                        String subdivisionCategory = iso_3166_2DataArray[0];
                        String _3166_2Code = iso_3166_2DataArray[1];
                        String subdivisionName = iso_3166_2DataArray[2];
                        //String localVariant = iso_3166_2DataArray[3];
                        //String languageCode = iso_3166_2DataArray[4];
                        _3166_2Code=_3166_2Code.replace("*","");
                        if(!operatedItem.contains(_3166_2Code)){
                            Map<String,Object> propertiesMap = new HashMap<>();
                            propertiesMap.put("ISO3166_1Alpha_2Code",_CountryRegionAlpha_2Code);
                            propertiesMap.put("ISO3166_2SubDivisionCode",_3166_2Code);
                            propertiesMap.put("ISO3166_2SubdivisionName",subdivisionName);
                            propertiesMap.put("ISO3166_2SubdivisionCategory",subdivisionCategory);
                            propertiesMap.put("Standard","ISO 3166-2:2013");
                            propertiesMap.put("StandardStatus","Officially assigned");
                            propertiesMap.put(Constant.GeospatialCodeProperty,_3166_2Code);
                            propertiesMap.put(Constant.GeospatialProperty,geospatialRegionName);
                            propertiesMap.put(Constant.GeospatialScaleGradeProperty, ""+Geospatial.GeospatialScaleGrade.PROVINCE);

                            Map<String,Object> _currentProvincesDataMap =  _ProvincesDataMap.get(_3166_2Code);

                            if(_currentProvincesDataMap == null){
                                logger.debug("CountryRegionAlpha_2Code {} not found in NE_10m_admin_states_provincesDataMap.",_CountryRegionAlpha_2Code);
                            }else{
                                propertiesMap.put("DivisionCategory_EN",_currentProvincesDataMap.get("type_en"));
                                //propertiesMap.put("DivisionCategory_CH",_DivisionCategory_CH);
                                propertiesMap.put(Constant.GeospatialEnglishNameProperty,_currentProvincesDataMap.get("name_en"));
                                propertiesMap.put(Constant.GeospatialChineseNameProperty,_currentProvincesDataMap.get("name_zh"));

                                String poiPointWKT = "POINT ("+_currentProvincesDataMap.get("longitude")+" "+_currentProvincesDataMap.get("latitude")+")";
                                propertiesMap.put(Constant._GeospatialGLGeometryPOI,poiPointWKT);
                                propertiesMap.put(Constant._GeospatialGlobalCRSAID,"EPSG:4326"); // CRS EPSG:4326 - WGS 84 - Geographic
                                propertiesMap.put(Constant._GeospatialGeometryType,""+GeospatialScaleFeatureSupportable.WKTGeometryType.MULTIPOLYGON);
                                String geomWKT = _currentProvincesDataMap.get("the_geom").toString();
                                propertiesMap.put(Constant._GeospatialGLGeometryContent,geomWKT);

                                GetSingleEntityTransformer getSingleEntityTransformer =
                                        new GetSingleEntityTransformer(Constant.GeospatialScaleProvinceEntityClass,workingGraphOperationExecutor);

                                String createGeospatialScaleEntitiesCql = CypherBuilder.createLabeledNodeWithProperties(conceptionTypeNameArray,propertiesMap);
                                Object newEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,createGeospatialScaleEntitiesCql);

                                if(newEntityRes != null && _CountryRegionEntityUID != null){
                                    Entity geospatialEntity = (Entity) newEntityRes;
                                    geospatialEntity.attachToRelation(_CountryRegionEntityUID, Constant.GeospatialScale_SpatialContainsRelationClass, null, true);
                                }
                            }
                            operatedItem.add(_3166_2Code);
                        }
                    }
                    reader.close();
                } catch (IOException | EngineServiceRuntimeException e) {
                    e.printStackTrace();
                } finally {
                    if (reader != null) {
                        try {
                            reader.close();
                        } catch (IOException e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    private static void generateGeospatialScaleEntities_ProvinceOfChina(GraphOperationExecutor workingGraphOperationExecutor, String geospatialRegionName) {
        Map<String,Map<String,Object>> _ChinaProvinceGISInfoMap = generateNE_10m_admin_states_provincesForChinaDataMap();
        String _ChinaGeospatialEntityUID = null;
        try {
            QueryParameters queryParameters = new QueryParameters();
            queryParameters.setResultNumber(1);
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialCodeProperty,"CN"));
            queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName), QueryParameters.FilteringLogic.AND);
            String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleCountryRegionEntityClass,queryParameters,null);
            GetSingleEntityTransformer getSingleEntityTransformer =
                    new GetSingleEntityTransformer(Constant.GeospatialScaleCountryRegionEntityClass,workingGraphOperationExecutor);

            Object geospatialEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,queryCql);
            if(geospatialEntityRes != null ){
                Entity geospatialEntity = (Entity) geospatialEntityRes;
                _ChinaGeospatialEntityUID = geospatialEntity.getEntityUID();
            }
        } catch (EngineServiceEntityExploreException e) {
            e.printStackTrace();
        }

        String[] conceptionTypeNameArray = new String[2];
        conceptionTypeNameArray[0] = Constant.GeospatialScaleEntityClass;
        conceptionTypeNameArray[1] = Constant.GeospatialScaleProvinceEntityClass;

        File file = new File(PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/"+"ChinaAdministrativeDivisionData(ISO_3166_2,GB_T_2260).txt");
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;

            while ((tempStr = reader.readLine()) != null) {
                String currentLine = !tempStr.startsWith("# DivisionCategory_EN")? tempStr : null;
                if(currentLine != null){
                    String[] administrativeDivisionInfoValueArray = currentLine.split("\\|");

                    Map<String,Object> propertiesMap = new HashMap<>();

                    String _DivisionCategory_EN = administrativeDivisionInfoValueArray[0].trim();
                    String _DivisionCategory_CH = administrativeDivisionInfoValueArray[1].trim();
                    String _ISO3166_2SubDivisionCode = administrativeDivisionInfoValueArray[2].trim();
                    String DivisionName_EN = administrativeDivisionInfoValueArray[3].trim();
                    String ChinaDivisionCode = administrativeDivisionInfoValueArray[4].trim();
                    String DivisionName_CH = administrativeDivisionInfoValueArray[5].trim();

                    propertiesMap.put("DivisionCategory_EN",_DivisionCategory_EN);
                    propertiesMap.put("DivisionCategory_CH",_DivisionCategory_CH);
                    propertiesMap.put("ISO3166_2SubDivisionCode",_ISO3166_2SubDivisionCode);
                    propertiesMap.put("ISO3166_1Alpha_2Code","CN");
                    propertiesMap.put("ChinaDivisionCode",ChinaDivisionCode);
                    propertiesMap.put(Constant.GeospatialEnglishNameProperty,DivisionName_EN);
                    propertiesMap.put(Constant.GeospatialChineseNameProperty,DivisionName_CH);
                    propertiesMap.put("Standard","GB/T 2260 | ISO 3166-2:2013");
                    propertiesMap.put("StandardStatus","Officially assigned");

                    propertiesMap.put(Constant.GeospatialCodeProperty,ChinaDivisionCode);
                    propertiesMap.put(Constant.GeospatialProperty,geospatialRegionName);
                    propertiesMap.put(Constant.GeospatialScaleGradeProperty, ""+Geospatial.GeospatialScaleGrade.PROVINCE);

                    GetSingleEntityTransformer getSingleEntityTransformer =
                            new GetSingleEntityTransformer(Constant.GeospatialScaleProvinceEntityClass,workingGraphOperationExecutor);

                    if(_ChinaProvinceGISInfoMap.get(ChinaDivisionCode) != null){
                        propertiesMap.put(Constant._GeospatialGeometryType,""+GeospatialScaleFeatureSupportable.WKTGeometryType.MULTIPOLYGON);
                        String geomWKT = _ChinaProvinceGISInfoMap.get(ChinaDivisionCode).get("the_geom").toString();
                        String poiPointWKT = "POINT ("+_ChinaProvinceGISInfoMap.get(ChinaDivisionCode).get("longitude")+" "+_ChinaProvinceGISInfoMap.get(ChinaDivisionCode).get("latitude")+")";
                        propertiesMap.put(Constant._GeospatialGLGeometryPOI,poiPointWKT);
                        propertiesMap.put(Constant._GeospatialGlobalCRSAID,"EPSG:4326"); // CRS EPSG:4326 - WGS 84 - Geographic
                        propertiesMap.put(Constant._GeospatialGLGeometryContent,geomWKT);
                        propertiesMap.put(Constant._GeospatialCLGeometryPOI,poiPointWKT);
                        propertiesMap.put(Constant._GeospatialCountryCRSAID,"EPSG:4490"); // CRS EPSG:4490 - CGCS2000 - Geographic
                        propertiesMap.put(Constant._GeospatialCLGeometryContent,geomWKT);
                    }

                    String createGeospatialScaleEntitiesCql = CypherBuilder.createLabeledNodeWithProperties(conceptionTypeNameArray,propertiesMap);
                    Object newEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,createGeospatialScaleEntitiesCql);

                    if(newEntityRes != null && _ChinaGeospatialEntityUID != null){
                        Entity geospatialEntity = (Entity) newEntityRes;
                        geospatialEntity.attachToRelation(_ChinaGeospatialEntityUID, Constant.GeospatialScale_SpatialContainsRelationClass, null, true);
                    }
                }
            }
            reader.close();
        } catch (IOException | EngineServiceRuntimeException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
    }

    private static void generateGeospatialScaleEntities_PrefectureAndLaterOfChina(GraphOperationExecutor workingGraphOperationExecutor, String geospatialRegionName){
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(100);
        queryParameters.setDefaultFilteringItem(new EqualFilteringItem("ISO3166_1Alpha_2Code","CN"));
        queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName), QueryParameters.FilteringLogic.AND);
        try {
            String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleProvinceEntityClass,queryParameters,null);
            GetListEntityTransformer getListEntityTransformer = new GetListEntityTransformer(Constant.GeospatialScaleProvinceEntityClass,workingGraphOperationExecutor);
            Object resultEntityList = workingGraphOperationExecutor.executeRead(getListEntityTransformer,queryCql);
            if(resultEntityList != null){
                List<Entity> resultContinentList =  (List<Entity>)resultEntityList;

                int degreeOfParallelism = Runtime.getRuntime().availableProcessors()/4 >4? Runtime.getRuntime().availableProcessors()/4 : 4;
                int singlePartitionSize = (resultContinentList.size()/degreeOfParallelism)+1;
                List<List<Entity>> rsList = Lists.partition(resultContinentList, singlePartitionSize);

                Map<String,String> _ChinaEntityWKTMap = generateChinaEntityWKTMap();
                ExecutorService executor = Executors.newFixedThreadPool(rsList.size());
                for(List<Entity> currentEntityValueList:rsList){
                    GeneratePrefectureAndLaterLevelEntitiesOfChinaThread generatePrefectureAndLaterLevelEntitiesOfChinaThread =
                            new GeneratePrefectureAndLaterLevelEntitiesOfChinaThread(currentEntityValueList,geospatialRegionName,_ChinaEntityWKTMap);
                    executor.execute(generatePrefectureAndLaterLevelEntitiesOfChinaThread);
                }
                executor.shutdown();
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            }
        } catch (EngineServiceEntityExploreException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void generatePrefectureAndLaterLevelEntitiesOfChina(Entity _ProvinceRegionEntity,
                                                                       String geospatialRegionName,GraphOperationExecutor workingGraphOperationExecutor,Map<String,String> _ChinaEntityWKTMap){
        String currentProvinceName = _ProvinceRegionEntity.getAttribute(Constant.GeospatialChineseNameProperty).getAttributeValue().toString();
        String[] conceptionTypeNameArray = new String[2];
        conceptionTypeNameArray[0] = Constant.GeospatialScaleEntityClass;

        String filePath =
                PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/ChinaData/China_DetailInfo(MinistryOfCivilAffairs)/"+currentProvinceName+".txt";
        File file = new File(filePath);
        if (file.exists()) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String tempStr;
                while ((tempStr = reader.readLine()) != null) {
                    String administrativeDivision_CodeInfoStr=tempStr.trim();
                    String[] codeInfoValueArray = administrativeDivision_CodeInfoStr.split(" ");
                    if(codeInfoValueArray.length ==3){
                        String firstPartCode = codeInfoValueArray[0].trim();
                        String secondPartCode = codeInfoValueArray[1].trim();
                        String administrativeDivisionContent = codeInfoValueArray[2];
                        String[] divisionNameArray = administrativeDivisionContent.split("-");
                        if(divisionNameArray.length == 1){
                            //length == 1 means is province level entity itself, ignore it
                        }else{
                            String _ChinaDivisionCode = null;
                            String _ChinaParentDivisionCode = null;
                            if(secondPartCode.equals("000000")){
                                _ChinaDivisionCode = firstPartCode;
                                if(_ChinaDivisionCode.endsWith("00")){
                                    _ChinaParentDivisionCode = firstPartCode.substring(0,2)+"0000";
                                }else {
                                    _ChinaParentDivisionCode = firstPartCode.substring(0, 4) + "00";
                                }
                            }else{
                                if(secondPartCode.endsWith("000")){
                                    _ChinaParentDivisionCode = firstPartCode;
                                    _ChinaDivisionCode = firstPartCode+secondPartCode;
                                    _ChinaDivisionCode = _ChinaDivisionCode.substring(0,9);
                                }else{
                                    _ChinaDivisionCode = firstPartCode+secondPartCode;
                                    _ChinaParentDivisionCode = _ChinaDivisionCode.substring(0,9);
                                }
                            }

                            String PROVINCE_Name = divisionNameArray[0].trim();
                            Map<String,Object> propertiesMap = new HashMap<>();

                            propertiesMap.put("ChinaParentDivisionCode",_ChinaParentDivisionCode);
                            propertiesMap.put("ChinaDivisionCode",_ChinaDivisionCode);
                            propertiesMap.put("Standard","GB/T 2260 | GB/T 10114");
                            propertiesMap.put("StandardStatus","Officially assigned");
                            propertiesMap.put("ChinaProvinceName",PROVINCE_Name);

                            propertiesMap.put(Constant.GeospatialCodeProperty,_ChinaDivisionCode);
                            propertiesMap.put(Constant.GeospatialProperty,geospatialRegionName);

                            if(divisionNameArray.length == 2){
                                conceptionTypeNameArray[1] = Constant.GeospatialScalePrefectureEntityClass;
                                propertiesMap.put(Constant.GeospatialScaleGradeProperty, ""+Geospatial.GeospatialScaleGrade.PREFECTURE);
                                String PREFECTURE_Name = divisionNameArray[1].trim();

                                propertiesMap.put("ChinaProvinceName",PROVINCE_Name);
                                propertiesMap.put(Constant.GeospatialChineseNameProperty,PREFECTURE_Name);
                            }else if(divisionNameArray.length == 3){
                                conceptionTypeNameArray[1] = Constant.GeospatialScaleCountyEntityClass;
                                propertiesMap.put(Constant.GeospatialScaleGradeProperty, ""+Geospatial.GeospatialScaleGrade.COUNTY);
                                String PREFECTURE_Name = divisionNameArray[1].trim();
                                String COUNTY_Name = divisionNameArray[2].trim();

                                propertiesMap.put("ChinaProvinceName",PROVINCE_Name);
                                propertiesMap.put("ChinaPrefectureName",PREFECTURE_Name);
                                propertiesMap.put(Constant.GeospatialChineseNameProperty,COUNTY_Name);
                            }else if(divisionNameArray.length == 4){
                                conceptionTypeNameArray[1] = Constant.GeospatialScaleTownshipEntityClass;
                                propertiesMap.put(Constant.GeospatialScaleGradeProperty, ""+Geospatial.GeospatialScaleGrade.TOWNSHIP);
                                String PREFECTURE_Name = divisionNameArray[1].trim();
                                String COUNTY_Name = divisionNameArray[2].trim();
                                String TOWNSHIP_Name = divisionNameArray[3].trim();

                                propertiesMap.put("ChinaProvinceName",PROVINCE_Name);
                                propertiesMap.put("ChinaPrefectureName",PREFECTURE_Name);
                                propertiesMap.put("ChinaCountyName",COUNTY_Name);
                                propertiesMap.put(Constant.GeospatialChineseNameProperty,TOWNSHIP_Name);
                            }else if(divisionNameArray.length == 5){
                                conceptionTypeNameArray[1] = Constant.GeospatialScaleVillageEntityClass;
                                propertiesMap.put(Constant.GeospatialScaleGradeProperty, ""+Geospatial.GeospatialScaleGrade.VILLAGE);
                                String PREFECTURE_Name = divisionNameArray[1].trim();
                                String COUNTY_Name = divisionNameArray[2].trim();
                                String TOWNSHIP_Name = divisionNameArray[3].trim();
                                String VILLAGE_Name = divisionNameArray[4].trim();

                                propertiesMap.put("ChinaProvinceName",PROVINCE_Name);
                                propertiesMap.put("ChinaPrefectureName",PREFECTURE_Name);
                                propertiesMap.put("ChinaCountyName",COUNTY_Name);
                                propertiesMap.put("ChinaTownshipName",TOWNSHIP_Name);
                                propertiesMap.put(Constant.GeospatialChineseNameProperty,VILLAGE_Name);
                            }
                            if(_ChinaEntityWKTMap.containsKey(_ChinaDivisionCode)){
                                propertiesMap.put(Constant._GeospatialGeometryType,""+GeospatialScaleFeatureSupportable.WKTGeometryType.MULTIPOLYGON);
                                propertiesMap.put(Constant._GeospatialGlobalCRSAID,"EPSG:4326"); // CRS EPSG:4326 - WGS 84 - Geographic
                                propertiesMap.put(Constant._GeospatialGLGeometryContent,_ChinaEntityWKTMap.get(_ChinaDivisionCode));
                                propertiesMap.put(Constant._GeospatialCountryCRSAID,"EPSG:4490"); // CRS EPSG:4490 - CGCS2000 - Geographic
                                propertiesMap.put(Constant._GeospatialCLGeometryContent,_ChinaEntityWKTMap.get(_ChinaDivisionCode));
                            }
                            if(_ChinaEntityWKTMap.containsKey(_ChinaDivisionCode+"_POINT")){
                                propertiesMap.put(Constant._GeospatialGLGeometryPOI,_ChinaEntityWKTMap.get(_ChinaDivisionCode+"_POINT"));
                                propertiesMap.put(Constant._GeospatialCLGeometryPOI,_ChinaEntityWKTMap.get(_ChinaDivisionCode+"_POINT"));
                            }
                            if(_ChinaEntityWKTMap.containsKey(_ChinaDivisionCode+"_BORDER")){
                                propertiesMap.put(Constant._GeospatialGLGeometryBorder,_ChinaEntityWKTMap.get(_ChinaDivisionCode+"_BORDER"));
                                propertiesMap.put(Constant._GeospatialCLGeometryBorder,_ChinaEntityWKTMap.get(_ChinaDivisionCode+"_BORDER"));
                            }
                            GetSingleEntityTransformer getSingleEntityTransformer =
                                    new GetSingleEntityTransformer(Constant.GeospatialScaleEntityClass,workingGraphOperationExecutor);
                            String createGeospatialScaleEntitiesCql = CypherBuilder.createLabeledNodeWithProperties(conceptionTypeNameArray,propertiesMap);
                            workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,createGeospatialScaleEntitiesCql);
                        }
                    }
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }
    }

    private static void linkGeospatialScaleEntitiesOfChina(GraphOperationExecutor workingGraphOperationExecutor, String geospatialRegionName){
        try {
            Map<String,String> _ChinaDivisionCodeAndEntityUIDMap = new HashMap<>();

            //get Province level entities info
            QueryParameters queryParameters = new QueryParameters();
            queryParameters.setResultNumber(100);
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem("ISO3166_1Alpha_2Code","CN"));
            queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName), QueryParameters.FilteringLogic.AND);

            List<String> attributeNamesList = new ArrayList<>();
            attributeNamesList.add(Constant.GeospatialCodeProperty);
            List<AttributeKind> attributeKindList = new ArrayList<>();
            attributeKindList.add(new Neo4JAttributeImpl(null, Constant.GeospatialCodeProperty,null, AttributeDataType.STRING,null));
            String queryCql = CypherBuilder.matchAttributesWithQueryParameters(Constant.GeospatialScaleProvinceEntityClass,queryParameters,attributeNamesList);

            GetListEntityValueTransformer getListEntityValueTransformer =
                    new GetListEntityValueTransformer(attributeNamesList,attributeKindList);
            Object resEntityRes = workingGraphOperationExecutor.executeRead(getListEntityValueTransformer, queryCql);

            if(resEntityRes != null){
                List<EntityValue> resultEntitiesValues = (List<EntityValue>)resEntityRes;
                for(EntityValue currentEntityValue : resultEntitiesValues){
                    _ChinaDivisionCodeAndEntityUIDMap.put(currentEntityValue.getEntityAttributesValue().get(Constant.GeospatialCodeProperty).toString(),
                            currentEntityValue.getEntityUID());
                }
            }

            queryParameters = new QueryParameters();
            queryParameters.setResultNumber(1000000);
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName));
            attributeNamesList.add("ChinaParentDivisionCode");
            attributeKindList.add(new Neo4JAttributeImpl(null,"ChinaParentDivisionCode",null, AttributeDataType.STRING,null));
            getListEntityValueTransformer =  new GetListEntityValueTransformer(attributeNamesList,attributeKindList);

            List<DivisionCodeInfo> divisionCodeInfoList = new ArrayList<>();

            queryCql = CypherBuilder.matchAttributesWithQueryParameters(Constant.GeospatialScalePrefectureEntityClass,queryParameters,attributeNamesList);
            loadGeospatialScaleEntitiesOfChina(getListEntityValueTransformer,queryCql,workingGraphOperationExecutor,divisionCodeInfoList,_ChinaDivisionCodeAndEntityUIDMap);

            queryCql = CypherBuilder.matchAttributesWithQueryParameters(Constant.GeospatialScaleCountyEntityClass,queryParameters,attributeNamesList);
            loadGeospatialScaleEntitiesOfChina(getListEntityValueTransformer,queryCql,workingGraphOperationExecutor,divisionCodeInfoList,_ChinaDivisionCodeAndEntityUIDMap);

            queryCql = CypherBuilder.matchAttributesWithQueryParameters(Constant.GeospatialScaleTownshipEntityClass,queryParameters,attributeNamesList);
            loadGeospatialScaleEntitiesOfChina(getListEntityValueTransformer,queryCql,workingGraphOperationExecutor,divisionCodeInfoList,_ChinaDivisionCodeAndEntityUIDMap);

            queryCql = CypherBuilder.matchAttributesWithQueryParameters(Constant.GeospatialScaleVillageEntityClass,queryParameters,attributeNamesList);
            loadGeospatialScaleEntitiesOfChina(getListEntityValueTransformer,queryCql,workingGraphOperationExecutor,divisionCodeInfoList,_ChinaDivisionCodeAndEntityUIDMap);

            int degreeOfParallelism = Runtime.getRuntime().availableProcessors()/4 >4? Runtime.getRuntime().availableProcessors()/4 : 4;
            int singlePartitionSize = (divisionCodeInfoList.size()/degreeOfParallelism)+1;
            List<List<DivisionCodeInfo>> rsList = Lists.partition(divisionCodeInfoList, singlePartitionSize);

            ExecutorService executor = Executors.newFixedThreadPool(rsList.size());
            for(List<DivisionCodeInfo> currentEntityValueList:rsList){
                LinkGeospatialScaleEntityThread linkGeospatialScaleEntityThread = new LinkGeospatialScaleEntityThread(currentEntityValueList);
                executor.execute(linkGeospatialScaleEntityThread);
            }
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (EngineServiceEntityExploreException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class LinkGeospatialScaleEntityThread implements Runnable{

        private List<DivisionCodeInfo> divisionCodeInfoList;
        public LinkGeospatialScaleEntityThread(List<DivisionCodeInfo> divisionCodeInfoList){
            this.divisionCodeInfoList = divisionCodeInfoList;
        }

        @Override
        public void run() {
            GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();

            GetSingleRelationshipEntityTransformer getSingleRelationshipEntityTransformer = new GetSingleRelationshipEntityTransformer
                    (Constant.GeospatialScale_SpatialContainsRelationClass,graphOperationExecutor);
            for(DivisionCodeInfo currentDivisionCodeInfo:divisionCodeInfoList){
                if(currentDivisionCodeInfo.getDivisionCode() !=null &&
                        currentDivisionCodeInfo.getDivisionEntityUID() !=null &&
                        currentDivisionCodeInfo.getParentDivisionEntityUID() !=null &&
                        currentDivisionCodeInfo.getParentDivisionCode() !=null){
                    Map<String,Object> relationPropertiesMap = new HashMap<>();
                    String linkToTimeScaleEntityCql = CypherBuilder.createNodesRelationshipByIdMatch(Long.parseLong(currentDivisionCodeInfo.getParentDivisionEntityUID()),Long.parseLong(currentDivisionCodeInfo.getDivisionEntityUID()), Constant.GeospatialScale_SpatialContainsRelationClass,relationPropertiesMap);
                    graphOperationExecutor.executeWrite(getSingleRelationshipEntityTransformer, linkToTimeScaleEntityCql);
                }
            }
            graphOperationExecutor.close();
        }
    }

    private static class GeneratePrefectureAndLaterLevelEntitiesOfChinaThread implements Runnable{

        private List<Entity> _ChinaProvinceEntityList;
        private String geospatialRegionName;
        private Map<String,String> _ChinaEntityWKTMap;
        public GeneratePrefectureAndLaterLevelEntitiesOfChinaThread(List<Entity> _ChinaProvinceEntityList,String geospatialRegionName,Map<String,String> _ChinaEntityWKTMap){
            this._ChinaProvinceEntityList = _ChinaProvinceEntityList;
            this.geospatialRegionName = geospatialRegionName;
            this._ChinaEntityWKTMap = _ChinaEntityWKTMap;
        }

        @Override
        public void run() {
            GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
            for(Entity currentEntity : this._ChinaProvinceEntityList){
                generatePrefectureAndLaterLevelEntitiesOfChina(currentEntity,this.geospatialRegionName,graphOperationExecutor,this._ChinaEntityWKTMap);
            }
            graphOperationExecutor.close();
        }
    }

    private static void linkSpecialAdministrativeRegionEntitiesOfChina(GraphOperationExecutor workingGraphOperationExecutor, String geospatialRegionName){
        try {
            Entity geospatialEntity_MO_R = null;
            Entity geospatialEntity_TW_R = null;
            Entity geospatialEntity_HK_R = null;
            Entity geospatialEntity_MO_P = null;
            Entity geospatialEntity_TW_P = null;
            Entity geospatialEntity_HK_P = null;

            GetSingleEntityTransformer getSingleEntityTransformer =
                    new GetSingleEntityTransformer(Constant.GeospatialScaleCountryRegionEntityClass,workingGraphOperationExecutor);

            QueryParameters queryParameters = new QueryParameters();
            queryParameters.setResultNumber(1);
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialCodeProperty,"MO"));
            queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName), QueryParameters.FilteringLogic.AND);
            String queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleCountryRegionEntityClass,queryParameters,null);
            Object geospatialEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,queryCql);
            if(geospatialEntityRes != null ){
                geospatialEntity_MO_R = (Entity) geospatialEntityRes;
            }

            queryParameters = new QueryParameters();
            queryParameters.setResultNumber(1);
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialCodeProperty,"TW"));
            queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName), QueryParameters.FilteringLogic.AND);
            queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleCountryRegionEntityClass,queryParameters,null);
            geospatialEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,queryCql);
            if(geospatialEntityRes != null ){
                geospatialEntity_TW_R = (Entity) geospatialEntityRes;
            }

            queryParameters = new QueryParameters();
            queryParameters.setResultNumber(1);
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem(Constant.GeospatialCodeProperty,"HK"));
            queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName), QueryParameters.FilteringLogic.AND);
            queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleCountryRegionEntityClass,queryParameters,null);
            geospatialEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,queryCql);
            if(geospatialEntityRes != null ){
                geospatialEntity_HK_R = (Entity) geospatialEntityRes;
            }

            getSingleEntityTransformer =
                    new GetSingleEntityTransformer(Constant.GeospatialScaleProvinceEntityClass,workingGraphOperationExecutor);

            queryParameters = new QueryParameters();
            queryParameters.setResultNumber(1);
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem("ISO3166_2SubDivisionCode","CN-TW"));
            queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName), QueryParameters.FilteringLogic.AND);
            queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleProvinceEntityClass,queryParameters,null);
            geospatialEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,queryCql);
            if(geospatialEntityRes != null ){
                geospatialEntity_TW_P = (Entity) geospatialEntityRes;
            }

            queryParameters = new QueryParameters();
            queryParameters.setResultNumber(1);
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem("ISO3166_2SubDivisionCode","CN-HK"));
            queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName), QueryParameters.FilteringLogic.AND);
            queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleProvinceEntityClass,queryParameters,null);
            geospatialEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,queryCql);
            if(geospatialEntityRes != null ){
                geospatialEntity_HK_P = (Entity) geospatialEntityRes;
            }

            queryParameters = new QueryParameters();
            queryParameters.setResultNumber(1);
            queryParameters.setDefaultFilteringItem(new EqualFilteringItem("ISO3166_2SubDivisionCode","CN-MO"));
            queryParameters.addFilteringItem(new EqualFilteringItem(Constant.GeospatialProperty,geospatialRegionName), QueryParameters.FilteringLogic.AND);
            queryCql = CypherBuilder.matchNodesWithQueryParameters(Constant.GeospatialScaleProvinceEntityClass,queryParameters,null);
            geospatialEntityRes = workingGraphOperationExecutor.executeWrite(getSingleEntityTransformer,queryCql);
            if(geospatialEntityRes != null ){
                geospatialEntity_MO_P = (Entity) geospatialEntityRes;
            }

            if(geospatialEntity_MO_R != null && geospatialEntity_MO_P != null){
                geospatialEntity_MO_R.attachToRelation(geospatialEntity_MO_P.getEntityUID(), Constant.GeospatialScale_SpatialIdenticalRelationClass,null,false);
            }
            if(geospatialEntity_HK_R != null && geospatialEntity_HK_P != null){
                geospatialEntity_HK_R.attachToRelation(geospatialEntity_HK_P.getEntityUID(), Constant.GeospatialScale_SpatialIdenticalRelationClass,null,false);
            }
            if(geospatialEntity_TW_R != null && geospatialEntity_TW_P != null){
                geospatialEntity_TW_R.attachToRelation(geospatialEntity_TW_P.getEntityUID(), Constant.GeospatialScale_SpatialIdenticalRelationClass,null,false);
            }
        } catch (EngineServiceEntityExploreException | EngineServiceRuntimeException e) {
            e.printStackTrace();
        }
    }

    private static void loadGeospatialScaleEntitiesOfChina(GetListEntityValueTransformer getListEntityValueTransformer,
                                                           String queryCql, GraphOperationExecutor workingGraphOperationExecutor,
                                                           List<DivisionCodeInfo> divisionCodeInfoList, Map<String,String> _ChinaDivisionCodeAndEntityUIDMap){
        Object resEntityRes = workingGraphOperationExecutor.executeRead(getListEntityValueTransformer, queryCql);
        if(resEntityRes != null){
            List<EntityValue> resultEntitiesValues = (List<EntityValue>)resEntityRes;
            for(EntityValue currentEntityValue : resultEntitiesValues){
                _ChinaDivisionCodeAndEntityUIDMap.put(currentEntityValue.getEntityAttributesValue().get(Constant.GeospatialCodeProperty).toString(),
                        currentEntityValue.getEntityUID());
                divisionCodeInfoList.add(new DivisionCodeInfo(
                        currentEntityValue.getEntityAttributesValue().get(Constant.GeospatialCodeProperty).toString(),
                        currentEntityValue.getEntityUID(),
                        currentEntityValue.getEntityAttributesValue().get("ChinaParentDivisionCode").toString(),
                        _ChinaDivisionCodeAndEntityUIDMap.get(currentEntityValue.getEntityAttributesValue().get("ChinaParentDivisionCode").toString())
                ));
            }
        }
    }

    private static Map<String,Map<String,Object>> generateNE_10m_admin_states_provincesDataMap(){
        Map<String,Map<String,Object>> _ProvincesISO_3166_2DataMap = new HashMap<>();
        String filePath =
                PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/statesAndProvinces/ne_10m_admin_1_states_provinces/"+"ne_10m_admin_1_states_provinces.shp";
        SimpleFeatureCollection colls = readShp(filePath,null);
        SimpleFeatureIterator iters = colls.features();

        while(iters.hasNext()){
            SimpleFeature sf = iters.next();
            Map<String,Object> _ISO_3166_2Data = new HashMap<>();
            String iso_3166_2Code = sf.getAttribute("iso_3166_2").toString();
            iso_3166_2Code = iso_3166_2Code.replace("~","");
            _ProvincesISO_3166_2DataMap.put(iso_3166_2Code,_ISO_3166_2Data);

            _ISO_3166_2Data.put("the_geom",sf.getAttribute("the_geom"));
            _ISO_3166_2Data.put("name_en",sf.getAttribute("name_en"));
            _ISO_3166_2Data.put("name_zh",sf.getAttribute("name_zh"));
            _ISO_3166_2Data.put("gns_name",sf.getAttribute("gns_name"));
            _ISO_3166_2Data.put("geonunit",sf.getAttribute("geonunit"));
            _ISO_3166_2Data.put("latitude",sf.getAttribute("latitude"));
            _ISO_3166_2Data.put("longitude",sf.getAttribute("longitude"));
            _ISO_3166_2Data.put("iso_a2",sf.getAttribute("iso_a2"));
            _ISO_3166_2Data.put("name_local",sf.getAttribute("name_local"));
            _ISO_3166_2Data.put("type",sf.getAttribute("type"));
            _ISO_3166_2Data.put("type_en",sf.getAttribute("type_en"));
            _ISO_3166_2Data.put("gn_name",sf.getAttribute("gn_name"));
            _ISO_3166_2Data.put("woe_label",sf.getAttribute("woe_label"));
            _ISO_3166_2Data.put("woe_name",sf.getAttribute("woe_name"));
        }
        return _ProvincesISO_3166_2DataMap;
    }

    private static Map<String,Map<String,Object>> generateNE_10m_CountriesDataMap(){
        Map<String,Map<String,Object>> _NE_10m_CountriesDataMap = new HashMap<>();
        String filePath =
                PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/statesAndProvinces/ne_10m_admin_0_countries_modified/"+"ne_10m_admin_0_countries_modified.shp";
        SimpleFeatureCollection colls = readShp(filePath,null);
        SimpleFeatureIterator iters = colls.features();

        while(iters.hasNext()){
            SimpleFeature sf = iters.next();
            Map<String,Object> _ISO_3166_1Data = new HashMap<>();
            String iso_3166_1Code = sf.getAttribute("ISO_A2").toString();
            _ISO_3166_1Data.put("the_geom",sf.getAttribute("the_geom"));
            if(!iso_3166_1Code.equals("-99")){
                _NE_10m_CountriesDataMap.put(iso_3166_1Code,_ISO_3166_1Data);
            }else{
                String countryName = sf.getAttribute("ADMIN").toString();
                if(countryName.equals("France")){
                    _NE_10m_CountriesDataMap.put("FR",_ISO_3166_1Data);
                }else if(countryName.equals("Norway")){
                    _NE_10m_CountriesDataMap.put("NO",_ISO_3166_1Data);
                }
            }
        }
        return _NE_10m_CountriesDataMap;
    }

    private static Map<String,Map<String,Object>> generateNE_10m_admin_states_provincesForChinaDataMap(){
        Map<String,Map<String,Object>> _ProvincesISO_3166_2DataMap = new HashMap<>();
        String filePath =
                PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/statesAndProvinces/ne_10m_admin_1_states_provinces_modifiedForChina/"+"ne_10m_admin_1_states_provinces.shp";
        SimpleFeatureCollection colls = readShp(filePath,null);
        SimpleFeatureIterator iters = colls.features();

        Map<String,String> ne_ChinaCodeMapping = new HashMap<>();
        ne_ChinaCodeMapping.put("CN-11","110000");
        ne_ChinaCodeMapping.put("CN-12","120000");
        ne_ChinaCodeMapping.put("CN-13","130000");
        ne_ChinaCodeMapping.put("CN-14","140000");
        ne_ChinaCodeMapping.put("CN-15","150000");
        ne_ChinaCodeMapping.put("CN-21","210000");
        ne_ChinaCodeMapping.put("CN-22","220000");
        ne_ChinaCodeMapping.put("CN-23","230000");
        ne_ChinaCodeMapping.put("CN-31","310000");
        ne_ChinaCodeMapping.put("CN-32","320000");
        ne_ChinaCodeMapping.put("CN-33","330000");
        ne_ChinaCodeMapping.put("CN-34","340000");
        ne_ChinaCodeMapping.put("CN-35","350000");
        ne_ChinaCodeMapping.put("CN-36","360000");
        ne_ChinaCodeMapping.put("CN-37","370000");
        ne_ChinaCodeMapping.put("CN-41","410000");
        ne_ChinaCodeMapping.put("CN-42","420000");
        ne_ChinaCodeMapping.put("CN-43","430000");
        ne_ChinaCodeMapping.put("CN-44","440000");
        ne_ChinaCodeMapping.put("CN-45","450000");
        ne_ChinaCodeMapping.put("CN-46","460000");
        ne_ChinaCodeMapping.put("CN-50","500000");
        ne_ChinaCodeMapping.put("CN-51","510000");
        ne_ChinaCodeMapping.put("CN-52","520000");
        ne_ChinaCodeMapping.put("CN-53","530000");
        ne_ChinaCodeMapping.put("CN-54","540000");
        ne_ChinaCodeMapping.put("CN-61","610000");
        ne_ChinaCodeMapping.put("CN-62","620000");
        ne_ChinaCodeMapping.put("CN-63","630000");
        ne_ChinaCodeMapping.put("CN-64","640000");
        ne_ChinaCodeMapping.put("CN-65","650000");

        ne_ChinaCodeMapping.put("HK-X01~", "810101");
        ne_ChinaCodeMapping.put("HK-X02~", "810102");
        ne_ChinaCodeMapping.put("HK-X03~", "810103");
        ne_ChinaCodeMapping.put("HK-X04~", "810104");
        ne_ChinaCodeMapping.put("HK-X05~", "810201");
        ne_ChinaCodeMapping.put("HK-X06~", "810203");
        ne_ChinaCodeMapping.put("HK-X07~", "810202");
        ne_ChinaCodeMapping.put("HK-X08~", "810204");
        ne_ChinaCodeMapping.put("HK-X09~", "810205");
        ne_ChinaCodeMapping.put("HK-X10~", "810306");
        ne_ChinaCodeMapping.put("HK-X11~", "810307");
        ne_ChinaCodeMapping.put("HK-X12~", "810308");
        ne_ChinaCodeMapping.put("HK-X13~", "810301");
        ne_ChinaCodeMapping.put("HK-X14~", "810302");
        ne_ChinaCodeMapping.put("HK-X15~", "810303");
        ne_ChinaCodeMapping.put("HK-X16~", "810304");
        ne_ChinaCodeMapping.put("HK-X17~", "810305");
        ne_ChinaCodeMapping.put("HK-X18~", "810309");

        ne_ChinaCodeMapping.put("TW-CHA", "712700");
        ne_ChinaCodeMapping.put("TW-CYI", "710700");
        ne_ChinaCodeMapping.put("TW-CYQ", "713000");
        ne_ChinaCodeMapping.put("TW-HSQ", "712400");
        ne_ChinaCodeMapping.put("TW-HSZ", "710600");
        ne_ChinaCodeMapping.put("TW-HUA", "713500");
        ne_ChinaCodeMapping.put("TW-ILA", "712200");
        ne_ChinaCodeMapping.put("TW-KEE", "710300");
        ne_ChinaCodeMapping.put("TW-KHH", "710200");
        ne_ChinaCodeMapping.put("TW-KIN", "713700");
        ne_ChinaCodeMapping.put("TW-MIA", "712500");
        ne_ChinaCodeMapping.put("TW-NAN", "712800");
        ne_ChinaCodeMapping.put("TW-PEN", "713600");
        ne_ChinaCodeMapping.put("TW-PIF", "713300");
        ne_ChinaCodeMapping.put("TW_TAO", "712300");
        ne_ChinaCodeMapping.put("TW_TNN", "710500");
        ne_ChinaCodeMapping.put("TW-TPE", "710100");
        ne_ChinaCodeMapping.put("TW-TPQ", "710800");
        ne_ChinaCodeMapping.put("TW-TTT", "713400");
        ne_ChinaCodeMapping.put("TW-TXG", "710400");
        ne_ChinaCodeMapping.put("TW-YUN", "712900");

        ne_ChinaCodeMapping.put("MO-M", "820000");

        while(iters.hasNext()){
            SimpleFeature sf = iters.next();
            Map<String,Object> _ISO_3166_2Data = new HashMap<>();
            String iso_3166_2Code = sf.getAttribute("iso_3166_2").toString();
            if(ne_ChinaCodeMapping.containsKey(iso_3166_2Code)){
                _ProvincesISO_3166_2DataMap.put(ne_ChinaCodeMapping.get(iso_3166_2Code),_ISO_3166_2Data);
                _ISO_3166_2Data.put("the_geom",sf.getAttribute("the_geom"));
                _ISO_3166_2Data.put("name_en",sf.getAttribute("name_en"));
                _ISO_3166_2Data.put("name_zh",sf.getAttribute("name_zh"));
                _ISO_3166_2Data.put("gns_name",sf.getAttribute("gns_name"));
                _ISO_3166_2Data.put("geonunit",sf.getAttribute("geonunit"));
                _ISO_3166_2Data.put("latitude",sf.getAttribute("latitude"));
                _ISO_3166_2Data.put("longitude",sf.getAttribute("longitude"));
                _ISO_3166_2Data.put("iso_a2",sf.getAttribute("iso_a2"));
                _ISO_3166_2Data.put("name_local",sf.getAttribute("name_local"));
                _ISO_3166_2Data.put("type",sf.getAttribute("type"));
                _ISO_3166_2Data.put("type_en",sf.getAttribute("type_en"));
                _ISO_3166_2Data.put("gn_name",sf.getAttribute("gn_name"));
                _ISO_3166_2Data.put("woe_label",sf.getAttribute("woe_label"));
                _ISO_3166_2Data.put("woe_name",sf.getAttribute("woe_name"));
            }
        }
        return _ProvincesISO_3166_2DataMap;
    }

    private static SimpleFeatureCollection  readShp(String path , Filter filter){
        SimpleFeatureSource featureSource = readStoreByShp(path);
        if(featureSource == null){
            return null;
        }
        try {
            return filter != null ? featureSource.getFeatures(filter) : featureSource.getFeatures() ;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null ;
    }

    private static  SimpleFeatureSource readStoreByShp(String path ){
        File file = new File(path);
        FileDataStore store;
        SimpleFeatureSource featureSource = null;
        try {
            store = FileDataStoreFinder.getDataStore(file);
            ((ShapefileDataStore) store).setCharset(Charset.forName("UTF-8"));
            featureSource = store.getFeatureSource();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return featureSource;
    }

    private static Map<String,String> generateChinaEntityWKTMap(){
        Map<String,String> _ChinaEntityWKTMap = new HashMap<>();
        generateChina_PrefectureEntityWKTMap(_ChinaEntityWKTMap);
        generateChina_CountyEntityWKTMap(_ChinaEntityWKTMap);
        generateChina_TownshipEntityWKTMap(_ChinaEntityWKTMap);
        String filePath = PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/ChinaData/China_GISInfo_Point_Border.txt";
        File file = new File(filePath);
        if (file.exists()) {
            BufferedReader reader = null;
            try {
                reader = new BufferedReader(new FileReader(file));
                String tempStr;
                while ((tempStr = reader.readLine()) != null) {
                    String _ChinaEntityWKTStr = tempStr.trim();
                    String[] wktDataValueArray = _ChinaEntityWKTStr.split("-");
                    String entityGeospatialCode = wktDataValueArray[0];
                    String entityPoint = wktDataValueArray[2];
                    String entityBorder = wktDataValueArray[3];
                    _ChinaEntityWKTMap.put(entityGeospatialCode.trim()+"_POINT",entityPoint);
                    _ChinaEntityWKTMap.put(entityGeospatialCode.trim()+"_BORDER",entityBorder);
                }
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }
        return _ChinaEntityWKTMap;
    }

    private static class DivisionCodeInfo {
        private String divisionEntityUID;
        private String parentDivisionEntityUID;
        private String parentDivisionCode;
        private String divisionCode;

        DivisionCodeInfo(String divisionCode, String divisionEntityUID, String parentDivisionCode,String parentDivisionEntityUID) {
            this.divisionCode = divisionCode;
            this.divisionEntityUID = divisionEntityUID;
            this.parentDivisionCode = parentDivisionCode;
            this.parentDivisionEntityUID = parentDivisionEntityUID;
        }

        public String getDivisionEntityUID() {
            return divisionEntityUID;
        }

        public String getParentDivisionEntityUID() {
            return parentDivisionEntityUID;
        }

        public String getParentDivisionCode() {
            return parentDivisionCode;
        }

        public String getDivisionCode() {
            return divisionCode;
        }
    }

    private static void generateChina_PrefectureEntityWKTMap(Map<String,String> entityWKTMap){
        String filePath =
                PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/ChinaData/China_Prefecture_shp/"+"China_Prefecture.shp";
        SimpleFeatureCollection colls = readShp(filePath,null);
        SimpleFeatureIterator iters = colls.features();
        while(iters.hasNext()){
            SimpleFeature sf = iters.next();
            String prefectureCode = sf.getAttribute("市代码").toString();
            String prefectureWKT = sf.getAttribute("the_geom").toString();
            entityWKTMap.put(prefectureCode.trim(),prefectureWKT);
        }

    }

    private static void generateChina_CountyEntityWKTMap(Map<String,String> entityWKTMap){
        String filePath =
                PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/ChinaData/China_County_shp/"+"China_County.shp";
        SimpleFeatureCollection colls = readShp(filePath,null);
        SimpleFeatureIterator iters = colls.features();
        while(iters.hasNext()){
            SimpleFeature sf = iters.next();
            String prefectureCode = sf.getAttribute("PAC").toString();
            String prefectureWKT = sf.getAttribute("the_geom").toString();
            entityWKTMap.put(prefectureCode.trim(),prefectureWKT);
        }
    }

    private static void generateChina_TownshipEntityWKTMap(Map<String,String> entityWKTMap){
        String filePath =
                PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/ChinaData/China_Township_shp/"+"我国乡镇行政区划.shp";
        File shpFile = new File(filePath);
        if(shpFile.exists()){
            String folderPath =
                    PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+GEOSPATIAL_DATA_FOLDER+"/ChinaData/China_DetailInfo(MinistryOfCivilAffairs)/";
            File folder = new File(folderPath);

            if(folder.exists() && folder.isDirectory()){
                Map<String,String> _ChinaDivisionCodeMap = new HashMap<>();
                Map<String,String> _ChinaTownshipDivisionCodeMap = new HashMap<>();
                Map<String,Integer> _ChinaTownshipDupCountMap = new HashMap<>() ;

                File[] ministryOfCivilAffairsFiles = folder.listFiles();
                for(File currentFile:ministryOfCivilAffairsFiles){
                    BufferedReader reader = null;
                    try {
                        reader = new BufferedReader(new FileReader(currentFile));
                        String tempStr;
                        while ((tempStr = reader.readLine()) != null) {
                            String administrativeDivision_CodeInfoStr=tempStr.trim();
                            String[] codeInfoValueArray = administrativeDivision_CodeInfoStr.split(" ");
                            if(codeInfoValueArray.length ==3){
                                String firstPartCode = codeInfoValueArray[0].trim();
                                String secondPartCode = codeInfoValueArray[1].trim();
                                String administrativeDivisionContent = codeInfoValueArray[2];
                                String[] divisionNameArray = administrativeDivisionContent.split("-");
                                if(divisionNameArray.length == 1){
                                    //length == 1 means is province level entity itself, ignore it
                                }else{
                                    String _ChinaDivisionCode = null;

                                    if(secondPartCode.equals("000000")){
                                        _ChinaDivisionCode = firstPartCode;
                                    }else{
                                        if(secondPartCode.endsWith("000")){
                                            _ChinaDivisionCode = firstPartCode+secondPartCode;
                                            _ChinaDivisionCode = _ChinaDivisionCode.substring(0,9);
                                        }else{
                                            _ChinaDivisionCode = firstPartCode+secondPartCode;
                                        }
                                    }

                                    if(divisionNameArray.length == 4){
                                        //String PREFECTURE_Name = divisionNameArray[1].trim();
                                        String COUNTY_Name = divisionNameArray[2].trim();
                                        String TOWNSHIP_Name = divisionNameArray[3].trim();
                                        //String townshipKey = PREFECTURE_Name+"_"+COUNTY_Name+"_"+TOWNSHIP_Name;
                                        String townshipKey = COUNTY_Name+"_"+TOWNSHIP_Name;

                                        _ChinaDivisionCodeMap.put(townshipKey,_ChinaDivisionCode);
                                        _ChinaTownshipDivisionCodeMap.put(TOWNSHIP_Name,_ChinaDivisionCode);

                                        if(_ChinaTownshipDupCountMap.get(TOWNSHIP_Name)!=null){
                                            _ChinaTownshipDupCountMap.put(TOWNSHIP_Name,_ChinaTownshipDupCountMap.get(TOWNSHIP_Name)+1);
                                        }else{
                                            _ChinaTownshipDupCountMap.put(TOWNSHIP_Name,1);
                                        }
                                    }
                                }
                            }
                        }
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (IOException e1) {
                                e1.printStackTrace();
                            }
                        }
                    }
                }

                //remove ChinaTownshipDivisionCode if there are more than one Township name appears
                for(String currentChinaTownshipName:_ChinaTownshipDupCountMap.keySet()){
                    if(_ChinaTownshipDupCountMap.get(currentChinaTownshipName)>1){
                        _ChinaTownshipDivisionCodeMap.remove(currentChinaTownshipName);
                    }
                }

                // get WKT from SHP
                SimpleFeatureCollection colls = readShp(filePath,null);
                SimpleFeatureIterator iters = colls.features();
                while(iters.hasNext()){
                    SimpleFeature sf = iters.next();
                    //String provinceName = sf.getAttribute("省").toString();
                    //String prefectureName = sf.getAttribute("市").toString();
                    String countyName = sf.getAttribute("县").toString();
                    String townshipName = sf.getAttribute("乡").toString();
                    String townshipWKT = sf.getAttribute("the_geom").toString();
                    //String townshipKey = prefectureCode+"_"+countyCode+"_"+townshipCode;
                    String townshipKey = countyName+"_"+townshipName;
                    if(_ChinaDivisionCodeMap.get(townshipKey) != null){
                        entityWKTMap.put(_ChinaDivisionCodeMap.get(townshipKey),townshipWKT);
                    }
                }
            }
        }
    }
}
