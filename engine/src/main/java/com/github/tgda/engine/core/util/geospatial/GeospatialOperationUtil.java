package com.github.tgda.engine.core.util.geospatial;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.util.BatchDataOperationUtil;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.Constant;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.referencing.CRS;
import org.opengis.feature.GeometryAttribute;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeospatialOperationUtil {

    public static void importSHPDataDirectlyToConceptionKind(String conceptionKindName, boolean removeExistData, File shpFile, String fileEncode) throws IOException, EngineServiceRuntimeException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        String charsetEncode = fileEncode != null ?  fileEncode : "UTF-8";
        // 读取到数据存储中
        FileDataStore dataStore = FileDataStoreFinder.getDataStore(shpFile);
        ((ShapefileDataStore) dataStore).setCharset(Charset.forName(charsetEncode));
        // 获取特征资源
        SimpleFeatureSource simpleFeatureSource = dataStore.getFeatureSource();
        String conceptionKindNameValue = conceptionKindName != null ? conceptionKindName : simpleFeatureSource.getName().toString();
        SimpleFeatureType simpleFeatureType = dataStore.getSchema();
        CoordinateReferenceSystem crs = simpleFeatureType.getCoordinateReferenceSystem();

        String _CRSName = "";
        if (crs != null) {
            _CRSName = crs.getName().getCode();
        }
        String entityCRSAID = null;
        String _CRS_Range = null;
        if("GCS_WGS_1984".equals(_CRSName)||_CRSName.contains("WGS84")){
            entityCRSAID= "EPSG:4326";
            _CRS_Range = "GlobalLevel";
        }else if("CGCS_2000".equals(_CRSName)||_CRSName.contains("CGCS2000")){
            entityCRSAID= "EPSG:4545";
            _CRS_Range = "CountryLevel";
        }else{
            _CRS_Range = "LocalLevel";
            Integer _EpsgCodeValue = null;
            try {
                if (crs!=null) {
                    _EpsgCodeValue = CRS.lookupEpsgCode(crs,true);
                }
            } catch (FactoryException e) {
                EngineServiceRuntimeException engineServiceRuntimeException = new EngineServiceRuntimeException();
                engineServiceRuntimeException.setCauseMessage(e.getMessage());
                throw engineServiceRuntimeException;
            }
            if(_EpsgCodeValue != null){
                entityCRSAID= "EPSG:"+_EpsgCodeValue.intValue();
            }
        }
        // 要素集合
        SimpleFeatureCollection simpleFeatureCollection = simpleFeatureSource.getFeatures();
        List<EntityValue> _targetEntityValueList = Lists.newArrayList();
        // 获取要素迭代器
        SimpleFeatureIterator featureIterator = simpleFeatureCollection.features();
        int idx =0;
        while(featureIterator.hasNext()){
            Map<String,Object> newEntityValueMap = new HashMap<>();
            // 要素对象
            SimpleFeature feature = featureIterator.next();
            // 要素属性信息，名称，值，类型
            List<Property> propertyList = (List<Property>) feature.getValue();
            for(Property property : propertyList){
                String propertyName = property.getName().toString();
                if(!propertyName.equals("the_geom")){
                    //handle invalid chars
                    propertyName = propertyName.replaceAll("△","Delta_");
                    Object propertyValue = property.getValue();
                    if(propertyValue != null && validatePropertyValue(coreRealm,propertyValue)){
                        newEntityValueMap.put(propertyName,propertyValue);
                    }
                }
            }

            if(feature.getDefaultGeometry() != null) {
                String geometryContent = feature.getDefaultGeometry().toString();
                GeometryAttribute geometryAttribute = feature.getDefaultGeometryProperty();
                String geometryType = geometryAttribute.getType().getName().toString();
                String geometryTypeValue = "GEOMETRYCOLLECTION";
                if ("Point".equals(geometryType)) {
                    geometryTypeValue = "POINT";
                }
                if ("MultiPoint".equals(geometryType)) {
                    geometryTypeValue = "MULTIPOINT";
                }
                if ("LineString".equals(geometryType)) {
                    geometryTypeValue = "LINESTRING";
                }
                if ("MultiLineString".equals(geometryType)) {
                    geometryTypeValue = "MULTILINESTRING";
                }
                if ("Polygon".equals(geometryType)) {
                    geometryTypeValue = "POLYGON";
                }
                if ("MultiPolygon".equals(geometryType)) {
                    geometryTypeValue = "MULTIPOLYGON";
                }

                newEntityValueMap.put(Constant._GeospatialGeometryType, geometryTypeValue);
                if (_CRS_Range.equals("GlobalLevel")) {
                    newEntityValueMap.put(Constant._GeospatialGLGeometryContent, geometryContent);
                    newEntityValueMap.put(Constant._GeospatialGlobalCRSAID, entityCRSAID);
                }
                if (_CRS_Range.equals("CountryLevel")) {
                    newEntityValueMap.put(Constant._GeospatialCLGeometryContent, geometryContent);
                    newEntityValueMap.put(Constant._GeospatialCountryCRSAID, entityCRSAID);
                }
                if (_CRS_Range.equals("LocalLevel")) {
                    newEntityValueMap.put(Constant._GeospatialLLGeometryContent, geometryContent);
                    if (entityCRSAID != null) {
                        newEntityValueMap.put(Constant._GeospatialLocalCRSAID, entityCRSAID);
                    }
                }
            }
            EntityValue entityValue = new EntityValue(newEntityValueMap);
            _targetEntityValueList.add(entityValue);
        }

        Type targetConceptionType = coreRealm.getType(conceptionKindNameValue);
        if(targetConceptionType != null){
            if(removeExistData){
                targetConceptionType.purgeAllEntities();
            }
        }else{
            coreRealm.createType(conceptionKindNameValue,"-");
        }
        BatchDataOperationUtil.batchAddNewEntities(conceptionKindNameValue, _targetEntityValueList, BatchDataOperationUtil.CPUUsageRate.High);
    }

    public interface EntityAttributesProcess {
        void doEntityAttributesProcess(Map<String,Object> entityValueMap);
    }

    public static void importSHPDataDirectlyToConceptionKind(String conceptionKindName, boolean removeExistData, File shpFile, String fileEncode,EntityAttributesProcess conceptionEntityAttributesProcess) throws IOException, EngineServiceRuntimeException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        String charsetEncode = fileEncode != null ?  fileEncode : "UTF-8";
        // 读取到数据存储中
        FileDataStore dataStore = FileDataStoreFinder.getDataStore(shpFile);
        ((ShapefileDataStore) dataStore).setCharset(Charset.forName(charsetEncode));
        // 获取特征资源
        SimpleFeatureSource simpleFeatureSource = dataStore.getFeatureSource();
        String conceptionKindNameValue = conceptionKindName != null ? conceptionKindName : simpleFeatureSource.getName().toString();
        SimpleFeatureType simpleFeatureType = dataStore.getSchema();
        CoordinateReferenceSystem crs = simpleFeatureType.getCoordinateReferenceSystem();

        String _CRSName = "";
        if (crs != null) {
            _CRSName = crs.getName().getCode();
        }
        String entityCRSAID = null;
        String _CRS_Range = null;
        if("GCS_WGS_1984".equals(_CRSName)||_CRSName.contains("WGS84")){
            entityCRSAID= "EPSG:4326";
            _CRS_Range = "GlobalLevel";
        }else if("CGCS_2000".equals(_CRSName)||_CRSName.contains("CGCS2000")){
            entityCRSAID= "EPSG:4545";
            _CRS_Range = "CountryLevel";
        }else{
            _CRS_Range = "LocalLevel";
            Integer _EpsgCodeValue = null;
            try {
                if (crs!=null) {
                    _EpsgCodeValue = CRS.lookupEpsgCode(crs,true);
                }
            } catch (FactoryException e) {
                EngineServiceRuntimeException engineServiceRuntimeException = new EngineServiceRuntimeException();
                engineServiceRuntimeException.setCauseMessage(e.getMessage());
                throw engineServiceRuntimeException;
            }
            if(_EpsgCodeValue != null){
                entityCRSAID= "EPSG:"+_EpsgCodeValue.intValue();
            }
        }
        // 要素集合
        SimpleFeatureCollection simpleFeatureCollection = simpleFeatureSource.getFeatures();
        List<EntityValue> _targetEntityValueList = Lists.newArrayList();
        // 获取要素迭代器
        SimpleFeatureIterator featureIterator = simpleFeatureCollection.features();
        int idx =0;
        while(featureIterator.hasNext()){
            Map<String,Object> newEntityValueMap = new HashMap<>();
            // 要素对象
            SimpleFeature feature = featureIterator.next();
            // 要素属性信息，名称，值，类型
            List<Property> propertyList = (List<Property>) feature.getValue();
            for(Property property : propertyList){
                String propertyName = property.getName().toString();
                if(!propertyName.equals("the_geom")){
                    //handle invalid chars
                    propertyName = propertyName.replaceAll("△","Delta_");
                    Object propertyValue = property.getValue();
                    if(propertyValue != null && validatePropertyValue(coreRealm,propertyValue)){
                        newEntityValueMap.put(propertyName,propertyValue);
                    }
                }
            }

            if(feature.getDefaultGeometry() != null) {
                String geometryContent = feature.getDefaultGeometry().toString();
                GeometryAttribute geometryAttribute = feature.getDefaultGeometryProperty();
                String geometryType = geometryAttribute.getType().getName().toString();
                String geometryTypeValue = "GEOMETRYCOLLECTION";
                if ("Point".equals(geometryType)) {
                    geometryTypeValue = "POINT";
                }
                if ("MultiPoint".equals(geometryType)) {
                    geometryTypeValue = "MULTIPOINT";
                }
                if ("LineString".equals(geometryType)) {
                    geometryTypeValue = "LINESTRING";
                }
                if ("MultiLineString".equals(geometryType)) {
                    geometryTypeValue = "MULTILINESTRING";
                }
                if ("Polygon".equals(geometryType)) {
                    geometryTypeValue = "POLYGON";
                }
                if ("MultiPolygon".equals(geometryType)) {
                    geometryTypeValue = "MULTIPOLYGON";
                }

                newEntityValueMap.put(Constant._GeospatialGeometryType, geometryTypeValue);
                if (_CRS_Range.equals("GlobalLevel")) {
                    newEntityValueMap.put(Constant._GeospatialGLGeometryContent, geometryContent);
                    newEntityValueMap.put(Constant._GeospatialGlobalCRSAID, entityCRSAID);
                }
                if (_CRS_Range.equals("CountryLevel")) {
                    newEntityValueMap.put(Constant._GeospatialCLGeometryContent, geometryContent);
                    newEntityValueMap.put(Constant._GeospatialCountryCRSAID, entityCRSAID);
                }
                if (_CRS_Range.equals("LocalLevel")) {
                    newEntityValueMap.put(Constant._GeospatialLLGeometryContent, geometryContent);
                    if (entityCRSAID != null) {
                        newEntityValueMap.put(Constant._GeospatialLocalCRSAID, entityCRSAID);
                    }
                }
            }
            if(conceptionEntityAttributesProcess != null){
                conceptionEntityAttributesProcess.doEntityAttributesProcess(newEntityValueMap);
            }
            EntityValue entityValue = new EntityValue(newEntityValueMap);
            _targetEntityValueList.add(entityValue);
        }

        Type targetConceptionType = coreRealm.getType(conceptionKindNameValue);
        if(targetConceptionType != null){
            if(removeExistData){
                targetConceptionType.purgeAllEntities();
            }
        }else{
            coreRealm.createType(conceptionKindNameValue,"-");
        }
        BatchDataOperationUtil.batchAddNewEntities(conceptionKindNameValue, _targetEntityValueList, BatchDataOperationUtil.CPUUsageRate.High);
    }

    private static boolean validatePropertyValue(Engine coreRealm, Object propertyValue){
        if(coreRealm.getStorageImplTech().equals(StorageImplTech.NEO4J)){
            if(propertyValue instanceof Date){
                if(((Date) propertyValue).getYear() <= 1900){
                    //For Neo4j Tech Impl
                    //Neo4j can not store date which's Year small than 1900
                    return false;
                }
            }

        }
        return true;
    }
}
