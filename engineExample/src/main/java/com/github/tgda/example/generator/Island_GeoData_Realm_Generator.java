package com.github.tgda.example.generator;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.feature.GeospatialScaleFeatureSupportable;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class Island_GeoData_Realm_Generator {

    private static final String IndividualTreeConceptionType = "IndividualTree";
    private static final String FrutexConceptionType = "Frutex";
    private static final String FunctionalZoneConceptionType = "FunctionalZone";
    private static final String ZoneSectionConceptionType = "ZoneSection";
    private static final String SectionBlockConceptionType = "SectionBlock";
    private static final String RoadConceptionType = "Road";
    private static final String BuildingConceptionType = "Building";
    private static final String ConstructionLandConceptionType = "ConstructionLand";

    private static final String _LocalCRSAID = "EPSG:4545"; // CGCS2000 / 3-degree Gauss-Kruger CM 108E - Projected

    public static void main(String[] args) throws EngineServiceRuntimeException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        coreRealm.openGlobalSession();
        generateIndividualTreeData(coreRealm);
        generateFrutexData(coreRealm);
        generateFunctionalZoneData(coreRealm);
        generateZoneSectionData(coreRealm);
        generateSectionBlockData(coreRealm);
        generateOtherData(coreRealm);

        coreRealm.closeGlobalSession();
    }

    private static int generateIndividualTreeData(Engine coreRealm) throws EngineServiceRuntimeException {
        Type _IndividualTreeConceptionType = coreRealm.getType(IndividualTreeConceptionType);
        if(_IndividualTreeConceptionType != null){
            coreRealm.removeType(IndividualTreeConceptionType,true);
        }
        _IndividualTreeConceptionType = coreRealm.getType(IndividualTreeConceptionType);
        if(_IndividualTreeConceptionType == null){
            _IndividualTreeConceptionType = coreRealm.createType(IndividualTreeConceptionType,"单木");
        }

        int addedResultCount = 0;
        String filePath ="realmExampleData/island_geoData/islandGISData/danmu1.shp";
        SimpleFeatureCollection colls = readShp(filePath,null,"GBK");
        SimpleFeatureIterator iters = colls.features();
        while(iters.hasNext()){
            SimpleFeature sf = iters.next();

            Map<String,Object> entityDataMap = new HashMap<>();
            entityDataMap.put("X",sf.getAttribute("X"));
            entityDataMap.put("Y",sf.getAttribute("Y"));
            entityDataMap.put("SG",sf.getAttribute("SG"));
            entityDataMap.put("XJ",sf.getAttribute("XJ"));
            entityDataMap.put("GF",sf.getAttribute("GF"));
            entityDataMap.put("SGMJ",sf.getAttribute("SGMJ"));
            entityDataMap.put("SGTJ",sf.getAttribute("SGTJ"));
            entityDataMap.put("SZ",sf.getAttribute("SZ"));
            entityDataMap.put("ZXG",sf.getAttribute("ZXG"));
            entityDataMap.put("CSZS",sf.getAttribute("CSZS"));
            entityDataMap.put("Shape_Leng",sf.getAttribute("Shape_Leng"));
            entityDataMap.put("GNQHID",sf.getAttribute("GNQHID"));
            entityDataMap.put("DMID",sf.getAttribute("DMID"));

            String _LLGeometryContent = sf.getDefaultGeometry().toString();
            EntityValue currentEntityValue = new EntityValue();
            currentEntityValue.setEntityAttributesValue(entityDataMap);
            Entity resultEntity = _IndividualTreeConceptionType.newEntity(currentEntityValue,false);
            if(resultEntity != null){
                resultEntity.addOrUpdateGeometryType(GeospatialScaleFeatureSupportable.WKTGeometryType.POINT);
                resultEntity.addOrUpdateLocalCRSAID(_LocalCRSAID);
                resultEntity.addOrUpdateLLGeometryContent(_LLGeometryContent);
                addedResultCount++;
            }
        }
        return addedResultCount;
    }

    private static int generateFrutexData(Engine coreRealm) throws EngineServiceRuntimeException {
        Type _FrutexConceptionType = coreRealm.getType(FrutexConceptionType);
        if(_FrutexConceptionType != null){
            coreRealm.removeType(FrutexConceptionType,true);
        }
        _FrutexConceptionType = coreRealm.getType(FrutexConceptionType);
        if(_FrutexConceptionType == null){
            _FrutexConceptionType = coreRealm.createType(FrutexConceptionType,"灌木");
        }

        int addedResultCount = 0;
        String filePath ="realmExampleData/island_geoData/islandGISData/guanmu.shp";
        SimpleFeatureCollection colls = readShp(filePath,null,"GBK");
        SimpleFeatureIterator iters = colls.features();
        while(iters.hasNext()){
            SimpleFeature sf = iters.next();

            Map<String,Object> entityDataMap = new HashMap<>();
            entityDataMap.put("TreeLocati",sf.getAttribute("TreeLocati"));
            entityDataMap.put("TreeLoca_1",sf.getAttribute("TreeLoca_1"));
            entityDataMap.put("TreeHeight",sf.getAttribute("TreeHeight"));
            entityDataMap.put("DBH",sf.getAttribute("DBH"));
            entityDataMap.put("CrownDiame",sf.getAttribute("CrownDiame"));
            entityDataMap.put("CrownArea",sf.getAttribute("CrownArea"));
            entityDataMap.put("CrownVolum",sf.getAttribute("CrownVolum"));
            entityDataMap.put("GMLX",sf.getAttribute("GMLX"));

            String _LLGeometryContent = sf.getDefaultGeometry().toString();
            EntityValue currentEntityValue = new EntityValue();
            currentEntityValue.setEntityAttributesValue(entityDataMap);
            Entity resultEntity = _FrutexConceptionType.newEntity(currentEntityValue,false);
            if(resultEntity != null){
                resultEntity.addOrUpdateGeometryType(GeospatialScaleFeatureSupportable.WKTGeometryType.POINT);
                resultEntity.addOrUpdateLocalCRSAID(_LocalCRSAID);
                resultEntity.addOrUpdateLLGeometryContent(_LLGeometryContent);
                addedResultCount++;
            }
        }
        return addedResultCount;
    }

    private static int generateFunctionalZoneData(Engine coreRealm) throws EngineServiceRuntimeException {
        Type _FunctionalZoneConceptionType = coreRealm.getType(FunctionalZoneConceptionType);
        if(_FunctionalZoneConceptionType != null){
            coreRealm.removeType(FunctionalZoneConceptionType,true);
        }
        _FunctionalZoneConceptionType = coreRealm.getType(FunctionalZoneConceptionType);
        if(_FunctionalZoneConceptionType == null){
            _FunctionalZoneConceptionType = coreRealm.createType(FunctionalZoneConceptionType,"功能区");
        }

        int addedResultCount = 0;
        String filePath ="realmExampleData/island_geoData/islandGISData/gongnengqu.shp";
        SimpleFeatureCollection colls = readShp(filePath,null,"GBK");
        SimpleFeatureIterator iters = colls.features();
        while(iters.hasNext()){
            SimpleFeature sf = iters.next();

            Map<String,Object> entityDataMap = new HashMap<>();
            entityDataMap.put("GNQHID",sf.getAttribute("GNQHID"));
            entityDataMap.put("GNQLX",sf.getAttribute("GNQLX"));
            entityDataMap.put("GNQMS",sf.getAttribute("GNQMS"));
            entityDataMap.put("GNQMJ",sf.getAttribute("GNQMJ"));
            entityDataMap.put("GNQMC",sf.getAttribute("GNQMC"));
            entityDataMap.put("Shape_Leng",sf.getAttribute("Shape_Leng"));
            entityDataMap.put("Shape_Area",sf.getAttribute("Shape_Area"));

            String _LLGeometryContent = sf.getDefaultGeometry().toString();
            EntityValue currentEntityValue = new EntityValue();
            currentEntityValue.setEntityAttributesValue(entityDataMap);
            Entity resultEntity = _FunctionalZoneConceptionType.newEntity(currentEntityValue,false);
            if(resultEntity != null){
                resultEntity.addOrUpdateGeometryType(GeospatialScaleFeatureSupportable.WKTGeometryType.MULTIPOLYGON);
                resultEntity.addOrUpdateLocalCRSAID(_LocalCRSAID);
                resultEntity.addOrUpdateLLGeometryContent(_LLGeometryContent);
                addedResultCount++;
            }
        }
        return addedResultCount;
    }

    private static int generateZoneSectionData(Engine coreRealm) throws EngineServiceRuntimeException {
        Type _ZoneSectionConceptionType = coreRealm.getType(ZoneSectionConceptionType);
        if(_ZoneSectionConceptionType != null){
            coreRealm.removeType(ZoneSectionConceptionType,true);
        }
        _ZoneSectionConceptionType = coreRealm.getType(ZoneSectionConceptionType);
        if(_ZoneSectionConceptionType == null){
            _ZoneSectionConceptionType = coreRealm.createType(ZoneSectionConceptionType,"分区板块");
        }

        int addedResultCount = 0;
        String filePath ="realmExampleData/island_geoData/islandGISData/bankuai.shp";
        SimpleFeatureCollection colls = readShp(filePath,null,"GBK");
        SimpleFeatureIterator iters = colls.features();
        while(iters.hasNext()){
            SimpleFeature sf = iters.next();

            Map<String,Object> entityDataMap = new HashMap<>();
            entityDataMap.put("GNQBH",sf.getAttribute("GNQBH"));
            entityDataMap.put("GNQHID",sf.getAttribute("GNQHID"));
            entityDataMap.put("BKLX",sf.getAttribute("BKLX"));
            entityDataMap.put("BKMS",sf.getAttribute("BKMS"));
            entityDataMap.put("BKMJ",sf.getAttribute("BKMJ"));
            entityDataMap.put("BKMC",sf.getAttribute("BKMC"));
            entityDataMap.put("Shape_Leng",sf.getAttribute("Shape_Leng"));
            entityDataMap.put("Shape_Area",sf.getAttribute("Shape_Area"));

            String _LLGeometryContent = sf.getDefaultGeometry().toString();
            EntityValue currentEntityValue = new EntityValue();
            currentEntityValue.setEntityAttributesValue(entityDataMap);
            Entity resultEntity = _ZoneSectionConceptionType.newEntity(currentEntityValue,false);
            if(resultEntity != null){
                resultEntity.addOrUpdateGeometryType(GeospatialScaleFeatureSupportable.WKTGeometryType.MULTIPOLYGON);
                resultEntity.addOrUpdateLocalCRSAID(_LocalCRSAID);
                resultEntity.addOrUpdateLLGeometryContent(_LLGeometryContent);
                addedResultCount++;
            }
        }
        return addedResultCount;
    }

    private static int generateSectionBlockData(Engine coreRealm) throws EngineServiceRuntimeException {
        Type _SectionBlockConceptionType = coreRealm.getType(SectionBlockConceptionType);
        if(_SectionBlockConceptionType != null){
            coreRealm.removeType(SectionBlockConceptionType,true);
        }
        _SectionBlockConceptionType = coreRealm.getType(SectionBlockConceptionType);
        if(_SectionBlockConceptionType == null){
            _SectionBlockConceptionType = coreRealm.createType(SectionBlockConceptionType,"分区子板块");
        }

        int addedResultCount = 0;
        String filePath ="realmExampleData/island_geoData/islandGISData/xiaoban.shp";
        SimpleFeatureCollection colls = readShp(filePath,null,"GBK");
        SimpleFeatureIterator iters = colls.features();
        while(iters.hasNext()){
            SimpleFeature sf = iters.next();

            Map<String,Object> entityDataMap = new HashMap<>();
            entityDataMap.put("XBLX",sf.getAttribute("XBLX"));
            entityDataMap.put("XBMS",sf.getAttribute("XBMS"));
            entityDataMap.put("XFLX",sf.getAttribute("XFLX"));
            entityDataMap.put("XBMJ",sf.getAttribute("XBMJ"));
            entityDataMap.put("YSSZ",sf.getAttribute("YSSZ"));
            entityDataMap.put("SZZC",sf.getAttribute("SZZC"));
            entityDataMap.put("YBD",sf.getAttribute("YBD"));
            entityDataMap.put("STLX",sf.getAttribute("STLX"));
            entityDataMap.put("BKMC",sf.getAttribute("BKMC"));
            entityDataMap.put("Xmin",sf.getAttribute("Xmin"));
            entityDataMap.put("Ymax",sf.getAttribute("Ymax"));
            entityDataMap.put("BH",sf.getAttribute("BH"));
            entityDataMap.put("BKBH",sf.getAttribute("BKBH"));
            entityDataMap.put("XBMC",sf.getAttribute("XBMC"));
            entityDataMap.put("GNQHID",sf.getAttribute("GNQHID"));
            entityDataMap.put("LXBH",sf.getAttribute("LXBH"));
            entityDataMap.put("Shape_Leng",sf.getAttribute("Shape_Leng"));
            entityDataMap.put("Shape_Area",sf.getAttribute("Shape_Area"));

            String _LLGeometryContent = sf.getDefaultGeometry().toString();
            EntityValue currentEntityValue = new EntityValue();
            currentEntityValue.setEntityAttributesValue(entityDataMap);
            Entity resultEntity = _SectionBlockConceptionType.newEntity(currentEntityValue,false);
            if(resultEntity != null){
                resultEntity.addOrUpdateGeometryType(GeospatialScaleFeatureSupportable.WKTGeometryType.MULTIPOLYGON);
                resultEntity.addOrUpdateLocalCRSAID(_LocalCRSAID);
                resultEntity.addOrUpdateLLGeometryContent(_LLGeometryContent);
                addedResultCount++;
            }
        }
        return addedResultCount;
    }

    private static int generateOtherData(Engine coreRealm) throws EngineServiceRuntimeException {
        Type _RoadConceptionType = coreRealm.getType(RoadConceptionType);
        if(_RoadConceptionType != null){
            coreRealm.removeType(RoadConceptionType,true);
        }
        _RoadConceptionType = coreRealm.getType(RoadConceptionType);
        if(_RoadConceptionType == null){
            _RoadConceptionType = coreRealm.createType(RoadConceptionType,"道路");
        }

        Type _BuildingConceptionType = coreRealm.getType(BuildingConceptionType);
        if(_BuildingConceptionType != null){
            coreRealm.removeType(BuildingConceptionType,true);
        }
        _BuildingConceptionType = coreRealm.getType(BuildingConceptionType);
        if(_BuildingConceptionType == null){
            _BuildingConceptionType = coreRealm.createType(BuildingConceptionType,"建筑");
        }

        Type _ConstructionLandConceptionType = coreRealm.getType(ConstructionLandConceptionType);
        if(_ConstructionLandConceptionType != null){
            coreRealm.removeType(ConstructionLandConceptionType,true);
        }
        _ConstructionLandConceptionType = coreRealm.getType(ConstructionLandConceptionType);
        if(_ConstructionLandConceptionType == null){
            _ConstructionLandConceptionType = coreRealm.createType(ConstructionLandConceptionType,"建设用地");
        }

        int addedResultCount = 0;
        String filePath ="realmExampleData/island_geoData/islandGISData/道路（其他）/行道树_路_建筑.shp";
        SimpleFeatureCollection colls = readShp(filePath,null,"UTF-8");
        SimpleFeatureIterator iters = colls.features();
        while(iters.hasNext()){
            SimpleFeature sf = iters.next();

            Map<String,Object> entityDataMap = new HashMap<>();
            entityDataMap.put("XBLX",sf.getAttribute("XBLX"));
            entityDataMap.put("GNQ",sf.getAttribute("GNQ"));
            entityDataMap.put("BK",sf.getAttribute("BK"));
            entityDataMap.put("XBID",sf.getAttribute("XBID"));
            entityDataMap.put("XBMC",sf.getAttribute("XBMC"));
            entityDataMap.put("XBMS",sf.getAttribute("XBMS"));
            entityDataMap.put("XFLX",sf.getAttribute("XFLX"));
            entityDataMap.put("GNQHID",sf.getAttribute("GNQHID"));
            entityDataMap.put("GNCMC",sf.getAttribute("GNCMC"));
            entityDataMap.put("BX",sf.getAttribute("BX"));
            entityDataMap.put("XBMJ",sf.getAttribute("XBMJ"));
            entityDataMap.put("YSSZ",sf.getAttribute("YSSZ"));
            entityDataMap.put("SZZC",sf.getAttribute("SZZC"));
            entityDataMap.put("PJNL",sf.getAttribute("PJNL"));
            entityDataMap.put("PJSG",sf.getAttribute("PJSG"));
            entityDataMap.put("YSSG",sf.getAttribute("YSSG"));
            entityDataMap.put("PJXJ",sf.getAttribute("PJXJ"));
            entityDataMap.put("XJDMJ",sf.getAttribute("XJDMJ"));
            entityDataMap.put("XJL",sf.getAttribute("XJL"));
            entityDataMap.put("SWL",sf.getAttribute("SWL"));
            entityDataMap.put("ZS",sf.getAttribute("ZS"));
            entityDataMap.put("SZL",sf.getAttribute("SZL"));
            entityDataMap.put("XB_X",sf.getAttribute("XB_X"));
            entityDataMap.put("XB_Y",sf.getAttribute("XB_Y"));
            entityDataMap.put("XBBH",sf.getAttribute("XBBH"));
            entityDataMap.put("SUM_SGMJ",sf.getAttribute("SUM_SGMJ"));
            entityDataMap.put("YBD",sf.getAttribute("YBD"));
            entityDataMap.put("TRLX",sf.getAttribute("TRLX"));
            entityDataMap.put("TRQSCD",sf.getAttribute("TRQSCD"));
            entityDataMap.put("TRQSCDDJ",sf.getAttribute("TRQSCDDJ"));
            entityDataMap.put("TRZD",sf.getAttribute("TRZD"));
            entityDataMap.put("STLX",sf.getAttribute("STLX"));

            String _LLGeometryContent = sf.getDefaultGeometry().toString();
            EntityValue currentEntityValue = new EntityValue();
            currentEntityValue.setEntityAttributesValue(entityDataMap);

            String entityType = sf.getAttribute("XBLX").toString();
            Entity resultEntity = null;
            if(entityType.equals("建筑")){
                resultEntity = _BuildingConceptionType.newEntity(currentEntityValue,false);
            }else if(entityType.equals("建设用地")){
                resultEntity = _ConstructionLandConceptionType.newEntity(currentEntityValue,false);
            }else{
                resultEntity = _RoadConceptionType.newEntity(currentEntityValue,false);
            }

            if(resultEntity != null){
                resultEntity.addOrUpdateGeometryType(GeospatialScaleFeatureSupportable.WKTGeometryType.MULTIPOLYGON);
                resultEntity.addOrUpdateLocalCRSAID(_LocalCRSAID);
                resultEntity.addOrUpdateLLGeometryContent(_LLGeometryContent);
                addedResultCount++;
            }
        }
        return addedResultCount;
    }

    private static SimpleFeatureCollection  readShp(String path , Filter filter,String encodeType){
        SimpleFeatureSource featureSource = readStoreByShp(path,encodeType);
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

    private static  SimpleFeatureSource readStoreByShp(String path,String encodeType){
        File file = new File(path);
        FileDataStore store;
        SimpleFeatureSource featureSource = null;
        try {
            store = FileDataStoreFinder.getDataStore(file);
            ((ShapefileDataStore) store).setCharset(Charset.forName(encodeType));
            featureSource = store.getFeatureSource();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return featureSource;
    }
}
