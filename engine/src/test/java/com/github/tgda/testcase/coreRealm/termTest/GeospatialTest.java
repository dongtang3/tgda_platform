package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.analysis.query.AttributesParameters;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.EqualFilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.payload.EntitiesRetrieveResult;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.GeospatialScaleDataPair;
import com.github.tgda.engine.core.payload.GeospatialScaleEventsRetrieveResult;
import com.github.tgda.engine.core.structure.InheritanceTree;
import com.github.tgda.coreRealm.realmServiceCore.term.*;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.Constant;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeospatialTest {

    private static String testRealmName = "UNIT_TEST_Realm";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for GeospatialTest");
        System.out.println("--------------------------------------------------");
        setupGeospatialFunction();
    }

    private void setupGeospatialFunction(){
        CoreRealm coreRealm = EngineFactory.getDefaultEngine();
        Assert.assertEquals(coreRealm.getStorageImplTech(), StorageImplTech.NEO4J);
        //long resultCount = coreRealm.removeGeospatialWithEntities();
        Geospatial geospatialRegion = coreRealm.getOrCreateGeospatial();
        List<GeospatialScaleEntity> entityList = geospatialRegion.listContinentEntities();
        if(entityList.size() == 0){
            geospatialRegion.createGeospatialScaleEntities();
        }
    }

    @Test
    public void testGeospatialFunction() throws EngineServiceRuntimeException {
        CoreRealm coreRealm = EngineFactory.getDefaultEngine();
        coreRealm.openGlobalSession();
        Assert.assertEquals(coreRealm.getStorageImplTech(), StorageImplTech.NEO4J);

        List<Geospatial> geospatialRegionList = coreRealm.getGeospatials();
        Assert.assertTrue(geospatialRegionList.size()>=1);

        Geospatial defaultGeospatial = coreRealm.getOrCreateGeospatial();
        Assert.assertEquals(defaultGeospatial.getGeospatialName(), Constant._defaultGeospatialName);

        Geospatial geospatialRegion1 = coreRealm.getOrCreateGeospatial("geospatialRegion1");
        Assert.assertEquals(geospatialRegion1.getGeospatialName(),"geospatialRegion1");

        geospatialRegion1 = coreRealm.getOrCreateGeospatial("geospatialRegion1");
        Assert.assertEquals(geospatialRegion1.getGeospatialName(),"geospatialRegion1");

        Geospatial geospatialRegion2 = coreRealm.getOrCreateGeospatial("geospatialRegion2");
        Assert.assertEquals(geospatialRegion2.getGeospatialName(),"geospatialRegion2");

        geospatialRegionList = coreRealm.getGeospatials();
        Assert.assertTrue(geospatialRegionList.size()>=3);
        int orgCount = geospatialRegionList.size();
        for(Geospatial currentGeospatial: geospatialRegionList){
            boolean matchResult = currentGeospatial.getGeospatialName().equals(Constant._defaultGeospatialName)|
                                    currentGeospatial.getGeospatialName().equals("geospatialRegion1")|
                                    currentGeospatial.getGeospatialName().equals("geospatialRegion2");
            Assert.assertTrue(matchResult);
        }

        long removeRegionEntitiesCount = coreRealm.removeGeospatialWithEntities("geospatialRegion2");
        Assert.assertEquals(removeRegionEntitiesCount,1l);
        geospatialRegionList = coreRealm.getGeospatials();
        Assert.assertEquals(geospatialRegionList.size(),orgCount-1);

        List<GeospatialScaleEntity> continentGeospatialScaleEntityList = defaultGeospatial.listContinentEntities();
        Assert.assertEquals(continentGeospatialScaleEntityList.size(),7);
        for(GeospatialScaleEntity currentGeospatialScaleEntity:continentGeospatialScaleEntityList){
            Assert.assertNotNull(currentGeospatialScaleEntity.getGeospatialCode());
            Assert.assertNotNull(currentGeospatialScaleEntity.getGeospatialScaleGrade());
            Assert.assertNotNull(currentGeospatialScaleEntity.getChineseName());
            Assert.assertNotNull(currentGeospatialScaleEntity.getEnglishName());
        }

        GeospatialScaleEntity targetGeospatialScaleEntity1 = defaultGeospatial.getEntityByGeospatialCode("640522406498");
        Assert.assertEquals(targetGeospatialScaleEntity1.getGeospatialCode(),"640522406498");
        Assert.assertEquals(targetGeospatialScaleEntity1.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.VILLAGE);
        Assert.assertEquals(targetGeospatialScaleEntity1.getChineseName(),"海兴虚拟社区");
        Assert.assertNull(targetGeospatialScaleEntity1.getEnglishName());

        GeospatialScaleEntity targetGeospatialScaleEntity2 = defaultGeospatial.getEntityByGeospatialCode("AD-07");
        Assert.assertEquals(targetGeospatialScaleEntity2.getGeospatialCode(),"AD-07");
        Assert.assertEquals(targetGeospatialScaleEntity2.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.PROVINCE);
        Assert.assertEquals(targetGeospatialScaleEntity2.getChineseName(),"");
        Assert.assertEquals(targetGeospatialScaleEntity2.getEnglishName(),"Andorra la Vella");

        GeospatialScaleEntity targetGeospatialScaleEntity3 = defaultGeospatial.getContinentEntity(Geospatial.GeospatialProperty.ChineseName,"北");
        Assert.assertEquals(targetGeospatialScaleEntity3.getGeospatialCode(),"North America");
        Assert.assertEquals(targetGeospatialScaleEntity3.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.CONTINENT);
        Assert.assertEquals(targetGeospatialScaleEntity3.getChineseName(),"北美洲");
        Assert.assertEquals(targetGeospatialScaleEntity3.getEnglishName(),"North America");

        List<GeospatialScaleEntity> countryRegionGeospatialScaleEntityList2  = defaultGeospatial.listCountryRegionEntities(Geospatial.GeospatialProperty.GeospatialCode,"U");
        Assert.assertEquals(countryRegionGeospatialScaleEntityList2.size(),6);
        for(GeospatialScaleEntity currentGeospatialScaleEntity:countryRegionGeospatialScaleEntityList2){
            Assert.assertTrue(currentGeospatialScaleEntity.getGeospatialCode().startsWith("U"));
            Assert.assertEquals(currentGeospatialScaleEntity.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.COUNTRY_REGION);
            Assert.assertNotNull(currentGeospatialScaleEntity.getChineseName());
            Assert.assertNotNull(currentGeospatialScaleEntity.getEnglishName());
        }

        GeospatialScaleEntity targetGeospatialScaleEntity4 = defaultGeospatial.getCountryRegionEntity(Geospatial.GeospatialProperty.GeospatialCode,"CN");
        Assert.assertEquals(targetGeospatialScaleEntity4.getGeospatialCode(),"CN");
        Assert.assertEquals(targetGeospatialScaleEntity4.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.COUNTRY_REGION);
        Assert.assertEquals(targetGeospatialScaleEntity4.getChineseName(),"中国");
        Assert.assertEquals(targetGeospatialScaleEntity4.getEnglishName(),"China");

        List<GeospatialScaleEntity> countryRegionGeospatialScaleEntityList3 = defaultGeospatial.listProvinceEntities(Geospatial.GeospatialProperty.ChineseName,"中国",null);
        Assert.assertEquals(countryRegionGeospatialScaleEntityList3.size(),34);
        for(GeospatialScaleEntity currentGeospatialScaleEntity:countryRegionGeospatialScaleEntityList3){
            Assert.assertNotNull(currentGeospatialScaleEntity.getGeospatialCode());
            Assert.assertEquals(currentGeospatialScaleEntity.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.PROVINCE);
            Assert.assertNotNull(currentGeospatialScaleEntity.getChineseName());
            Assert.assertNotNull(currentGeospatialScaleEntity.getEnglishName());
        }

        GeospatialScaleEntity targetGeospatialScaleEntity5 = defaultGeospatial.getProvinceEntity(Geospatial.GeospatialProperty.ChineseName,"中国","江西省");
        Assert.assertEquals(targetGeospatialScaleEntity5.getGeospatialCode(),"360000");
        Assert.assertEquals(targetGeospatialScaleEntity5.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.PROVINCE);
        Assert.assertEquals(targetGeospatialScaleEntity5.getChineseName(),"江西省");
        Assert.assertEquals(targetGeospatialScaleEntity5.getEnglishName(),"Jiangxi Sheng");

        List<GeospatialScaleEntity> countryRegionGeospatialScaleEntityList4 = defaultGeospatial.listPrefectureEntities(Geospatial.GeospatialProperty.ChineseName,"中国","江西省",null);
        Assert.assertEquals(countryRegionGeospatialScaleEntityList4.size(),11);
        for(GeospatialScaleEntity currentGeospatialScaleEntity:countryRegionGeospatialScaleEntityList4){
            Assert.assertTrue(currentGeospatialScaleEntity.getGeospatialCode().startsWith("36"));
            Assert.assertEquals(currentGeospatialScaleEntity.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.PREFECTURE);
            Assert.assertNotNull(currentGeospatialScaleEntity.getChineseName());
            Assert.assertNull(currentGeospatialScaleEntity.getEnglishName());
        }

        GeospatialScaleEntity targetGeospatialScaleEntity6 = defaultGeospatial.getPrefectureEntity(Geospatial.GeospatialProperty.ChineseName,"中国","江西省","宜春市");
        Assert.assertEquals(targetGeospatialScaleEntity6.getGeospatialCode(),"360900");
        Assert.assertEquals(targetGeospatialScaleEntity6.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.PREFECTURE);
        Assert.assertEquals(targetGeospatialScaleEntity6.getChineseName(),"宜春市");
        Assert.assertNull(targetGeospatialScaleEntity6.getEnglishName());

        List<GeospatialScaleEntity> countryRegionGeospatialScaleEntityList5 = defaultGeospatial.listCountyEntities(Geospatial.GeospatialProperty.ChineseName,"中国","江西省","宜春市",null);
        Assert.assertEquals(countryRegionGeospatialScaleEntityList5.size(),10);
        for(GeospatialScaleEntity currentGeospatialScaleEntity:countryRegionGeospatialScaleEntityList5){
            Assert.assertTrue(currentGeospatialScaleEntity.getGeospatialCode().startsWith("3609"));
            Assert.assertEquals(currentGeospatialScaleEntity.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.COUNTY);
            Assert.assertNotNull(currentGeospatialScaleEntity.getChineseName());
            Assert.assertNull(currentGeospatialScaleEntity.getEnglishName());
        }

        GeospatialScaleEntity targetGeospatialScaleEntity7 = defaultGeospatial.getCountyEntity(Geospatial.GeospatialProperty.ChineseName,"中国","江西省","宜春市","袁州区");
        Assert.assertEquals(targetGeospatialScaleEntity7.getGeospatialCode(),"360902");
        Assert.assertEquals(targetGeospatialScaleEntity7.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.COUNTY);
        Assert.assertEquals(targetGeospatialScaleEntity7.getChineseName(),"袁州区");
        Assert.assertNull(targetGeospatialScaleEntity7.getEnglishName());

        List<GeospatialScaleEntity> countryRegionGeospatialScaleEntityList6 = defaultGeospatial.listTownshipEntities(Geospatial.GeospatialProperty.ChineseName,"中国","江西省","宜春市","袁州区",null);
        Assert.assertEquals(countryRegionGeospatialScaleEntityList6.size(),38);
        for(GeospatialScaleEntity currentGeospatialScaleEntity:countryRegionGeospatialScaleEntityList6){
            Assert.assertTrue(currentGeospatialScaleEntity.getGeospatialCode().startsWith("360902"));
            Assert.assertEquals(currentGeospatialScaleEntity.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.TOWNSHIP);
            Assert.assertNotNull(currentGeospatialScaleEntity.getChineseName());
            Assert.assertNull(currentGeospatialScaleEntity.getEnglishName());
        }

        GeospatialScaleEntity targetGeospatialScaleEntity8 = defaultGeospatial.getTownshipEntity(Geospatial.GeospatialProperty.ChineseName,"中国","江西省","宜春市","袁州区","飞剑潭乡");
        Assert.assertEquals(targetGeospatialScaleEntity8.getGeospatialCode(),"360902213");
        Assert.assertEquals(targetGeospatialScaleEntity8.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.TOWNSHIP);
        Assert.assertEquals(targetGeospatialScaleEntity8.getChineseName(),"飞剑潭乡");
        Assert.assertNull(targetGeospatialScaleEntity8.getEnglishName());

        List<GeospatialScaleEntity> countryRegionGeospatialScaleEntityList7 = defaultGeospatial.listVillageEntities(Geospatial.GeospatialProperty.ChineseName,"中国","江西省","宜春市","袁州区","飞剑潭乡",null);
        Assert.assertEquals(countryRegionGeospatialScaleEntityList7.size(),10);
        for(GeospatialScaleEntity currentGeospatialScaleEntity:countryRegionGeospatialScaleEntityList7){
            Assert.assertTrue(currentGeospatialScaleEntity.getGeospatialCode().startsWith("360902213"));
            Assert.assertEquals(currentGeospatialScaleEntity.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.VILLAGE);
            Assert.assertNotNull(currentGeospatialScaleEntity.getChineseName());
            Assert.assertNull(currentGeospatialScaleEntity.getEnglishName());
        }

        GeospatialScaleEntity targetGeospatialScaleEntity9 = defaultGeospatial.getVillageEntity(Geospatial.GeospatialProperty.ChineseName,"中国","江西省","宜春市","袁州区","飞剑潭乡","塘源村委会");
        Assert.assertEquals(targetGeospatialScaleEntity9.getGeospatialCode(),"360902213201");
        Assert.assertEquals(targetGeospatialScaleEntity9.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.VILLAGE);
        Assert.assertEquals(targetGeospatialScaleEntity9.getChineseName(),"塘源村委会");
        Assert.assertNull(targetGeospatialScaleEntity9.getEnglishName());

        GeospatialScaleEntity targetGeospatialScaleEntity10 = targetGeospatialScaleEntity9.getParentEntity();
        Assert.assertEquals(targetGeospatialScaleEntity10.getGeospatialCode(),"360902213");
        Assert.assertEquals(targetGeospatialScaleEntity10.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.TOWNSHIP);
        Assert.assertEquals(targetGeospatialScaleEntity10.getChineseName(),"飞剑潭乡");
        Assert.assertNull(targetGeospatialScaleEntity10.getEnglishName());

        List<GeospatialScaleEntity> countryRegionGeospatialScaleEntityList8 = targetGeospatialScaleEntity9.getFellowEntities();
        Assert.assertEquals(countryRegionGeospatialScaleEntityList8.size(),10);
        for(GeospatialScaleEntity currentGeospatialScaleEntity:countryRegionGeospatialScaleEntityList8){
            Assert.assertTrue(currentGeospatialScaleEntity.getGeospatialCode().startsWith("360902213"));
            Assert.assertEquals(currentGeospatialScaleEntity.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.VILLAGE);
            Assert.assertNotNull(currentGeospatialScaleEntity.getChineseName());
            Assert.assertNull(currentGeospatialScaleEntity.getEnglishName());
        }

        InheritanceTree<GeospatialScaleEntity> geospatialScaleEntityTree = targetGeospatialScaleEntity6.getOffspringEntities();
        Assert.assertEquals(geospatialScaleEntityTree.getNode(geospatialScaleEntityTree.getRootID()).getGeospatialCode(),"360900");
        Assert.assertEquals(geospatialScaleEntityTree.size(),3012);

        ConceptionKind _ConceptionKind01 = coreRealm.getConceptionKind("GeospatialFeatureTestKind");
        if(_ConceptionKind01 != null){
            coreRealm.removeConceptionKind("GeospatialFeatureTestKind",true);
        }
        _ConceptionKind01 = coreRealm.getConceptionKind("GeospatialFeatureTestKind");
        if(_ConceptionKind01 == null){
            _ConceptionKind01 = coreRealm.createConceptionKind("GeospatialFeatureTestKind","-");
        }

        Map<String,Object> newEntityValue= new HashMap<>();
        newEntityValue.put("prop1",10000l);

        EntityValue entityValue = new EntityValue(newEntityValue);
        Entity _Entity01 = _ConceptionKind01.newEntity(entityValue,false);

        Map<String,Object> eventDataMap= new HashMap<>();
        eventDataMap.put("data1","this is s data");
        eventDataMap.put("data2",new Date());

        GeospatialScaleEvent _GeospatialScaleEvent1 = _Entity01.attachGeospatialScaleEvent("360902213200","eventAttachComment",eventDataMap);
        Assert.assertNotNull(_GeospatialScaleEvent1.getGeospatialScaleEventUID());
        Assert.assertEquals(_GeospatialScaleEvent1.getReferLocation(),"360902213200");
        Assert.assertEquals(_GeospatialScaleEvent1.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.VILLAGE);
        Assert.assertEquals(_GeospatialScaleEvent1.getGeospatialName(), Constant._defaultGeospatialName);
        Assert.assertEquals(_GeospatialScaleEvent1.getEventComment(),"eventAttachComment");

        GeospatialScaleEntity targetGeospatialScaleEntity = _GeospatialScaleEvent1.getReferGeospatialScaleEntity();
        Assert.assertNull(targetGeospatialScaleEntity.getEnglishName());
        Assert.assertEquals(targetGeospatialScaleEntity.getChineseName(),"殊桥村委会");
        Assert.assertEquals(targetGeospatialScaleEntity.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.VILLAGE);
        Assert.assertEquals(targetGeospatialScaleEntity.getGeospatialCode(),"360902213200");
        Assert.assertEquals(targetGeospatialScaleEntity.getParentEntity().getGeospatialCode(),"360902213");

        Entity targetEntity = _GeospatialScaleEvent1.getAttachEntity();
        Assert.assertEquals(targetEntity.getConceptionKindName(),"GeospatialFeatureTestKind");
        Assert.assertEquals(targetEntity.getEntityUID(),_Entity01.getEntityUID());

        GeospatialScaleEvent _GeospatialScaleEvent2 = _Entity01.attachGeospatialScaleEvent("360902213209","eventAttachComment",eventDataMap);

        List<GeospatialScaleEvent> targetGeospatialScaleEventList = _Entity01.getAttachedGeospatialScaleEvents();
        Assert.assertEquals(targetGeospatialScaleEventList.size(),2);
        for(GeospatialScaleEvent currentGeospatialScaleEvent:targetGeospatialScaleEventList){
            Assert.assertNotNull(currentGeospatialScaleEvent.getGeospatialScaleEventUID());
            Assert.assertNotNull(currentGeospatialScaleEvent.getReferLocation());
            Assert.assertEquals(currentGeospatialScaleEvent.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.VILLAGE);
            Assert.assertEquals(currentGeospatialScaleEvent.getGeospatialName(), Constant._defaultGeospatialName);
            Assert.assertNotNull(currentGeospatialScaleEvent.getEventComment());
        }

        List<GeospatialScaleEntity> targetGeospatialScaleEntityList = _Entity01.getAttachedGeospatialScaleEntities();
        Assert.assertEquals(targetGeospatialScaleEntityList.size(),2);
        for(GeospatialScaleEntity currentGeospatialScaleEntity:targetGeospatialScaleEntityList){
            Assert.assertNull(currentGeospatialScaleEntity.getEnglishName());
            Assert.assertNotNull(currentGeospatialScaleEntity.getChineseName());
            Assert.assertEquals(currentGeospatialScaleEntity.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.VILLAGE);
            Assert.assertTrue(currentGeospatialScaleEntity.getGeospatialCode().startsWith("360902213"));
            Assert.assertEquals(currentGeospatialScaleEntity.getParentEntity().getGeospatialCode(),"360902213");
        }

        List<GeospatialScaleDataPair> targetGeospatialScaleDataPairList = _Entity01.getAttachedGeospatialScaleDataPairs();
        Assert.assertEquals(targetGeospatialScaleDataPairList.size(),2);
        for(GeospatialScaleDataPair currentGeospatialScaleDataPair:targetGeospatialScaleDataPairList){
            GeospatialScaleEntity currentGeospatialScaleEntity = currentGeospatialScaleDataPair.getGeospatialScaleEntity();
            Assert.assertNull(currentGeospatialScaleEntity.getEnglishName());
            Assert.assertNotNull(currentGeospatialScaleEntity.getChineseName());
            Assert.assertEquals(currentGeospatialScaleEntity.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.VILLAGE);
            Assert.assertTrue(currentGeospatialScaleEntity.getGeospatialCode().startsWith("360902213"));
            Assert.assertEquals(currentGeospatialScaleEntity.getParentEntity().getGeospatialCode(),"360902213");

            GeospatialScaleEvent currentGeospatialScaleEvent = currentGeospatialScaleDataPair.getGeospatialScaleEvent();
            Assert.assertNotNull(currentGeospatialScaleEvent.getGeospatialScaleEventUID());
            Assert.assertNotNull(currentGeospatialScaleEvent.getReferLocation());
            Assert.assertEquals(currentGeospatialScaleEvent.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.VILLAGE);
            Assert.assertEquals(currentGeospatialScaleEvent.getGeospatialName(), Constant._defaultGeospatialName);
            Assert.assertNotNull(currentGeospatialScaleEvent.getEventComment());
        }

        GeospatialScaleEvent _GeospatialScaleEventForDelete = _Entity01.attachGeospatialScaleEvent("360902213","eventAttachComment",eventDataMap);
        targetGeospatialScaleDataPairList = _Entity01.getAttachedGeospatialScaleDataPairs();
        Assert.assertEquals(targetGeospatialScaleDataPairList.size(),3);
        boolean detachGeospatialScaleEventResult = _Entity01.detachGeospatialScaleEvent(_GeospatialScaleEventForDelete.getGeospatialScaleEventUID());
        Assert.assertTrue(detachGeospatialScaleEventResult);
        targetGeospatialScaleDataPairList = _Entity01.getAttachedGeospatialScaleDataPairs();
        Assert.assertEquals(targetGeospatialScaleDataPairList.size(),2);

        boolean exceptionShouldThrown = false;
        try {
            _Entity01.detachGeospatialScaleEvent("12345678900000000");
        }catch(EngineServiceRuntimeException e){
            exceptionShouldThrown = true;
        }
        Assert.assertTrue(exceptionShouldThrown);

        GeospatialScaleEntity targetGeospatialScaleEntity11 = defaultGeospatial.getEntityByGeospatialCode("360902213200");
        Assert.assertEquals(targetGeospatialScaleEntity11.countAttachedConceptionEntities(GeospatialScaleEntity.GeospatialScaleLevel.SELF),Long.valueOf("1"));
        Assert.assertEquals(targetGeospatialScaleEntity11.countAttachedConceptionEntities(GeospatialScaleEntity.GeospatialScaleLevel.CHILD),Long.valueOf("0"));
        Assert.assertEquals(targetGeospatialScaleEntity11.countAttachedConceptionEntities(GeospatialScaleEntity.GeospatialScaleLevel.OFFSPRING),Long.valueOf("0"));

        targetGeospatialScaleEntity11 = defaultGeospatial.getEntityByGeospatialCode("360902");
        Assert.assertEquals(targetGeospatialScaleEntity11.countAttachedConceptionEntities(GeospatialScaleEntity.GeospatialScaleLevel.SELF),Long.valueOf("0"));
        Assert.assertEquals(targetGeospatialScaleEntity11.countAttachedConceptionEntities(GeospatialScaleEntity.GeospatialScaleLevel.CHILD),Long.valueOf("0"));
        Assert.assertEquals(targetGeospatialScaleEntity11.countAttachedConceptionEntities(GeospatialScaleEntity.GeospatialScaleLevel.OFFSPRING),Long.valueOf("2"));

        targetGeospatialScaleEntity11 = defaultGeospatial.getEntityByGeospatialCode("360902213");
        Assert.assertEquals(targetGeospatialScaleEntity11.countAttachedConceptionEntities(GeospatialScaleEntity.GeospatialScaleLevel.SELF),Long.valueOf("0"));
        Assert.assertEquals(targetGeospatialScaleEntity11.countAttachedConceptionEntities(GeospatialScaleEntity.GeospatialScaleLevel.CHILD),Long.valueOf("2"));
        Assert.assertEquals(targetGeospatialScaleEntity11.countAttachedConceptionEntities(GeospatialScaleEntity.GeospatialScaleLevel.OFFSPRING),Long.valueOf("2"));

        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(10000000);
        queryParameters.setDistinctMode(true);
        EntitiesRetrieveResult entitiesRetrieveResult = targetGeospatialScaleEntity11.getAttachedConceptionEntities("GeospatialFeatureTestKind",queryParameters,GeospatialScaleEntity.GeospatialScaleLevel.SELF);
         Assert.assertEquals(entitiesRetrieveResult.getConceptionEntities().size(),0);
        EntitiesRetrieveResult entitiesRetrieveResult2 = targetGeospatialScaleEntity11.getAttachedConceptionEntities(null,queryParameters,GeospatialScaleEntity.GeospatialScaleLevel.CHILD);
        Assert.assertEquals(entitiesRetrieveResult2.getConceptionEntities().size(),1);
        EntitiesRetrieveResult entitiesRetrieveResult3 = targetGeospatialScaleEntity11.getAttachedConceptionEntities(null,queryParameters,GeospatialScaleEntity.GeospatialScaleLevel.OFFSPRING);
        Assert.assertEquals(entitiesRetrieveResult3.getConceptionEntities().size(),1);
        for(Entity currentEntity: entitiesRetrieveResult3.getConceptionEntities()){
            Assert.assertNotNull(currentEntity.getEntityUID());
            Assert.assertEquals(currentEntity.getConceptionKindName(),"GeospatialFeatureTestKind");
        }

        AttributesParameters attributesParameters = new AttributesParameters();
        attributesParameters.setDefaultFilteringItem(new EqualFilteringItem("prop1",10000l));
        Assert.assertEquals(targetGeospatialScaleEntity11.
                countAttachedConceptionEntities(null,attributesParameters,true,GeospatialScaleEntity.GeospatialScaleLevel.SELF),Long.valueOf("0"));
        Assert.assertEquals(targetGeospatialScaleEntity11.
                countAttachedConceptionEntities(null,attributesParameters,true,GeospatialScaleEntity.GeospatialScaleLevel.CHILD),Long.valueOf("2"));
        Assert.assertEquals(targetGeospatialScaleEntity11.
                countAttachedConceptionEntities(null,attributesParameters,true,GeospatialScaleEntity.GeospatialScaleLevel.OFFSPRING),Long.valueOf("2"));

        GeospatialScaleEventsRetrieveResult geospatialScaleEventsRetrieveResult1 = targetGeospatialScaleEntity11.getAttachedGeospatialScaleEvents(queryParameters,GeospatialScaleEntity.GeospatialScaleLevel.SELF);
        Assert.assertEquals(geospatialScaleEventsRetrieveResult1.getGeospatialScaleEvents().size(),0);
        GeospatialScaleEventsRetrieveResult geospatialScaleEventsRetrieveResult2 = targetGeospatialScaleEntity11.getAttachedGeospatialScaleEvents(queryParameters,GeospatialScaleEntity.GeospatialScaleLevel.CHILD);
        Assert.assertEquals(geospatialScaleEventsRetrieveResult2.getGeospatialScaleEvents().size(),2);
        GeospatialScaleEventsRetrieveResult geospatialScaleEventsRetrieveResult3 = targetGeospatialScaleEntity11.getAttachedGeospatialScaleEvents(queryParameters,GeospatialScaleEntity.GeospatialScaleLevel.OFFSPRING);
        Assert.assertEquals(geospatialScaleEventsRetrieveResult3.getGeospatialScaleEvents().size(),2);
        for(GeospatialScaleEvent currentGeospatialScaleEvent:geospatialScaleEventsRetrieveResult3.getGeospatialScaleEvents()){
            Assert.assertEquals(currentGeospatialScaleEvent.getEventComment(),"eventAttachComment");
            Assert.assertNotNull(currentGeospatialScaleEvent.getGeospatialScaleEventUID());
            Assert.assertEquals(currentGeospatialScaleEvent.getGeospatialScaleGrade(),Geospatial.GeospatialScaleGrade.VILLAGE);
            Assert.assertNotNull(currentGeospatialScaleEvent.getReferLocation());
        }

        AttributesParameters attributesParameters2 = new AttributesParameters();
        attributesParameters2.setDefaultFilteringItem(new EqualFilteringItem("TGDA_GeospatialScaleEventComment","eventAttachComment"));
        Assert.assertEquals(targetGeospatialScaleEntity11.
                countAttachedGeospatialScaleEvents(attributesParameters2,true,GeospatialScaleEntity.GeospatialScaleLevel.SELF),Long.valueOf("0"));
        Assert.assertEquals(targetGeospatialScaleEntity11.
                countAttachedGeospatialScaleEvents(attributesParameters2,true,GeospatialScaleEntity.GeospatialScaleLevel.CHILD),Long.valueOf("2"));
        Assert.assertEquals(targetGeospatialScaleEntity11.
                countAttachedGeospatialScaleEvents(attributesParameters2,true,GeospatialScaleEntity.GeospatialScaleLevel.OFFSPRING),Long.valueOf("2"));

        Assert.assertEquals(_GeospatialScaleEvent1.getAliasConceptionKindNames().size(),0);
        _GeospatialScaleEvent1.joinConceptionKinds(new String[]{"conceptionKD01","conceptionKD02"});
        Assert.assertEquals(_GeospatialScaleEvent1.getAliasConceptionKindNames().size(),2);
        _GeospatialScaleEvent1.retreatFromConceptionKind("conceptionKD01");
        Assert.assertEquals(_GeospatialScaleEvent1.getAliasConceptionKindNames().size(),1);

        boolean detachResult = _Entity01.detachGeospatialScaleEvent(_GeospatialScaleEvent1.getGeospatialScaleEventUID());
        Assert.assertTrue(detachResult);

        long removeRefersEventsResult = defaultGeospatial.removeRefersGeospatialScaleEvents();
        Assert.assertTrue(removeRefersEventsResult > 0);

        boolean exceptionShouldThrown2 = false;
        try {
            _Entity01.detachGeospatialScaleEvent(_GeospatialScaleEvent2.getGeospatialScaleEventUID());
        }catch(EngineServiceRuntimeException e){
            exceptionShouldThrown2 = true;
        }
        Assert.assertTrue(exceptionShouldThrown2);

        coreRealm.closeGlobalSession();
    }
}
