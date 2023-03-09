package com.github.tgda.testcase.coreRealm.termTest;

import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.feature.GeospatialScaleCalculable;
import com.github.tgda.engine.core.feature.GeospatialScaleFeatureSupportable;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.config.PropertiesHandler;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import com.github.tgda.engine.core.util.geospatial.GeospatialOperationUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeospatialScaleCalculableTest {

    private static String testRealmName = "UNIT_TEST_Realm";

    @BeforeTest
    public void initData(){
        System.out.println("--------------------------------------------------");
        System.out.println("Init unit test data for GeospatialScaleCalculableTest");
        System.out.println("--------------------------------------------------");
    }

    @Test
    public void testGeospatialScaleCalculableFunction() throws EngineServiceRuntimeException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        coreRealm.openGlobalSession();

        Assert.assertEquals(coreRealm.getStorageImplTech(), StorageImplTech.NEO4J);

        Type areaType = coreRealm.getType("AreaForGeospatialScaleCalculable");
        if(areaType == null){
            areaType = coreRealm.createType("AreaForGeospatialScaleCalculable","AreaForTest");
        }
        areaType.purgeAllEntities();

        Map<String,Object> newEntityValue= new HashMap<>();
        EntityValue entityValue = new EntityValue(newEntityValue);
        Entity _hugeAreaEntity = areaType.newEntity(entityValue,false);

        boolean exceptionShouldHappen = false;
        List<String> targetKind = new ArrayList<>();
        targetKind.add("TargetKind1");
        try {
            _hugeAreaEntity.getSpatialPredicateMatchedConceptionEntities("TargetKind1",null, GeospatialScaleCalculable.SpatialPredicateType.Contains,
                    GeospatialScaleCalculable.SpatialScaleLevel.Country);
        }catch (EngineServiceException e){
            exceptionShouldHappen = true;
        }
        Assert.assertTrue(exceptionShouldHappen);

        String hugeArea = "POLYGON ((118.076235 32.061369,118.499544 32.061369,118.499544 31.437824,118.076235 31.437824,118.076235 32.061369))";
        String smallArea = "MULTIPOLYGON (((118.33287452033196 31.817914072778905, 118.33188244361592 31.8170718164435, 118.33308243049693 31.814735116433507, 118.33218222590527 31.813423603874792, 118.33432458618609 31.809807897742036, 118.33286750964245 31.807440265431744, 118.32918322656808 31.808979569878645, 118.32442807482224 31.80898867020534, 118.32142998285764 31.804981696796535, 118.32016818874801 31.79536149464915, 118.32010526941771 31.795730673442538, 118.31766367908432 31.795990308714675, 118.31864904899547 31.794347194142752, 118.31667874522526 31.79281863652409, 118.31492077126376 31.793989086480412, 118.31196478430206 31.795782788327973, 118.31149328069154 31.79921220308652, 118.30729413494244 31.801737090988816, 118.30605235795917 31.79969591068926, 118.30395270790713 31.79988253191936, 118.2947364283958 31.79796395633624, 118.28903679011748 31.796110787292687, 118.28826485366874 31.7942885283173, 118.28547903018803 31.793087936869753, 118.28312018406537 31.795608218544913, 118.28290592318834 31.80067930351856, 118.28329132758292 31.80308624866969, 118.2842342702596 31.80439833465757, 118.28277658282404 31.805713299047643, 118.27732940348201 31.80404094950633, 118.2735118843507 31.805211734147615, 118.2733279748092 31.805423930518703, 118.2659184587035 31.813935884290146, 118.25991085637973 31.819664900069593, 118.25896677010772 31.82061342045259, 118.25681961821812 31.820687107590572, 118.25235539604212 31.82349645827081, 118.24904927200602 31.821673269171267, 118.24728938301266 31.821673174205067, 118.24540004452201 31.824153389063753, 118.23637589887444 31.831155712487814, 118.24663536010588 31.839634432187477, 118.24762464044883 31.842515074779936, 118.24384494613093 31.846525296046114, 118.23791798599912 31.846851412384108, 118.24487481141499 31.855532268447206, 118.24861278887444 31.857172998475992, 118.25432192814027 31.856042366715705, 118.25891446370974 31.856442224864725, 118.26532583948085 31.86574307856165, 118.27139866504088 31.874552657817947, 118.27740181250164 31.885629546698514, 118.27924611331821 31.899002507267287, 118.2888496198382 31.89760558708156, 118.29510621710234 31.913594146652795, 118.29922024569613 31.9151550998543, 118.31091766153929 31.91338573485763, 118.32158555739245 31.91384003256907, 118.32295693575205 31.91482064242524, 118.33201119450881 31.92423342916698, 118.33383761983255 31.92613137429138, 118.34549066216233 31.935327796554102, 118.34947485603996 31.935210728643426, 118.35547401138747 31.936840008758907, 118.35850694442624 31.93683376734503, 118.35795138887013 31.93472005206677, 118.357934027759 31.933630642344532, 118.3583940972039 31.932543402761592, 118.35893229164888 31.93202256942872, 118.36019965276122 31.9313780381798, 118.36154513887362 31.930859374986486, 118.36335069443088 31.930496961793523, 118.3649565972101 31.92968532985035, 118.36703124998968 31.928634982629692, 118.36984374999192 31.927180989576097, 118.37118055554845 31.92613932291028, 118.37256944443831 31.924698350688892, 118.37436631943943 31.922335069439878, 118.37459487436067 31.921988187856954, 118.37546874999562 31.9206618923571, 118.38055555555351 31.91662977430358, 118.38286458333202 31.915993923609783, 118.38534722222151 31.915965711804734, 118.39007812499989 31.915831163194156, 118.39050347222214 31.915963541666404, 118.39211805555554 31.91629774305536, 118.39460937500004 31.916569010416513, 118.39581597222227 31.91633029513874, 118.3974913194445 31.91585286458319, 118.3990972222223 31.91469835069432, 118.40075520833346 31.913426649305478, 118.40252647496867 31.912046709020103, 118.40262152777804 31.91197265625003, 118.40455729166713 31.91001302083356, 118.40529977663148 31.909196946383375, 118.40369045487377 31.90386535790837, 118.40515040120343 31.900184686749334, 118.39909810927888 31.89500963658762, 118.39656601040222 31.89391690301774, 118.39223149221507 31.892825158260948, 118.3895279881867 31.89486692073246, 118.38463827467503 31.892865626849993, 118.38678345907371 31.890349596070397, 118.3863119891223 31.886997194065053, 118.3883716379001 31.881600752203624, 118.39476455374083 31.878062064154683, 118.39630995383884 31.873286614048236, 118.40167616592362 31.868254785248602, 118.39991607898031 31.864938232089326, 118.39519494769964 31.86300683113066, 118.40051744897985 31.862276636034743, 118.4027076716755 31.855532211078003, 118.40030305726778 31.85535026831466, 118.4013769423855 31.84856886139183, 118.39682747496093 31.846636137171508, 118.39695672653167 31.844011599224544, 118.39433887974639 31.843137193376034, 118.39348075521731 31.845470955734488, 118.38944578785852 31.84423259496119, 118.38502762407576 31.8445264590793, 118.38605646528305 31.84077001643885, 118.38099540649326 31.841029006900563, 118.3810815389511 31.83898754324857, 118.37439042519446 31.835237455681824, 118.37220414071072 31.840235894111082, 118.37121716423093 31.84045611216872, 118.3688594420203 31.844251771873385, 118.36855979748397 31.84417914266596, 118.36247374655875 31.841671032493565, 118.36251693764684 31.839009462427715, 118.36101708218433 31.838245532095353, 118.3530463427351 31.83563269223635, 118.34777576460795 31.838887368719043, 118.34477538506415 31.83426185345891, 118.3513312424222 31.82367438269537, 118.35188872163386 31.819515689098548, 118.3346241370908 31.81940100846461, 118.33287452033196 31.817914072778905)))";

        _hugeAreaEntity.addOrUpdateLLGeometryContent(hugeArea);
        Entity _smallAreaEntity = areaType.newEntity(entityValue,false);
        _smallAreaEntity.addOrUpdateLLGeometryContent(smallArea);

        boolean spatialPredicateMatchedCheckResult = _hugeAreaEntity.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Contains, _smallAreaEntity.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(spatialPredicateMatchedCheckResult);

        spatialPredicateMatchedCheckResult = _smallAreaEntity.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Within, _hugeAreaEntity.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(spatialPredicateMatchedCheckResult);

        String locationPointInSmallArea = "Point(118.3270 31.8573)";
        String locationPointOutSmallArea = "Point(118.3789 31.7784)"; //in huge Area
        String locationPointOutHugeArea = "Point(118.5891 31.7230)";

        Type locationType = coreRealm.getType("LocationForGeospatialScaleCalculable");
        if(locationType == null){
            locationType = coreRealm.createType("LocationForGeospatialScaleCalculable","locationForTest");
        }
        locationType.purgeAllEntities();

        Map<String,Object> newLocationEntityValue= new HashMap<>();
        EntityValue entityValue2 = new EntityValue(newLocationEntityValue);
        Entity _LocationInSmallAreaEntity1 = areaType.newEntity(entityValue2,false);
        _LocationInSmallAreaEntity1.addOrUpdateLLGeometryContent(locationPointInSmallArea);
        Entity _LocationInSmallAreaEntity2 = areaType.newEntity(entityValue2,false);
        _LocationInSmallAreaEntity2.addOrUpdateLLGeometryContent(locationPointOutSmallArea);

        spatialPredicateMatchedCheckResult = _smallAreaEntity.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Contains, _LocationInSmallAreaEntity1.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(spatialPredicateMatchedCheckResult);

        spatialPredicateMatchedCheckResult = _LocationInSmallAreaEntity1.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Within, _smallAreaEntity.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(spatialPredicateMatchedCheckResult);

        spatialPredicateMatchedCheckResult = _smallAreaEntity.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Contains, _LocationInSmallAreaEntity2.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertFalse(spatialPredicateMatchedCheckResult);

        spatialPredicateMatchedCheckResult = _LocationInSmallAreaEntity2.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Within, _smallAreaEntity.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertFalse(spatialPredicateMatchedCheckResult);

        spatialPredicateMatchedCheckResult = _hugeAreaEntity.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Contains, _LocationInSmallAreaEntity1.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(spatialPredicateMatchedCheckResult);

        spatialPredicateMatchedCheckResult = _hugeAreaEntity.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Contains, _LocationInSmallAreaEntity2.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(spatialPredicateMatchedCheckResult);

        spatialPredicateMatchedCheckResult = _LocationInSmallAreaEntity1.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Within, _hugeAreaEntity.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(spatialPredicateMatchedCheckResult);

        spatialPredicateMatchedCheckResult = _LocationInSmallAreaEntity2.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Within, _hugeAreaEntity.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(spatialPredicateMatchedCheckResult);

        Entity _LocationInSmallAreaEntity3 = areaType.newEntity(entityValue2,false);
        _LocationInSmallAreaEntity3.addOrUpdateLLGeometryContent(locationPointOutHugeArea);

        spatialPredicateMatchedCheckResult = _LocationInSmallAreaEntity3.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Within, _hugeAreaEntity.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertFalse(spatialPredicateMatchedCheckResult);

        spatialPredicateMatchedCheckResult = _hugeAreaEntity.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Contains, _LocationInSmallAreaEntity3.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertFalse(spatialPredicateMatchedCheckResult);

        spatialPredicateMatchedCheckResult = _hugeAreaEntity.isSpatialPredicateMatchedWith(GeospatialScaleCalculable.SpatialPredicateType.Disjoint, _LocationInSmallAreaEntity3.getEntityUID(), GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(spatialPredicateMatchedCheckResult);

        Assert.assertEquals(_hugeAreaEntity.getEntityGeometryType(GeospatialScaleCalculable.SpatialScaleLevel.Local), GeospatialScaleFeatureSupportable.WKTGeometryType.POLYGON);
        Assert.assertEquals(_smallAreaEntity.getEntityGeometryType(GeospatialScaleCalculable.SpatialScaleLevel.Local), GeospatialScaleFeatureSupportable.WKTGeometryType.MULTIPOLYGON);
        Assert.assertEquals(_LocationInSmallAreaEntity3.getEntityGeometryType(GeospatialScaleCalculable.SpatialScaleLevel.Local), GeospatialScaleFeatureSupportable.WKTGeometryType.POINT);

        double entitiesDistance = _LocationInSmallAreaEntity1.getEntitiesSpatialDistance(_LocationInSmallAreaEntity2.getEntityUID(),GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(entitiesDistance>0);

        Assert.assertTrue(_LocationInSmallAreaEntity1.isSpatialDistanceWithinEntity(_LocationInSmallAreaEntity2.getEntityUID(),entitiesDistance+1,GeospatialScaleCalculable.SpatialScaleLevel.Local)
        );
        Assert.assertFalse(_LocationInSmallAreaEntity1.isSpatialDistanceWithinEntity(_LocationInSmallAreaEntity2.getEntityUID(),entitiesDistance-0.01,GeospatialScaleCalculable.SpatialScaleLevel.Local)
        );

        String bufferedWKTContent = _LocationInSmallAreaEntity1.getEntitySpatialBufferWKTGeometryContent(0.1,GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(bufferedWKTContent.startsWith("POLYGON"));

        String envelopeWKTContent = _hugeAreaEntity.getEntitySpatialEnvelopeWKTGeometryContent(GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(envelopeWKTContent.startsWith("POLYGON"));

        envelopeWKTContent = _LocationInSmallAreaEntity1.getEntitySpatialEnvelopeWKTGeometryContent(GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(envelopeWKTContent.startsWith("POINT"));

        String centroidWKTContent = _LocationInSmallAreaEntity1.getEntitySpatialCentroidPointWKTGeometryContent(GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(centroidWKTContent.startsWith("POINT"));

        String interiorWKTContent = _LocationInSmallAreaEntity1.getEntitySpatialInteriorPointWKTGeometryContent(GeospatialScaleCalculable.SpatialScaleLevel.Local);
        Assert.assertTrue(interiorWKTContent.startsWith("POINT"));

        coreRealm.closeGlobalSession();

        //Prepare data for mass WKT computation
        String filePath = PropertiesHandler.SYSTEM_RESOURCE_ROOT+"/"+"geospatialData"+"/ChinaData/China_Township_shp/"+"我国乡镇行政区划.shp";
        try {
            GeospatialOperationUtil.importSHPDataDirectlyToConceptionKind("TestWKTConceptionKind",true,new File(filePath),null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        String spaceLocationPoint = "Point(104.4860 36.9892)";
        locationType = coreRealm.getType("LocationForGeospatialScaleCalculable");
        coreRealm.openGlobalSession();
        newLocationEntityValue= new HashMap<>();
        EntityValue entityValueForLocationCompute = new EntityValue(newLocationEntityValue);
        Entity entityValueForLocationComputeEntity = locationType.newEntity(entityValueForLocationCompute,false);
        entityValueForLocationComputeEntity.addOrUpdateGLGeometryContent(spaceLocationPoint);

        try {
            List<Entity> resultEntityList = entityValueForLocationComputeEntity.getSpatialPredicateMatchedConceptionEntities("TestWKTConceptionKind",null, GeospatialScaleCalculable.SpatialPredicateType.Within, GeospatialScaleCalculable.SpatialScaleLevel.Global);
            Assert.assertEquals(resultEntityList.size(),1);
            Assert.assertEquals(resultEntityList.get(0).getAttribute("省").getAttributeValue(),"甘肃省");
            Assert.assertEquals(resultEntityList.get(0).getAttribute("市").getAttributeValue(),"白银市");
            Assert.assertEquals(resultEntityList.get(0).getAttribute("县").getAttributeValue(),"靖远县");
            Assert.assertEquals(resultEntityList.get(0).getAttribute("乡").getAttributeValue(),"石门乡");

            resultEntityList = entityValueForLocationComputeEntity.getSpatialBufferMatchedConceptionEntities("TestWKTConceptionKind",null, 0.5,GeospatialScaleCalculable.SpatialPredicateType.Contains, GeospatialScaleCalculable.SpatialScaleLevel.Global);
            Assert.assertEquals(resultEntityList.size(),13);
            for(Entity currentEntity :resultEntityList){
                Assert.assertEquals(currentEntity.getAttribute("市").getAttributeValue(),"白银市");
            }
        } catch (EngineServiceEntityExploreException e) {
            throw new RuntimeException(e);
        }
        coreRealm.closeGlobalSession();
    }
}
