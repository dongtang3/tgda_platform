package com.github.tgda.example.generator;

import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.term.Type;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.factory.EngineFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UndergroundPipelineNetwork_Realm_Generator {

    private static final String PipelineConceptionType = "Pipeline";

    private static final String PipePointConceptionType = "PipePoint";
    private static final String PipePointStandardCode = "standardCode";
    private static final String PipePointNetworkType = "networkType";
    private static final String PipePointGroundElevation = "groundElevation";
    private static final String PipePointName = "name";
    private static final String PipePointLocationRoad = "locationRoad";
    private static final String PipePointPointID = "pointID";
    private static final String PipePointElementType = "elementType";

    private static final String PipeTubulationConceptionType = "PipeTubulation";
    private static final String PipeTubulationStandardCode = "standardCode";
    private static final String PipeTubulationStartPortionTopElevation = "startPortionTopElevation";
    private static final String PipeTubulationNetworkType = "networkType";
    private static final String PipeTubulationEndPortionBuriedDepth = "endPortionBuriedDepth";
    private static final String PipeTubulationEndPortionTopElevation = "endPortionTopElevation";
    private static final String PipeTubulationTubulationID = "tubulationID";
    private static final String PipeTubulationCrossSection = "crossSection";
    private static final String PipeTubulationName = "name";
    private static final String PipeTubulationStartPortionBuriedDepth = "startPortionBuriedDepth";
    private static final String PipeTubulationLocationRoad = "locationRoad";
    private static final String PipeTubulationStartPointID = "startPointID";
    private static final String PipeTubulationMaterial = "material";
    private static final String PipeTubulationElementType = "elementType";
    private static final String PipeTubulationTubulationLength = "tubulationLength";
    private static final String PipeTubulationEndPointID = "endPointID";

    public static void main(String[] args) throws EngineServiceRuntimeException, EngineServiceEntityExploreException {

        class InsertRecordThread implements Runnable{
            private List<EntityValue> conceptionEntityValueList;
            private Type conceptionKind;

            public InsertRecordThread(Type conceptionKind, List<EntityValue> conceptionEntityValueList){
                this.conceptionEntityValueList = conceptionEntityValueList;
                this.conceptionKind = conceptionKind;
            }
            @Override
            public void run(){
                this.conceptionKind.newEntities(conceptionEntityValueList,false);
            }
        }

        Engine coreRealm = EngineFactory.getDefaultEngine();
        //Part 1

        Type _PipelineType = coreRealm.getType(PipelineConceptionType);
        if(_PipelineType != null){
            coreRealm.removeType(PipelineConceptionType,true);
        }
        _PipelineType = coreRealm.getType(PipelineConceptionType);
        if(_PipelineType == null){
            _PipelineType = coreRealm.createType(PipelineConceptionType,"地下管线");
        }

        Type _PipePointType = coreRealm.getType(PipePointConceptionType);
        if(_PipePointType != null){
            coreRealm.removeType(PipePointConceptionType,true);
        }
        _PipePointType = coreRealm.getType(PipePointConceptionType);
        if(_PipePointType == null){
            _PipePointType = coreRealm.createType(PipePointConceptionType,"管点");
        }

        Type _PipeTubulationType = coreRealm.getType(PipeTubulationConceptionType);
        if(_PipeTubulationType != null){
            coreRealm.removeType(PipeTubulationConceptionType,true);
        }
        _PipeTubulationType = coreRealm.getType(PipeTubulationConceptionType);
        if(_PipeTubulationType == null){
            _PipeTubulationType = coreRealm.createType(PipeTubulationConceptionType,"管段");
        }

        List<EntityValue> pipePointEntityValueList = new ArrayList<>();
        File file = new File("realmExampleData/underground_pipelinenetwork/UGPN_Point.csv");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                String currentLine = !tempStr.startsWith("standardCode")? tempStr : null;
                if(currentLine != null){
                    String[] dataItems =  currentLine.split(",");
                    String standardCode = dataItems[0];
                    String networkType = dataItems[1];
                    String groundElevation = dataItems[2];
                    String name = dataItems[3];
                    String locationRoad = dataItems[4];
                    String pointID = dataItems[5];
                    String elementType = dataItems[6];

                    Map<String,Object> newEntityValueMap = new HashMap<>();
                    newEntityValueMap.put(PipePointStandardCode,standardCode);
                    newEntityValueMap.put(PipePointNetworkType,networkType);
                    if(groundElevation.equals("")){
                        newEntityValueMap.put(PipePointGroundElevation,Float.valueOf(0.0f));
                    }else{
                        newEntityValueMap.put(PipePointGroundElevation,Float.parseFloat(groundElevation));
                    }
                    newEntityValueMap.put(PipePointName,name);
                    newEntityValueMap.put(PipePointLocationRoad,locationRoad);
                    newEntityValueMap.put(PipePointPointID,pointID);
                    newEntityValueMap.put(PipePointElementType,elementType);

                    EntityValue entityValue = new EntityValue(newEntityValueMap);
                    pipePointEntityValueList.add(entityValue);
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

        List<List<EntityValue>> pointRsList = Lists.partition(pipePointEntityValueList, 1500);
        ExecutorService executor1 = Executors.newFixedThreadPool(pointRsList.size());
        for (List<EntityValue> currentEntityValueList : pointRsList) {
            Type type = coreRealm.getType(PipePointConceptionType);
            InsertRecordThread insertRecordThread = new InsertRecordThread(type, currentEntityValueList);
            executor1.execute(insertRecordThread);
        }
        executor1.shutdown();

        List<EntityValue> pipeTubulationEntityValueList = new ArrayList<>();
        File file2 = new File("realmExampleData/underground_pipelinenetwork/UGPN_Tubulation.csv");
        BufferedReader reader2 = null;
        try {
            reader2 = new BufferedReader(new FileReader(file2));
            String tempStr;
            while ((tempStr = reader2.readLine()) != null) {
                String currentLine = !tempStr.startsWith("standardCode")? tempStr : null;
                if(currentLine != null){
                    String[] dataItems =  currentLine.split(",");

                    String standardCode = dataItems[0];
                    String startPortionTopElevation = dataItems[1];
                    String networkType = dataItems[2];
                    String endPortionBuriedDepth = dataItems[3];
                    String endPortionTopElevation = dataItems[4];
                    String tubulationID = dataItems[5];
                    String crossSection = dataItems[6];
                    String name = dataItems[7];
                    String startPortionBuriedDepth = dataItems[8];
                    String locationRoad = dataItems[9];
                    String startPointID = dataItems[10];
                    String material = dataItems[11];
                    String elementType = dataItems[12];
                    String tubulationLength = dataItems[13];
                    String endPointID = dataItems[14];

                    Map<String, Object> newEntityValueMap = new HashMap<>();
                    newEntityValueMap.put(PipeTubulationStandardCode, standardCode);
                    if (startPortionTopElevation.equals("")) {
                        newEntityValueMap.put(PipeTubulationStartPortionTopElevation, Float.valueOf(0.0f));
                    } else {
                        newEntityValueMap.put(PipeTubulationStartPortionTopElevation, Float.parseFloat(startPortionTopElevation));
                    }
                    newEntityValueMap.put(PipeTubulationNetworkType, networkType);
                    if (endPortionBuriedDepth.equals("")) {
                        newEntityValueMap.put(PipeTubulationEndPortionBuriedDepth, Float.valueOf(0.0f));
                    } else {
                        newEntityValueMap.put(PipeTubulationEndPortionBuriedDepth, Float.parseFloat(endPortionBuriedDepth));
                    }
                    if (endPortionTopElevation.equals("")) {
                            newEntityValueMap.put(PipeTubulationEndPortionTopElevation, Float.valueOf(0.0f));
                    } else {
                        newEntityValueMap.put(PipeTubulationEndPortionTopElevation, Float.parseFloat(endPortionTopElevation));
                    }
                    newEntityValueMap.put(PipeTubulationTubulationID, tubulationID);
                    newEntityValueMap.put(PipeTubulationCrossSection, crossSection);
                    newEntityValueMap.put(PipeTubulationName, name);
                    if (startPortionBuriedDepth.equals("")) {
                        newEntityValueMap.put(PipeTubulationStartPortionBuriedDepth, Float.valueOf(0.0f));
                    } else {
                        newEntityValueMap.put(PipeTubulationStartPortionBuriedDepth, Float.parseFloat(startPortionBuriedDepth));
                    }
                    newEntityValueMap.put(PipeTubulationLocationRoad, locationRoad);
                    newEntityValueMap.put(PipeTubulationStartPointID, startPointID);
                    newEntityValueMap.put(PipeTubulationMaterial, material);
                    newEntityValueMap.put(PipeTubulationElementType, elementType);
                    if (tubulationLength.equals("")) {
                        newEntityValueMap.put(PipeTubulationTubulationLength, Float.valueOf(0.0f));
                    } else {
                        newEntityValueMap.put(PipeTubulationTubulationLength, Float.parseFloat(tubulationLength));
                    }
                    newEntityValueMap.put(PipeTubulationEndPointID, endPointID);
                    EntityValue entityValue = new EntityValue(newEntityValueMap);
                    pipeTubulationEntityValueList.add(entityValue);
                }
            }
            reader2.close();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader2 != null) {
                try {
                    reader2.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        List<List<EntityValue>> tubulationRsList = Lists.partition(pipeTubulationEntityValueList, 1500);
        ExecutorService executor2 = Executors.newFixedThreadPool(pointRsList.size());
        for (List<EntityValue> currentEntityValueList : tubulationRsList) {
            Type type = coreRealm.getType(PipeTubulationConceptionType);
            InsertRecordThread insertRecordThread = new InsertRecordThread(type, currentEntityValueList);
            executor2.execute(insertRecordThread);
        }
        executor2.shutdown();

        //Part 2
        /*
        ConceptionKind _PipePointConceptionKind = coreRealm.getConceptionKind(PipePointConceptionType);

        List<String> attributeNamesList1 = new ArrayList<>();
        attributeNamesList1.add(PipePointPointID);
        attributeNamesList1.add(PipePointNetworkType);
        QueryParameters queryParameters1 = new QueryParameters();
        queryParameters1.setResultNumber(20000);

        ConceptionEntitiesAttributesRetrieveResult conceptionEntitiesAttributesRetrieveResult1 = _PipePointConceptionKind.getSingleValueEntityAttributesByAttributeNames(attributeNamesList1,queryParameters1);

        List<EntityValue> conceptionEntityValueList1 = conceptionEntitiesAttributesRetrieveResult1.getEntityValues();
        Map<String,String> idUIDMapping_PipePoint = new HashMap();
        for(EntityValue currentPointEntityValue : conceptionEntityValueList1){
            String uid = currentPointEntityValue.getEntityUID();
            String idValue = currentPointEntityValue.getEntityAttributesValue().get(PipePointPointID).toString();
            idUIDMapping_PipePoint.put(idValue,uid);
        }

        ConceptionKind _PipeTubulationConceptionKind = coreRealm.getConceptionKind(PipeTubulationConceptionType);

        List<String> attributeNamesList2 = new ArrayList<>();
        attributeNamesList2.add(PipeTubulationStartPointID);
        attributeNamesList2.add(PipeTubulationEndPointID);
        attributeNamesList2.add(PipeTubulationNetworkType);
        QueryParameters queryParameters2 = new QueryParameters();
        queryParameters2.setResultNumber(20000);

        ConceptionEntitiesAttributesRetrieveResult conceptionEntitiesAttributesRetrieveResult2 = _PipeTubulationConceptionKind.getSingleValueEntityAttributesByAttributeNames(attributeNamesList2,queryParameters2);
        List<EntityValue> conceptionEntityValueList2 = conceptionEntitiesAttributesRetrieveResult2.getEntityValues();

        class LinkPointAndTubulationThread implements Runnable{

            private Map<String,String> pipePointDataMap;
            private List<EntityValue> tubulationEntityValueList;

            public LinkPointAndTubulationThread(Map<String,String> pipePointDataMap,List<EntityValue> tubulationEntityValueList){
                this.pipePointDataMap = pipePointDataMap;
                this.tubulationEntityValueList = tubulationEntityValueList;
            }

            @Override
            public void run() {
                CoreRealm coreRealm = RealmTermFactory.getDefaultCoreRealm();
                coreRealm.openGlobalSession();
                ConceptionKind _PipeTubulationConceptionKind = coreRealm.getConceptionKind(PipeTubulationConceptionType);

                for(EntityValue currentEntityValue:tubulationEntityValueList){
                    String entityUID = currentEntityValue.getEntityUID();
                    String startPointId = currentEntityValue.getEntityAttributesValue().get(PipeTubulationStartPointID).toString();
                    String endPointId = currentEntityValue.getEntityAttributesValue().get(PipeTubulationEndPointID).toString();

                    Entity currentEntity = _PipeTubulationConceptionKind.getEntityByUID(entityUID);
                    String _startEntityUID = pipePointDataMap.get(startPointId);
                    if(_startEntityUID != null){
                        try {
                            currentEntity.attachToRelation(_startEntityUID,"connectTo",null,true);
                        } catch (CoreRealmServiceRuntimeException e) {
                            e.printStackTrace();
                        }
                    }

                    String _endEntityUID = pipePointDataMap.get(endPointId);
                    if(_endEntityUID != null){
                        try {
                            currentEntity.attachFromRelation(_endEntityUID,"connectTo",null,true);
                        } catch (CoreRealmServiceRuntimeException e) {
                            e.printStackTrace();
                        }
                    }
                }
                coreRealm.closeGlobalSession();
            }
        }

        List<List<EntityValue>> tubulationRsList2 = Lists.partition(conceptionEntityValueList2, 1500);

        //ExecutorService executor3 = Executors.newFixedThreadPool(tubulationRsList2.size());
        ExecutorService executor3 = Executors.newFixedThreadPool(5);
        for (List<EntityValue> currentEntityValueList : tubulationRsList2) {
            LinkPointAndTubulationThread linkPointAndTubulationThread = new LinkPointAndTubulationThread(idUIDMapping_PipePoint,currentEntityValueList);
            executor3.execute(linkPointAndTubulationThread);
        }
        executor3.shutdown();
        */

    }
}
