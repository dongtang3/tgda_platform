package com.github.tgda.example.generator;

import com.github.tgda.engine.core.payload.EntityValue;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.util.BatchDataOperationUtil;
import com.github.tgda.engine.core.payload.EntitiesAttributesRetrieveResult;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.term.TimeFlow;
import com.github.tgda.engine.core.util.factory.EngineFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SeattleRealTimeFire911Calls_Realm_Generator {

    private static final String Fire911CallConceptionType = "Fire911Call";
    private static final String Address = "address";
    private static final String Type = "type";
    private static final String Datetime = "datetime";
    private static final String Latitude = "latitude";
    private static final String Longitude = "longitude";
    private static final String Location = "location";
    private static final String IncidentNumber = "incidentNumber";

    public static void main(String[] args) throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        createData();
        linkData();
    }

    public static void createData() throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        Engine coreRealm = EngineFactory.getDefaultEngine();

        //Part 1
        com.github.tgda.engine.core.term.Type _Fire911CallType = coreRealm.getType(Fire911CallConceptionType);
        if(_Fire911CallType != null){
            coreRealm.removeType(Fire911CallConceptionType,true);
        }
        _Fire911CallType = coreRealm.getType(Fire911CallConceptionType);
        if(_Fire911CallType == null){
            _Fire911CallType = coreRealm.createType(Fire911CallConceptionType,"911报警记录");
        }

        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa");

        List<EntityValue> _Fire911CallEntityValueList = Lists.newArrayList();
        //Please unzip Seattle_Real_Time_Fire_911_Calls_huge.csv.zip before execute
        File file = new File("realmExampleData/seattle_fire_911_calls/Seattle_Real_Time_Fire_911_Calls_huge.csv");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                String currentLine = !tempStr.startsWith("Address,")? tempStr : null;
                if(currentLine != null){
                    String address = null;
                    String type = null;
                    String datetime = null;
                    String latitude = null;
                    String longitude = null;
                    String reportLocation = null;
                    String incidentNumber = null;

                    String[] dataItems =  currentLine.split(",");

                    if(dataItems.length == 7){
                        address = dataItems[0].trim();
                        type = dataItems[1].trim();
                        datetime = dataItems[2].trim();
                        latitude = dataItems[3].trim();
                        longitude = dataItems[4].trim();
                        reportLocation = dataItems[5].trim();
                        incidentNumber = dataItems[6].trim();
                    }

                    if(dataItems.length == 8){
                        address = dataItems[0].trim();
                        type = (dataItems[1].trim()+dataItems[2].trim()).replaceAll("\"","");
                        datetime = dataItems[3].trim();
                        latitude = dataItems[4].trim();
                        longitude = dataItems[5].trim();
                        reportLocation = dataItems[6].trim();
                        incidentNumber = dataItems[7].trim();
                    }

                    Map<String,Object> newEntityValueMap = new HashMap<>();

                    newEntityValueMap.put(Address,address);
                    newEntityValueMap.put(Type,type);
                    Date date = sdf.parse(datetime);
                    newEntityValueMap.put(Datetime,date);
                    if(!latitude.equals("")){
                        newEntityValueMap.put(Latitude,Double.parseDouble(latitude));
                    }
                    if(!longitude.equals("")){
                        newEntityValueMap.put(Longitude,Double.parseDouble(longitude));
                    }
                    newEntityValueMap.put(Location,reportLocation);
                    newEntityValueMap.put(IncidentNumber,incidentNumber);

                    newEntityValueMap.put("TGDA_GS_GLGeometryContent",reportLocation);
                    newEntityValueMap.put("TGDA_GS_GeometryType","POINT");
                    newEntityValueMap.put("TGDA_GS_GlobalCRSAID","EPSG:4326");

                    EntityValue entityValue = new EntityValue(newEntityValueMap);
                    _Fire911CallEntityValueList.add(entityValue);
                }
            }
            reader.close();
        } catch (IOException | ParseException e) {
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

        BatchDataOperationUtil.batchAddNewEntities(Fire911CallConceptionType,_Fire911CallEntityValueList,16);
    }

    public static void linkData() throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        //Part 2 link to time
        com.github.tgda.engine.core.term.Type type = coreRealm.getType(Fire911CallConceptionType);
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(10000000);
        List<String> attributeNamesList = new ArrayList<>();
        attributeNamesList.add(Datetime);
        EntitiesAttributesRetrieveResult conceptionEntitiesAttributeResult =  type.getSingleValueEntityAttributesByAttributeNames(attributeNamesList,queryParameters);
        conceptionEntitiesAttributeResult.getEntityValues();

        List<EntityValue> entityValueList = conceptionEntitiesAttributeResult.getEntityValues();
        BatchDataOperationUtil.batchAttachTimeScaleEvents(entityValueList,Datetime,"occurredAt",null, TimeFlow.TimeScaleGrade.MINUTE,10);
    }
}
