package com.github.tgda.example.generator;

import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.term.Type;
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

public class RoadWeatherInformationStationsRecords_Realm_Generator {

    private static final String RoadWeatherInformationStationsRecordsConceptionType = "RoadWeatherRecords";
    private static final String StationName = "stationName";
    private static final String StationLocation = "stationLocation";
    private static final String RecordDateTime = "dateTime";
    private static final String RecordId = "recordId";
    private static final String RoadSurfaceTemperature = "roadSurfaceTemperature";
    private static final String AirTemperature = "airTemperature";

    public static void main(String[] args) throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        createData();
        linkData();
    }

    private static void createData() throws EngineServiceRuntimeException {
        Engine coreRealm = EngineFactory.getDefaultEngine();

        //Part 1
        Type _WeatherInformationStationsRecordType = coreRealm.getType(RoadWeatherInformationStationsRecordsConceptionType);
        if(_WeatherInformationStationsRecordType != null){
            coreRealm.removeType(RoadWeatherInformationStationsRecordsConceptionType,true);
        }
        _WeatherInformationStationsRecordType = coreRealm.getType(RoadWeatherInformationStationsRecordsConceptionType);
        if(_WeatherInformationStationsRecordType == null){
            _WeatherInformationStationsRecordType = coreRealm.createType(RoadWeatherInformationStationsRecordsConceptionType,"道路天气信息记录");
        }

        SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss aa");
        List<EntityValue> _WeatherInformationStationsRecordEntityValueList = Lists.newArrayList();
        //Please unzip Road_Weather_Information_Stations_huge.csv.zip before execute
        File file = new File("realmExampleData/road_weather_information_stations_records/Road_Weather_Information_Stations_huge.csv");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                String currentLine = !tempStr.startsWith("StationName,StationLocation,DateTime,")? tempStr : null;
                if(currentLine != null){
                    String stationName = null;
                    String stationLocation = null;
                    String dateTime = null;
                    String recordId = null;
                    String roadSurfaceTemperature = null;
                    String airTemperature = null;

                    String[] dataItems =  currentLine.split(",");
                    stationName = dataItems[0].trim();
                    stationLocation = dataItems[1].trim();
                    dateTime = dataItems[2].trim();
                    recordId = dataItems[3].trim();
                    roadSurfaceTemperature = dataItems[4].trim();
                    airTemperature = dataItems[5].trim();

                    Map<String,Object> newEntityValueMap = new HashMap<>();
                    newEntityValueMap.put(StationName,stationName);
                    newEntityValueMap.put(StationLocation,stationLocation);
                    Date date = sdf.parse(dateTime);
                    newEntityValueMap.put(RecordDateTime,date);
                    newEntityValueMap.put(RecordId,recordId);
                    newEntityValueMap.put(RoadSurfaceTemperature,Double.valueOf(roadSurfaceTemperature));
                    newEntityValueMap.put(AirTemperature,Double.valueOf(airTemperature));

                    newEntityValueMap.put("TGDA_GS_GLGeometryContent",stationLocation);
                    newEntityValueMap.put("TGDA_GS_GeometryType","POINT");
                    newEntityValueMap.put("TGDA_GS_GlobalCRSAID","EPSG:4326");

                    EntityValue entityValue = new EntityValue(newEntityValueMap);
                    _WeatherInformationStationsRecordEntityValueList.add(entityValue);
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
        BatchDataOperationUtil.batchAddNewEntities(RoadWeatherInformationStationsRecordsConceptionType,_WeatherInformationStationsRecordEntityValueList,10);
    }

    private static void linkData() throws EngineServiceEntityExploreException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        //Part 2 link to time
        Type type = coreRealm.getType(RoadWeatherInformationStationsRecordsConceptionType);
        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(10000000);
        List<String> attributeNamesList = new ArrayList<>();
        attributeNamesList.add(RecordDateTime);
        EntitiesAttributesRetrieveResult conceptionEntitiesAttributeResult =  type.getSingleValueEntityAttributesByAttributeNames(attributeNamesList,queryParameters);

        List<EntityValue> entityValueList = conceptionEntitiesAttributeResult.getEntityValues();
        BatchDataOperationUtil.batchAttachTimeScaleEvents(entityValueList,RecordDateTime,"recordedAt",null, TimeFlow.TimeScaleGrade.MINUTE,10);
    }
}
