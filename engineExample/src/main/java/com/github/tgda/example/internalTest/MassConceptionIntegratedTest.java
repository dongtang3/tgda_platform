package com.github.tgda.example.internalTest;

import com.github.tgda.engine.core.payload.EntityValue;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.util.BatchDataOperationUtil;
import com.github.tgda.engine.core.payload.EntitiesAttributesRetrieveResult;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.term.TimeFlow;
import com.github.tgda.engine.core.util.factory.EngineFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MassConceptionIntegratedTest {

    private static final String ChinaFirmConceptionType = "ChinaFirm";
    private static final String Name = "name";
    private static final String CompanyType = "companyType";
    private static final String Address = "address";
    private static final String Status = "status";
    private static final String StartDate = "startDate";
    private static final String ApprovedTime = "approvedTime";
    private static final String Category = "category";
    private static final String City = "city";
    private static final String Province = "province";
    private static final String Lat_wgs = "lat_wgs";
    private static final String Lng_wgs = "lng_wgs";

    public static void main(String[] args) throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        //generateData();
        //linkApprovedAtTimeData(701,800);
        linkStartedDateAtTimeData(701,800);

    }

    public static void linkApprovedAtTimeData(int fromPage,int toPage) throws EngineServiceEntityExploreException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        Type _ChinaFirmType = coreRealm.getType(ChinaFirmConceptionType);
        if(_ChinaFirmType != null){
            QueryParameters queryParameters = new QueryParameters();
            queryParameters.addSortingAttribute("lastModifyDate", QueryParameters.SortingLogic.ASC);
            queryParameters.setStartPage(fromPage);
            queryParameters.setEndPage(toPage);
            queryParameters.setPageSize(10000);
            List<String> attributeNamesList = new ArrayList<>();
            attributeNamesList.add(ApprovedTime);
            EntitiesAttributesRetrieveResult conceptionEntitiesAttributeResult = _ChinaFirmType.getSingleValueEntityAttributesByAttributeNames(attributeNamesList,queryParameters);

            List<EntityValue> entityValueList = conceptionEntitiesAttributeResult.getEntityValues();
            BatchDataOperationUtil.batchAttachTimeScaleEvents(entityValueList,ApprovedTime,"approvedAt",null, TimeFlow.TimeScaleGrade.DAY,10);
        }
    }

    public static void linkStartedDateAtTimeData(int fromPage,int toPage) throws EngineServiceEntityExploreException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        Type _ChinaFirmType = coreRealm.getType(ChinaFirmConceptionType);
        if(_ChinaFirmType != null){
            QueryParameters queryParameters = new QueryParameters();
            queryParameters.addSortingAttribute("lastModifyDate", QueryParameters.SortingLogic.ASC);
            queryParameters.setStartPage(fromPage);
            queryParameters.setEndPage(toPage);
            queryParameters.setPageSize(10000);
            List<String> attributeNamesList = new ArrayList<>();
            attributeNamesList.add(StartDate);
            EntitiesAttributesRetrieveResult conceptionEntitiesAttributeResult = _ChinaFirmType.getSingleValueEntityAttributesByAttributeNames(attributeNamesList,queryParameters);

            List<EntityValue> entityValueList = conceptionEntitiesAttributeResult.getEntityValues();
            BatchDataOperationUtil.batchAttachTimeScaleEvents(entityValueList,StartDate,"startedAt",null, TimeFlow.TimeScaleGrade.DAY,10);
        }
    }

    public static void generateData() throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        Engine coreRealm = EngineFactory.getDefaultEngine();

        //Part 1
        Type _ChinaFirmType = coreRealm.getType(ChinaFirmConceptionType);
        if(_ChinaFirmType != null){
            coreRealm.removeType(ChinaFirmConceptionType,true);
        }
        _ChinaFirmType = coreRealm.getType(ChinaFirmConceptionType);
        if(_ChinaFirmType == null){
            _ChinaFirmType = coreRealm.createType(ChinaFirmConceptionType,"中国公司");
        }

        List<EntityValue> _ChinaFirmConceptionKindEntityValueList = Lists.newArrayList();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        File file = new File("/media/wangychu/Data/Data/firm_2015.csv"); //2010,2015
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {
                String currentLine = !tempStr.startsWith("name\tcompany_type")? tempStr : null;
                if(currentLine != null){
                    String[] dataItems =  currentLine.split("\t");
                    if(dataItems.length == 11){
                        String name = dataItems[0];
                        String companyType = dataItems[1];
                        String address = dataItems[2];
                        String status = dataItems[3];
                        String startDate = dataItems[4];
                        String approvedTime = dataItems[5];
                        String category = dataItems[6];
                        String city = dataItems[7];
                        String province = dataItems[8];
                        String lat_wgs = dataItems[9];
                        String lng_wgs = dataItems[10];
                        Map<String,Object> newEntityValueMap = new HashMap<>();
                        newEntityValueMap.put(Name,name);
                        newEntityValueMap.put(CompanyType,companyType);
                        newEntityValueMap.put(Address,address);
                        newEntityValueMap.put(Status,status);
                        if(!startDate.equals("")){
                            newEntityValueMap.put(StartDate,sdf.parse(startDate));
                        }
                        if(!approvedTime.equals("")){
                            newEntityValueMap.put(ApprovedTime,sdf.parse(approvedTime));
                        }
                        newEntityValueMap.put(Category,category);
                        newEntityValueMap.put(City,city);
                        newEntityValueMap.put(Province,province);
                        newEntityValueMap.put(Lat_wgs,Double.parseDouble(lat_wgs));
                        newEntityValueMap.put(Lng_wgs,Double.parseDouble(lng_wgs));

                        String wktContent = "POINT ("+lng_wgs+" "+lat_wgs+")";
                        newEntityValueMap.put("TGDA_GS_GeometryType","POINT");
                        newEntityValueMap.put("TGDA_GS_GlobalCRSAID","EPSG:4326");
                        newEntityValueMap.put("TGDA_GS_GLGeometryContent",wktContent);

                        EntityValue entityValue = new EntityValue(newEntityValueMap);
                        _ChinaFirmConceptionKindEntityValueList.add(entityValue);
                    }
                    if(dataItems.length == 10){
                        String name = "-";
                        String companyType = "-";
                        String address = dataItems[1];
                        String status = dataItems[2];
                        String startDate = dataItems[3];
                        String approvedTime = dataItems[4];
                        String category = dataItems[5];
                        String city = dataItems[6];
                        String province = dataItems[7];
                        String lat_wgs = dataItems[8];
                        String lng_wgs = dataItems[9];
                        Map<String,Object> newEntityValueMap = new HashMap<>();
                        newEntityValueMap.put(Name,name);
                        newEntityValueMap.put(CompanyType,companyType);
                        newEntityValueMap.put(Address,address);
                        newEntityValueMap.put(Status,status);
                        if(!startDate.equals("")){
                            newEntityValueMap.put(StartDate,sdf.parse(startDate));
                        }
                        if(!approvedTime.equals("")){
                            newEntityValueMap.put(ApprovedTime,sdf.parse(approvedTime));
                        }
                        newEntityValueMap.put(Category,category);
                        newEntityValueMap.put(City,city);
                        newEntityValueMap.put(Province,province);
                        newEntityValueMap.put(Lat_wgs,Double.parseDouble(lat_wgs));
                        newEntityValueMap.put(Lng_wgs,Double.parseDouble(lng_wgs));

                        String wktContent = "POINT ("+lng_wgs+" "+lat_wgs+")";
                        newEntityValueMap.put("TGDA_GS_GeometryType","POINT");
                        newEntityValueMap.put("TGDA_GS_GlobalCRSAID","EPSG:4326");
                        newEntityValueMap.put("TGDA_GS_GLGeometryContent",wktContent);

                        EntityValue entityValue = new EntityValue(newEntityValueMap);
                        _ChinaFirmConceptionKindEntityValueList.add(entityValue);
                    }
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

        BatchDataOperationUtil.batchAddNewEntities(ChinaFirmConceptionType,_ChinaFirmConceptionKindEntityValueList,10);
    }
}
