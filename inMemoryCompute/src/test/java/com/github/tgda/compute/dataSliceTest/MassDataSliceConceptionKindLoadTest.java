package com.github.tgda.compute.dataSliceTest;

import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.compute.applicationCapacity.compute.dataComputeUnit.dataService.DataSlicePropertyType;
import com.github.tgda.compute.applicationCapacity.compute.dataComputeUnit.util.CoreRealmOperationUtil;

import java.util.HashMap;
import java.util.Map;

public class MassDataSliceConceptionKindLoadTest {

    public static void main(String[] args){
        //refreshDataSliceAndLoadChinaFirmTest();
        //refreshDataSliceAndLoadTGDA_GS_CountryRegionTest();
        //refreshDataSliceAndLoadTGDA_GS_ProvinceTest();
        //refreshDataSliceAndLoadTGDA_GS_PrefectureTest();
        //refreshDataSliceAndLoadTGDA_GS_CountyTest();
    }

    public static void refreshDataSliceAndLoadChinaFirmTest(){
        Map<String, DataSlicePropertyType> dataSlicePropertyMap = new HashMap<>();
        dataSlicePropertyMap.put("TGDA_GS_GLGeometryContent",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("address",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("city",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("companyType",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("province",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("lng_wgs",DataSlicePropertyType.DOUBLE);
        dataSlicePropertyMap.put("lat_wgs",DataSlicePropertyType.DOUBLE);
        dataSlicePropertyMap.put("approvedTime",DataSlicePropertyType.DATE);
        dataSlicePropertyMap.put("name",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("category",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("startDate",DataSlicePropertyType.DATE);
        dataSlicePropertyMap.put("status",DataSlicePropertyType.STRING);

        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(100000000);

        CoreRealmOperationUtil.refreshDataSliceAndLoadDataFromConceptionKind("defaultSliceGroup",
                "ChinaFirm",dataSlicePropertyMap,"ChinaFirm",queryParameters,10);
    }

    public static void refreshDataSliceAndLoadTGDA_GS_CountryRegionTest(){
        Map<String, DataSlicePropertyType> dataSlicePropertyMap = new HashMap<>();
        dataSlicePropertyMap.put("TGDA_GS_GLGeometryContent",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("TGDA_GeospatialChineseName",DataSlicePropertyType.STRING);

        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(100000000);

        CoreRealmOperationUtil.refreshDataSliceAndLoadDataFromConceptionKind("defaultSliceGroup",
                "TGDA_GS_CountryRegion",dataSlicePropertyMap,"TGDA_GS_CountryRegion",queryParameters,10);
    }

    public static void refreshDataSliceAndLoadTGDA_GS_ProvinceTest(){
        Map<String, DataSlicePropertyType> dataSlicePropertyMap = new HashMap<>();
        dataSlicePropertyMap.put("TGDA_GS_GLGeometryContent",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("TGDA_GeospatialChineseName",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("TGDA_GeospatialCode",DataSlicePropertyType.STRING);

        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(100000000);

        CoreRealmOperationUtil.refreshDataSliceAndLoadDataFromConceptionKind("defaultSliceGroup",
                "TGDA_GS_Province",dataSlicePropertyMap,"TGDA_GS_Province",queryParameters,10);
    }

    public static void refreshDataSliceAndLoadTGDA_GS_PrefectureTest(){
        Map<String, DataSlicePropertyType> dataSlicePropertyMap = new HashMap<>();
        dataSlicePropertyMap.put("TGDA_GS_GLGeometryContent",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("ChinaProvinceName",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("TGDA_GeospatialChineseName",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("ChinaDivisionCode",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("TGDA_GeospatialCode",DataSlicePropertyType.STRING);

        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(100000000);

        CoreRealmOperationUtil.refreshDataSliceAndLoadDataFromConceptionKind("defaultSliceGroup",
                "TGDA_GS_Prefecture",dataSlicePropertyMap,"TGDA_GS_Prefecture",queryParameters,10);
    }

    public static void refreshDataSliceAndLoadTGDA_GS_CountyTest(){
        Map<String, DataSlicePropertyType> dataSlicePropertyMap = new HashMap<>();
        dataSlicePropertyMap.put("TGDA_GS_GLGeometryContent",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("ChinaProvinceName",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("ChinaPrefectureName",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("TGDA_GeospatialChineseName",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("ChinaDivisionCode",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("ChinaParentDivisionCode",DataSlicePropertyType.STRING);
        dataSlicePropertyMap.put("TGDA_GeospatialCode",DataSlicePropertyType.STRING);

        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setResultNumber(100000000);

        CoreRealmOperationUtil.refreshDataSliceAndLoadDataFromConceptionKind("defaultSliceGroup",
                "TGDA_GS_County",dataSlicePropertyMap,"TGDA_GS_County",queryParameters,10);
    }


}
