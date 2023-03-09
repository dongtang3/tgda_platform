package providerClientTest;

import com.github.tgda.supplier.client.AnalysisProviderClient;
import com.github.tgda.supplier.client.exception.AnalysisEngineRuntimeException;
import com.github.tgda.supplier.client.exception.ProviderClientInitException;
import com.github.tgda.supplier.feature.communication.AnalyseResponseCallback;
import com.github.tgda.supplier.feature.communication.messagePayload.AnalyseResponse;
import com.github.tgda.supplier.feature.communication.messagePayload.ResponseDataset;
import com.github.tgda.supplier.feature.communication.messagePayload.spatialAnalysis.SpatialPropertiesAggregateStatisticRequest;
import com.github.tgda.supplier.feature.communication.messagePayload.spatialAnalysis.SpatialCommonConfig;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class SpatialPropertiesStatisticTest01 {

    public static void main(String[] args){
        AnalysisProviderClient analysisProviderClient = new AnalysisProviderClient("127.0.0.1",9999);
        analysisProviderClient.openSession();

        SpatialPropertiesAggregateStatisticRequest spatialPropertiesAggregateStatisticRequest = new SpatialPropertiesAggregateStatisticRequest();
        spatialPropertiesAggregateStatisticRequest.setSubjectConception("CommunityReportingArea");
        spatialPropertiesAggregateStatisticRequest.setObjectConception("TreeCanopy");
        spatialPropertiesAggregateStatisticRequest.setPredicateType(SpatialCommonConfig.PredicateType.Contains);

        spatialPropertiesAggregateStatisticRequest.setSubjectIdentityProperty("OBJECTID");
        spatialPropertiesAggregateStatisticRequest.setSubjectCalculationProperty("SHAPE_AREA");
        spatialPropertiesAggregateStatisticRequest.setObjectCalculationProperty("SHAPE_AREA");
        spatialPropertiesAggregateStatisticRequest.setObjectAggregationType(SpatialPropertiesAggregateStatisticRequest.ObjectAggregationType.SUM);

        spatialPropertiesAggregateStatisticRequest.setSubjectReturnProperties(new String[]{"GEN_ALIAS","NEIGHDIST","DETL_NAMES"});
        spatialPropertiesAggregateStatisticRequest.setCalculationOperator(SpatialPropertiesAggregateStatisticRequest.CalculationOperator.Divide);
        spatialPropertiesAggregateStatisticRequest.setStatisticResultProperty("CalculationResult");

        try {
            System.out.println(new Date());
            AnalyseResponseCallback analyseResponseCallback = new AnalyseResponseCallback() {
                @Override
                public void onResponseReceived(Object analyseResponseObject) {
                    System.out.println(analyseResponseObject);
                    System.out.println(new Date());

                    try {
                        analysisProviderClient.closeSession();
                    } catch (ProviderClientInitException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onSuccessResponseReceived(AnalyseResponse analyseResponse) {
                    System.out.println(analyseResponse);
                    System.out.println(analyseResponse.getResponseUUID());
                    System.out.println(analyseResponse.getResponseDateTime());
                    System.out.println(analyseResponse.getRequestUUID());
                    System.out.println(analyseResponse.getResponseData());


                    ResponseDataset responseDataset = (ResponseDataset)analyseResponse.getResponseData();
                    Map<String,String> propertiesInfoMap =  responseDataset.getPropertiesInfo();
                    ArrayList<HashMap<String,Object>> datalist = responseDataset.getDataList();

                    for(HashMap<String,Object> currentDataRow : datalist){
                        System.out.println(currentDataRow);
                    }
                    System.out.println();
                    System.out.println(propertiesInfoMap);

                    try {
                        analysisProviderClient.closeSession();
                    } catch (ProviderClientInitException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void onFailureResponseReceived(Throwable throwable) {
                    System.out.println(throwable);
                    System.out.println(new Date());
                    try {
                        analysisProviderClient.closeSession();
                    } catch (ProviderClientInitException e) {
                        e.printStackTrace();
                    }
                }
            };

            analysisProviderClient.sendAnalyseRequest(spatialPropertiesAggregateStatisticRequest,analyseResponseCallback,600);

        } catch (AnalysisEngineRuntimeException | ProviderClientInitException e) {
            e.printStackTrace();
        }

        /*
        try {
            Thread.sleep(10000);
            analysisProviderClient.closeSession();
        } catch (ProviderClientInitException | InterruptedException e) {
            e.printStackTrace();
        }
        */
    }
}
