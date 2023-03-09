package com.github.tgda.knowledgeManage.consoleApplication;

import com.github.tgda.knowledgeManage.applicationCapacity.dataSlicesSynchronization.DataSlicesSynchronizationApplication;
import com.github.tgda.knowledgeManage.applicationCapacity.entityDisambiguation.EntityDisambiguationApplication;
import com.github.tgda.knowledgeManage.applicationCapacity.entityExtraction.EntityExtractionApplication;
import com.github.tgda.knowledgeManage.applicationCapacity.entityFusion.EntityFusionApplication;
import com.github.tgda.knowledgeManage.applicationCapacity.relationExtraction.RelationExtractionApplication;
import com.github.tgda.knowledgeManage.consoleApplication.feature.BaseApplication;

public class ApplicationCapacityRegistry {
    public static BaseApplication createConsoleApplication(String applicationFunctionName){
        if(applicationFunctionName.equals("relationExtraction")){
            return new RelationExtractionApplication();
        }
        if(applicationFunctionName.equals("entityExtraction")){
            return new EntityExtractionApplication();
        }
        if(applicationFunctionName.equals("entityFusion")){
            return new EntityFusionApplication();
        }
        if(applicationFunctionName.equals("entityDisambiguation")){
            return new EntityDisambiguationApplication();
        }
        if(applicationFunctionName.equals("dataSlicesSynchronization")){
            return new DataSlicesSynchronizationApplication();
        }
        return null;
    }
}
