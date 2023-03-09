package com.github.tgda.compute.consoleApplication;

import com.github.tgda.compute.applicationCapacity.compute.DataComputeApplication;
import com.github.tgda.compute.consoleApplication.feature.BaseApplication;

public class ApplicationCapacityRegistry {
    public static BaseApplication createConsoleApplication(String applicationFunctionName){
        if(applicationFunctionName.equals("dataComputeApplication")){
            return new DataComputeApplication();
        }
        return null;
    }
}
