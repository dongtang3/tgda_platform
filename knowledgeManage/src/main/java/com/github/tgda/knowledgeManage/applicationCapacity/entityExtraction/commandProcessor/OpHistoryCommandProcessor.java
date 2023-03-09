package com.github.tgda.knowledgeManage.applicationCapacity.entityExtraction.commandProcessor;

import com.github.tgda.knowledgeManage.applicationCapacity.entityExtraction.EntityExtractionApplication;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.receiver.EntityValueOperationsMessageReceiver;
import com.github.tgda.knowledgeManage.consoleApplication.feature.BaseCommandProcessor;

import java.util.List;
import java.util.Map;

public class OpHistoryCommandProcessor  implements BaseCommandProcessor {

    private EntityValueOperationsMessageReceiver conceptionEntityValueOperationsMessageReceiver;
    private Map<Object,Object> commandContextDataMap;

    public OpHistoryCommandProcessor(EntityValueOperationsMessageReceiver conceptionEntityValueOperationsMessageReceiver,
                                  Map<Object,Object> commandContextDataMap){
        this.conceptionEntityValueOperationsMessageReceiver = conceptionEntityValueOperationsMessageReceiver;
        this.commandContextDataMap = commandContextDataMap;
    }

    @Override
    public void processCommand(String command, String[] commandOptions) {
        List<String> messageReceiveHistoryList = (List<String>)this.commandContextDataMap.get(EntityExtractionApplication.MESSAGE_RECEIVE_HISTORY);

        StringBuffer historyInfoMessageStringBuffer=new StringBuffer();
        historyInfoMessageStringBuffer.append("\n\r");
        historyInfoMessageStringBuffer.append("================================================================");
        historyInfoMessageStringBuffer.append("\n\r");
        historyInfoMessageStringBuffer.append("-------------------------------------------------------------");
        for(String currentHistoryItem : messageReceiveHistoryList){
            historyInfoMessageStringBuffer.append("\n\r");
            historyInfoMessageStringBuffer.append(currentHistoryItem);
            historyInfoMessageStringBuffer.append("\n\r");
            historyInfoMessageStringBuffer.append("-------------------------------------------------------------");

        }
        historyInfoMessageStringBuffer.append("\n\r");
        historyInfoMessageStringBuffer.append("================================================================");
        System.out.println(historyInfoMessageStringBuffer.toString());
    }
}
