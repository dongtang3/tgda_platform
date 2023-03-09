package com.github.tgda.knowledgeManage.applicationCapacity.entityExtraction.commandProcessor;

import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.receiver.EntityValueOperationsMessageReceiver;
import com.github.tgda.knowledgeManage.consoleApplication.feature.BaseCommandProcessor;

import java.util.Map;

public class EntityExtractionCommandProcessorFactory {

    public static BaseCommandProcessor getCommandProcessor(String command, EntityValueOperationsMessageReceiver conceptionEntityValueOperationsMessageReceiver,Map<Object, Object> commandContextDataMap){
        if(command.equalsIgnoreCase("help")){
            HelpCommandProcessor helpCommandProcessor = new HelpCommandProcessor();
            return helpCommandProcessor;
        }if(command.equalsIgnoreCase("appinf")){
            AppInfCommandProcessor appInfCommandProcessor = new AppInfCommandProcessor(conceptionEntityValueOperationsMessageReceiver,commandContextDataMap);
            return appInfCommandProcessor;
        }if(command.equalsIgnoreCase("clear")){
            ClearCommandProcessor clearCommandProcessor = new ClearCommandProcessor();
            return clearCommandProcessor;
        }
        if(command.equalsIgnoreCase("ophistory")){
            OpHistoryCommandProcessor opHistoryCommandProcessor = new OpHistoryCommandProcessor(conceptionEntityValueOperationsMessageReceiver,commandContextDataMap);
            return opHistoryCommandProcessor;
        }
        return null;
    }
}
