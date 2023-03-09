package com.github.tgda.knowledgeManage.applicationCapacity.entityExtraction;

import com.github.tgda.knowledgeManage.applicationCapacity.entityExtraction.commandProcessor.EntityExtractionCommandProcessorFactory;
import com.github.tgda.knowledgeManage.applicationCapacity.entityExtraction.conceptionEntitiesExtract.GeneralEntityValueOperationsMessageHandler;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.ConfigurationErrorException;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.MessageHandleErrorException;

import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.receiver.EntityValueOperationsMessageReceiver;
import com.github.tgda.knowledgeManage.consoleApplication.feature.BaseApplication;
import com.github.tgda.knowledgeManage.consoleApplication.feature.BaseCommandProcessor;
import com.github.tgda.knowledgeManage.consoleApplication.util.ApplicationLauncherUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EntityExtractionApplication implements BaseApplication {

    public static final String APPLICATION_START_TIME = "APPLICATION_START_TIME";
    public static final String MESSAGE_RECEIVE_HISTORY = "MESSAGE_RECEIVE_HISTORY";

    private EntityValueOperationsMessageReceiver conceptionEntityValueOperationsMessageReceiver;
    private ExecutorService executorService;
    private Map<Object,Object> commandContextDataMap;

    @Override
    public boolean isDaemonApplication() {
        return true;
    }

    @Override
    public void executeDaemonLogic() {
        GeneralEntityValueOperationsMessageHandler generalEntityValueOperationsMessageHandler = new GeneralEntityValueOperationsMessageHandler(this.commandContextDataMap);
        try {
            conceptionEntityValueOperationsMessageReceiver = new EntityValueOperationsMessageReceiver(generalEntityValueOperationsMessageHandler);
        } catch (ConfigurationErrorException e) {
            e.printStackTrace();
        }
        executorService = Executors.newFixedThreadPool(1);
        executorService.submit(new Runnable() {
            public void run() {
                try {
                    String defaultMessageReceiverTopicName = ApplicationLauncherUtil.getApplicationInfoPropertyValue("EntityExtraction.MessageReceiver.defaultTopicName");
                    conceptionEntityValueOperationsMessageReceiver.startMessageReceive(new String[]{defaultMessageReceiverTopicName});
                } catch (ConfigurationErrorException e) {
                    e.printStackTrace();
                } catch (MessageHandleErrorException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ApplicationLauncherUtil.printApplicationConsoleBanner();
    }

    @Override
    public boolean initApplication() {
        this.commandContextDataMap = new ConcurrentHashMap<>();
        this.commandContextDataMap.put(APPLICATION_START_TIME,new Date());
        this.commandContextDataMap.put(MESSAGE_RECEIVE_HISTORY,new ArrayList<String>());
        return true;
    }

    @Override
    public boolean shutdownApplication() {
        if(conceptionEntityValueOperationsMessageReceiver != null){
            conceptionEntityValueOperationsMessageReceiver.stopMessageReceive();
        }
        if(executorService != null){
            executorService.shutdown();
        }
        return true;
    }

    @Override
    public void executeConsoleCommand(String consoleCommand) {
        if(consoleCommand != null){
            String[] commandOptions = consoleCommand.split(" ");
            if(commandOptions.length>0){
                String command = commandOptions[0];
                if(command.startsWith("-")||command.startsWith("--")){
                    System.out.println("Please input valid command and options");
                }else{
                    String[] options = Arrays.copyOfRange(commandOptions,1,commandOptions.length);
                    BaseCommandProcessor commandProcessor = EntityExtractionCommandProcessorFactory.getCommandProcessor(command,conceptionEntityValueOperationsMessageReceiver,this.commandContextDataMap);
                    if(commandProcessor!=null){
                        commandProcessor.processCommand(command,options);
                    }
                }
            }
        }
    }
}
