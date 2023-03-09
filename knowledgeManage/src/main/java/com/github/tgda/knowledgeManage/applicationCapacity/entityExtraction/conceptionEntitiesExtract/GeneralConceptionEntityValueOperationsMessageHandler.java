package com.github.tgda.knowledgeManage.applicationCapacity.entityExtraction.conceptionEntitiesExtract;

import com.github.tgda.engine.core.internal.neo4j.util.BatchDataOperationUtil;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import com.github.tgda.knowledgeManage.applicationCapacity.entityExtraction.EntityExtractionApplication;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationContent;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationPayload;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationType;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.receiver.EntityValueOperationsMessageHandler;
import com.github.tgda.knowledgeManage.consoleApplication.util.ApplicationLauncherUtil;

import java.util.*;

public class GeneralEntityValueOperationsMessageHandler extends EntityValueOperationsMessageHandler {

    private Map<Object,Object> commandContextDataMap;
    private int degreeOfParallelism = 1;
    private boolean autoCreateConceptionKind = false;
    private List<String> existConceptionKindList;

    public GeneralEntityValueOperationsMessageHandler(Map<Object,Object> commandContextDataMap){
        this.commandContextDataMap = commandContextDataMap;
        String degreeOfParallelismStr = ApplicationLauncherUtil.getApplicationInfoPropertyValue("EntityExtraction.MessageHandle.degreeOfParallelism");
        this.degreeOfParallelism = Integer.valueOf(degreeOfParallelismStr);
        String autoCreateConceptionKindStr = ApplicationLauncherUtil.getApplicationInfoPropertyValue("EntityExtraction.MessageHandle.autoCreateConceptionKind");
        this.autoCreateConceptionKind = Boolean.valueOf(autoCreateConceptionKindStr);
        this.existConceptionKindList = new ArrayList<>();
    }

    @Override
    public void handleEntityOperationContents(List<? extends EntityValueOperationPayload> conceptionEntityValueOperationPayloads) {

        Date currentDate = new Date();
        Map<String,List<EntityValue>> conceptionEntitiesKindGroupMap_INSERT = new HashMap<>();
        Map<String,List<EntityValue>> conceptionEntitiesKindGroupMap_UPDATE = new HashMap<>();
        Map<String,List<String>> conceptionEntitiesKindGroupMap_DELETE = new HashMap<>();

        long fromOffset = 0;
        long toOffset = 0;

        for(int i = 0;i < conceptionEntityValueOperationPayloads.size(); i++){
            EntityValueOperationPayload currentEntityValueOperationPayload = conceptionEntityValueOperationPayloads.get(i);
            EntityValueOperationContent conceptionEntityValueOperationContent = currentEntityValueOperationPayload.getEntityValueOperationContent();

            String targetCoreRealmName = conceptionEntityValueOperationContent.getCoreRealmName();
            String targetConceptionKindName = conceptionEntityValueOperationContent.getConceptionKindName();
            EntityValueOperationType currentDataOperationType = conceptionEntityValueOperationContent.getOperationType();
            EntityValue entityValue = currentEntityValueOperationPayload.getEntityValue();

            switch(currentDataOperationType){
                case INSERT:
                    prepareConceptionKindDataForModifyOperation(conceptionEntitiesKindGroupMap_INSERT,targetConceptionKindName, entityValue);
                    break;
                case UPDATE:
                    prepareConceptionKindDataForModifyOperation(conceptionEntitiesKindGroupMap_UPDATE,targetConceptionKindName, entityValue);
                    break;
                case DELETE:
                    prepareConceptionKindDataForDeleteOperation(conceptionEntitiesKindGroupMap_DELETE,targetConceptionKindName, entityValue);
                    break;
            }

            if(i == 0){
                fromOffset = currentEntityValueOperationPayload.getPayloadOffset();
            }
            if(i == conceptionEntityValueOperationPayloads.size() - 1){
                toOffset = currentEntityValueOperationPayload.getPayloadOffset();
            }

            /*
            System.out.println(currentEntityValueOperationPayload.getPayloadOffset());
            System.out.println(currentEntityValueOperationPayload.getPayloadKey());
            System.out.println(conceptionEntityValue.getEntityAttributesValue());
            System.out.println(conceptionEntityValue.getEntityUID());
            System.out.println(conceptionEntityValueOperationContent.getEntityUID());
            System.out.println(conceptionEntityValueOperationContent.getEntityAttributesValue());
            System.out.println(conceptionEntityValueOperationContent.getSenderId());
            System.out.println(conceptionEntityValueOperationContent.getSenderIP());
            System.out.println(conceptionEntityValueOperationContent.getSendTime());
            System.out.println(conceptionEntityValueOperationContent.isAddPerDefinedRelation());
            */
        }

        StringBuffer appInfoMessageStringBuffer=new StringBuffer();
        appInfoMessageStringBuffer.append("\n\r");
        appInfoMessageStringBuffer.append("--------------------------------------------------------------------------");
        appInfoMessageStringBuffer.append("\n\r");
        appInfoMessageStringBuffer.append("Received batch entity operation request at: "+ currentDate.toString());
        appInfoMessageStringBuffer.append("\n\r");
        appInfoMessageStringBuffer.append("Conception entity request number:           "+ conceptionEntityValueOperationPayloads.size());
        appInfoMessageStringBuffer.append("\n\r");
        appInfoMessageStringBuffer.append("--------------------------------------------------------------------------");

        System.out.println(appInfoMessageStringBuffer.toString());
        System.out.print(">_");

        List<String> messageReceiveHistoryList = (List<String>)commandContextDataMap.get(EntityExtractionApplication.MESSAGE_RECEIVE_HISTORY);

        StringBuffer currentReceiveHistoryStringBuffer=new StringBuffer();
        currentReceiveHistoryStringBuffer.append("Received operation request at: "+ currentDate.toString());
        currentReceiveHistoryStringBuffer.append("\n\r");
        currentReceiveHistoryStringBuffer.append("Entity request number:         "+ conceptionEntityValueOperationPayloads.size());
        currentReceiveHistoryStringBuffer.append("\n\r");
        currentReceiveHistoryStringBuffer.append("OffsetRage:                    "+ fromOffset + " to " + toOffset);
        messageReceiveHistoryList.add(currentReceiveHistoryStringBuffer.toString());

        Set<String> conceptionKindNameSet = conceptionEntitiesKindGroupMap_INSERT.keySet();
        Engine coreRealm = EngineFactory.getDefaultEngine();

        for(String currentConceptionKindName:conceptionKindNameSet){
            List<EntityValue> targetEntityValueList = conceptionEntitiesKindGroupMap_INSERT.get(currentConceptionKindName);
            if(existConceptionKindList.contains(currentConceptionKindName)){
                BatchDataOperationUtil.batchAddNewEntities(currentConceptionKindName, targetEntityValueList,this.degreeOfParallelism);
            }else{
                if(coreRealm.getType(currentConceptionKindName) == null){
                    if(this.autoCreateConceptionKind){
                        Type newCreatedType = coreRealm.createType(currentConceptionKindName,"AutoCreatedByEntityExtractionOperation");
                        if(newCreatedType != null){
                            existConceptionKindList.add(currentConceptionKindName);
                            BatchDataOperationUtil.batchAddNewEntities(currentConceptionKindName, targetEntityValueList,this.degreeOfParallelism);
                        }
                    }else{
                        break;
                    }
                }else{
                    existConceptionKindList.add(currentConceptionKindName);
                    BatchDataOperationUtil.batchAddNewEntities(currentConceptionKindName, targetEntityValueList,this.degreeOfParallelism);
                }
            }
        }
    }

    private void prepareConceptionKindDataForModifyOperation(Map<String,List<EntityValue>> conceptionEntitiesKindGroupMap,
                                                             String targetConceptionKindName, EntityValue entityValue){
        if(entityValue != null) {
            if (!conceptionEntitiesKindGroupMap.containsKey(targetConceptionKindName)) {
                List<EntityValue> conceptionKindEntityList = new ArrayList<>();
                conceptionEntitiesKindGroupMap.put(targetConceptionKindName, conceptionKindEntityList);
            }
            List<EntityValue> targetConceptionKindEntityList = conceptionEntitiesKindGroupMap.get(targetConceptionKindName);
            targetConceptionKindEntityList.add(entityValue);
        }
    }

    private void prepareConceptionKindDataForDeleteOperation(Map<String,List<String>> conceptionEntitiesKindGroupMap,
                                                             String targetConceptionKindName, EntityValue entityValue){
        if(entityValue != null && entityValue.getEntityUID() != null) {
            if (!conceptionEntitiesKindGroupMap.containsKey(targetConceptionKindName)) {
                List<String> conceptionKindEntityUIDList = new ArrayList<>();
                conceptionEntitiesKindGroupMap.put(targetConceptionKindName, conceptionKindEntityUIDList);
            }
            List<String> targetConceptionKindEntityUIDList = conceptionEntitiesKindGroupMap.get(targetConceptionKindName);
            targetConceptionKindEntityUIDList.add(entityValue.getEntityUID());
        }
    }
}
