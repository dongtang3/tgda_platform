package com.github.tgda.knowledgeManage.applicationService.eventStreaming;

import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.ConfigurationErrorException;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.MessageFormatErrorException;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.MessageHandleErrorException;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.SchemaFormatErrorException;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationContent;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationType;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.sender.EntityValueOperationsMessageSender;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.sender.MessageSentEventHandler;

import java.net.InetAddress;
import java.util.Date;
import java.util.List;

public class EventStreamingService {

    private static String senderId =null;
    private static String senderIP =null;

    static{
        InetAddress ia=null;
        try {
            ia=ia.getLocalHost();
            senderId=ia.getHostName();
            senderIP=ia.getHostAddress();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void newConceptionEntities(String coreRealmName, String conceptionKindName, List<EntityValue> entityValueList, boolean addPerDefinedRelation, MessageSentEventHandler messageSentEventHandler){
        EntityValueOperationsMessageSender conceptionEntityValueOperationsMessageSender;
        try {
            conceptionEntityValueOperationsMessageSender = new EntityValueOperationsMessageSender(messageSentEventHandler);
            conceptionEntityValueOperationsMessageSender.beginMessageSendBatch();
            for(EntityValue currentEntityValue : entityValueList){
                EntityValueOperationContent conceptionEntityValueOperationContent = generateOperationContent(coreRealmName,conceptionKindName);
                conceptionEntityValueOperationContent.setOperationType(EntityValueOperationType.INSERT);
                conceptionEntityValueOperationContent.setAddPerDefinedRelation(addPerDefinedRelation);
                conceptionEntityValueOperationsMessageSender.sendEntityValueOperationMessage(conceptionEntityValueOperationContent, currentEntityValue);
            }
            conceptionEntityValueOperationsMessageSender.finishMessageSendBatch();
        } catch (ConfigurationErrorException e) {
            e.printStackTrace();
        } catch (MessageFormatErrorException e) {
            e.printStackTrace();
        } catch (MessageHandleErrorException e) {
            e.printStackTrace();
        } catch (SchemaFormatErrorException e) {
            e.printStackTrace();
        }
    }

    public static void  updateConceptionEntities(String coreRealmName, String conceptionKindName, List<EntityValue> entityValueList, MessageSentEventHandler messageSentEventHandler){
        EntityValueOperationsMessageSender conceptionEntityValueOperationsMessageSender;
        try {
            conceptionEntityValueOperationsMessageSender = new EntityValueOperationsMessageSender(messageSentEventHandler);
            conceptionEntityValueOperationsMessageSender.beginMessageSendBatch();
            for(EntityValue currentEntityValue : entityValueList){
                EntityValueOperationContent conceptionEntityValueOperationContent = generateOperationContent(coreRealmName,conceptionKindName);
                conceptionEntityValueOperationContent.setOperationType(EntityValueOperationType.UPDATE);
                conceptionEntityValueOperationContent.setEntityUID(currentEntityValue.getEntityUID());
                conceptionEntityValueOperationsMessageSender.sendEntityValueOperationMessage(conceptionEntityValueOperationContent, currentEntityValue);
            }
            conceptionEntityValueOperationsMessageSender.finishMessageSendBatch();
        } catch (ConfigurationErrorException e) {
            e.printStackTrace();
        } catch (MessageFormatErrorException e) {
            e.printStackTrace();
        } catch (MessageHandleErrorException e) {
            e.printStackTrace();
        } catch (SchemaFormatErrorException e) {
            e.printStackTrace();
        }
    }

    public static void  deleteConceptionEntities(String coreRealmName,String conceptionKindName, List<String> conceptionEntityUIDList,MessageSentEventHandler messageSentEventHandler){
        EntityValueOperationsMessageSender conceptionEntityValueOperationsMessageSender;
        try {
            conceptionEntityValueOperationsMessageSender = new EntityValueOperationsMessageSender(messageSentEventHandler);
            conceptionEntityValueOperationsMessageSender.beginMessageSendBatch();
            for(String currentEntityUID:conceptionEntityUIDList){
                EntityValueOperationContent conceptionEntityValueOperationContent = generateOperationContent(coreRealmName,conceptionKindName);
                conceptionEntityValueOperationContent.setOperationType(EntityValueOperationType.DELETE);
                conceptionEntityValueOperationContent.setEntityUID(currentEntityUID);
                EntityValue currentEntityValue = new EntityValue();
                currentEntityValue.setEntityUID(currentEntityUID);
                conceptionEntityValueOperationsMessageSender.sendEntityValueOperationMessage(conceptionEntityValueOperationContent, currentEntityValue);
            }
            conceptionEntityValueOperationsMessageSender.finishMessageSendBatch();
        } catch (ConfigurationErrorException e) {
            e.printStackTrace();
        } catch (MessageFormatErrorException e) {
            e.printStackTrace();
        } catch (MessageHandleErrorException e) {
            e.printStackTrace();
        } catch (SchemaFormatErrorException e) {
            e.printStackTrace();
        }
    }

    private static EntityValueOperationContent generateOperationContent(String coreRealmName,String conceptionKindName){
        EntityValueOperationContent conceptionEntityValueOperationContent =new EntityValueOperationContent();
        conceptionEntityValueOperationContent.setCoreRealmName(coreRealmName);
        conceptionEntityValueOperationContent.setConceptionKindName(conceptionKindName);
        conceptionEntityValueOperationContent.setSenderId(senderId);
        conceptionEntityValueOperationContent.setSenderIP(senderIP);
        conceptionEntityValueOperationContent.setSendTime(new Date().getTime());
        return conceptionEntityValueOperationContent;
    }
}
