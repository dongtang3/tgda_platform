package com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.receiver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationContent;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationPayload;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationType;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public abstract class EntityValueOperationsMessageHandler extends AvroMessageHandler{

    @Override
    protected void operateRecord(Object recordKey, GenericRecord recordValue, long recordOffset) {}

    @Override
    public void handleMessages(ConsumerRecords<Object, Object> consumerRecords) {
        List<EntityValueOperationPayload> conceptionEntityValueOperationPayloadList = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();
        for(ConsumerRecord<Object, Object> consumerRecord:consumerRecords){
            Object recordKey = consumerRecord.key();
            Object recordValue = consumerRecord.value();
            GenericRecord genericRecordValue=(GenericRecord)recordValue;
            long recordOffset = consumerRecord.offset();

            EntityValueOperationPayload conceptionEntityValueOperationPayload = new EntityValueOperationPayload();
            conceptionEntityValueOperationPayload.setPayloadOffset(recordOffset);
            conceptionEntityValueOperationPayload.setPayloadKey(recordKey);

            EntityValueOperationContent conceptionEntityValueOperationContent = new EntityValueOperationContent();
            conceptionEntityValueOperationPayload.setEntityValueOperationContent(conceptionEntityValueOperationContent);
            conceptionEntityValueOperationPayloadList.add(conceptionEntityValueOperationPayload);

            if(genericRecordValue.get("senderId")!=null) {
                conceptionEntityValueOperationContent.setSenderId(genericRecordValue.get("senderId").toString());
            }
            if(genericRecordValue.get("senderIP")!=null) {
                conceptionEntityValueOperationContent.setSenderIP(genericRecordValue.get("senderIP").toString());
            }
            conceptionEntityValueOperationContent.setSendTime((Long)genericRecordValue.get("sendTime"));
            conceptionEntityValueOperationContent.setConceptionKindName(genericRecordValue.get("conceptionKindName").toString());
            conceptionEntityValueOperationContent.setCoreRealmName(genericRecordValue.get("coreRealmName").toString());

            String operationTypeValue = genericRecordValue.get("operationType").toString();
            switch(operationTypeValue){
                case "INSERT": conceptionEntityValueOperationContent.setOperationType(EntityValueOperationType.INSERT); break;
                case "DELETE": conceptionEntityValueOperationContent.setOperationType(EntityValueOperationType.DELETE); break;
                case "UPDATE": conceptionEntityValueOperationContent.setOperationType(EntityValueOperationType.UPDATE); break;
            }
            if(genericRecordValue.get("addPerDefinedRelation")!=null){
                conceptionEntityValueOperationContent.setAddPerDefinedRelation((Boolean)genericRecordValue.get("addPerDefinedRelation"));
            }
            if(genericRecordValue.get("conceptionEntityUID")!=null) {
                conceptionEntityValueOperationContent.setEntityUID(genericRecordValue.get("conceptionEntityUID").toString());
            }
            if(genericRecordValue.get("entityAttributesValue")!=null) {
                conceptionEntityValueOperationContent.setEntityAttributesValue(genericRecordValue.get("entityAttributesValue").toString());
            }

            EntityValue entityValue = new EntityValue();
            entityValue.setEntityUID(conceptionEntityValueOperationContent.getEntityUID());
            conceptionEntityValueOperationPayload.setEntityValue(entityValue);
            try {
                if(conceptionEntityValueOperationContent.getEntityAttributesValue() != null) {
                    byte[] entityPropertiesValueDecodedBytes = Base64.decodeBase64(conceptionEntityValueOperationContent.getEntityAttributesValue().getBytes());
                    HashMap entityPropertiesMap = mapper.readValue(entityPropertiesValueDecodedBytes, HashMap.class);
                    entityValue.setEntityAttributesValue(entityPropertiesMap);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        handleEntityOperationContents(conceptionEntityValueOperationPayloadList);
    }

    public abstract void handleEntityOperationContents(List<? extends EntityValueOperationPayload> infoObjectValueOperationPayloads);
}
