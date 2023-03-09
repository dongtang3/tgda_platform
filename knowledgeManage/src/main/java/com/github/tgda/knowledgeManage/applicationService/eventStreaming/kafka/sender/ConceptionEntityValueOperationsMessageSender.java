package com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.ConfigurationErrorException;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.MessageFormatErrorException;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.MessageHandleErrorException;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.SchemaFormatErrorException;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationContent;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationType;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.utils.AvroUtils;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.utils.PayloadMetaInfo;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Base64;

import java.io.IOException;
import java.util.Map;

public class EntityValueOperationsMessageSender  extends AvroMessageSender{

    private ObjectMapper mapper = new ObjectMapper();

    public EntityValueOperationsMessageSender(MessageSentEventHandler messageSentEventHandler) throws ConfigurationErrorException {
        super(messageSentEventHandler);
    }

    public void sendEntityValueOperationMessage(EntityValueOperationContent conceptionEntityValueOperationContent,
                                                    EntityValue entityValue) throws SchemaFormatErrorException, MessageFormatErrorException, MessageHandleErrorException {
        AvroUtils.initPayloadSchemas();
        Schema payloadSchema= AvroUtils.getSchema(AvroUtils.EntityValueOperationContentSchemaName);
        GenericRecord payloadRecord = new GenericData.Record(payloadSchema);

        if(conceptionEntityValueOperationContent.getSenderIP() != null){
            payloadRecord.put("senderIP",conceptionEntityValueOperationContent.getSenderIP());
        }
        if(conceptionEntityValueOperationContent.getSenderId() != null){
            payloadRecord.put("senderId",conceptionEntityValueOperationContent.getSenderId());
        }
        payloadRecord.put("sendTime",conceptionEntityValueOperationContent.getSendTime());

        payloadRecord.put("coreRealmName",conceptionEntityValueOperationContent.getCoreRealmName());
        payloadRecord.put("conceptionKindName",conceptionEntityValueOperationContent.getConceptionKindName());
        if(conceptionEntityValueOperationContent.getEntityUID() != null){
            payloadRecord.put("conceptionEntityUID",conceptionEntityValueOperationContent.getEntityUID());
        }

        EntityValueOperationType operationType= conceptionEntityValueOperationContent.getOperationType();
        switch(operationType){
            case INSERT:
                payloadRecord.put("operationType","INSERT");
                payloadRecord.put("addPerDefinedRelation",conceptionEntityValueOperationContent.isAddPerDefinedRelation());
                populatePayloadData(payloadRecord, entityValue);
                break;
            case UPDATE:
                payloadRecord.put("operationType","UPDATE");
                populatePayloadData(payloadRecord, entityValue);
                payloadRecord.put("conceptionEntityUID",conceptionEntityValueOperationContent.getEntityUID());
                break;
            case DELETE:
                payloadRecord.put("operationType","DELETE");
                payloadRecord.put("conceptionEntityUID",conceptionEntityValueOperationContent.getEntityUID());
                break;
        }

        PayloadMetaInfo pmi=new PayloadMetaInfo();
        pmi.setPayloadSchema(AvroUtils.EntityValueOperationContentSchemaName);
        pmi.setDestinationTopic(conceptionEntityValueOperationContent.getCoreRealmName());
        pmi.setPayloadKey(conceptionEntityValueOperationContent.getConceptionKindName());
        this.sendAvroMessage(pmi,payloadRecord);
    }

    private void populatePayloadData(GenericRecord payloadRecord, EntityValue entityValue) {
        if (entityValue != null) {
            try {
                Map<String, Object> entityAttributesValueMap = entityValue.getEntityAttributesValue();
                if (entityAttributesValueMap != null) {
                    String baseDatasetJsonInString = this.mapper.writeValueAsString(entityAttributesValueMap);
                    byte[] encodedBytes = Base64.encodeBase64(baseDatasetJsonInString.getBytes());
                    String baseDatasetValueString = new String(encodedBytes);
                    payloadRecord.put("entityAttributesValue", baseDatasetValueString);
                }
            } catch(IOException e){
                e.printStackTrace();
            }
        }
    }
}
