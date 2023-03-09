package com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.receiver;

import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.ConfigurationErrorException;

public class EntityValueOperationsMessageReceiver extends AvroMessageReceiver{

    public EntityValueOperationsMessageReceiver(EntityValueOperationsMessageHandler messageHandler) throws ConfigurationErrorException {
        super(messageHandler);
        super.setBatchHandleMode(true);
        this.setKeyDeserializer(org.apache.kafka.common.serialization.StringDeserializer.class);
        this.setValueDeserializer(io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
    }

    public EntityValueOperationsMessageReceiver(String consumerGroupId, EntityValueOperationsMessageHandler messageHandler) throws ConfigurationErrorException {
        super(consumerGroupId,messageHandler);
        super.setBatchHandleMode(true);
        this.setKeyDeserializer(org.apache.kafka.common.serialization.StringDeserializer.class);
        this.setValueDeserializer(io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
    }
}
