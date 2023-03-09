package com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.receiver;

import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.ConfigurationErrorException;

public class AvroMessageReceiver extends UniversalMessageReceiver{

    public AvroMessageReceiver(AvroMessageHandler messageHandler) throws ConfigurationErrorException {
        super(messageHandler);
        this.setKeyDeserializer(org.apache.kafka.common.serialization.StringDeserializer.class);
        this.setValueDeserializer(io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
    }

    public AvroMessageReceiver(String consumerGroupId,AvroMessageHandler messageHandler) throws ConfigurationErrorException {
        super(consumerGroupId,messageHandler);
        this.setKeyDeserializer(org.apache.kafka.common.serialization.StringDeserializer.class);
        this.setValueDeserializer(io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
    }
}