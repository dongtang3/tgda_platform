package com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload;

import com.github.tgda.engine.core.payload.EntityValue;

public class EntityValueOperationPayload {
    private EntityValueOperationContent conceptionEntityValueOperationContent;
    private long payloadOffset;
    private Object payloadKey;
    private EntityValue entityValue;

    public EntityValueOperationContent getEntityValueOperationContent() {
        return conceptionEntityValueOperationContent;
    }

    public void setEntityValueOperationContent(EntityValueOperationContent conceptionEntityValueOperationContent) {
        this.conceptionEntityValueOperationContent = conceptionEntityValueOperationContent;
    }

    public long getPayloadOffset() {
        return payloadOffset;
    }

    public void setPayloadOffset(long payloadOffset) {
        this.payloadOffset = payloadOffset;
    }

    public EntityValue getEntityValue() {
        return entityValue;
    }

    public void setEntityValue(EntityValue entityValue) {
        this.entityValue = entityValue;
    }

    public Object getPayloadKey() {
        return payloadKey;
    }

    public void setPayloadKey(Object payloadKey) {
        this.payloadKey = payloadKey;
    }
}
