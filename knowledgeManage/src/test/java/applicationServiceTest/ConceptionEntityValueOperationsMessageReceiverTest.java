package applicationServiceTest;

import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.ConfigurationErrorException;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.exception.MessageHandleErrorException;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationContent;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.payload.EntityValueOperationPayload;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.receiver.EntityValueOperationsMessageHandler;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.receiver.EntityValueOperationsMessageReceiver;

import java.util.List;

public class EntityValueOperationsMessageReceiverTest {

    public static void main(String[] args) throws MessageHandleErrorException, ConfigurationErrorException{
        doReceive();
    }

    private static void doReceive() throws ConfigurationErrorException, MessageHandleErrorException {

        EntityValueOperationsMessageHandler conceptionEntityValueOperationsMessageHandler = new EntityValueOperationsMessageHandler() {
            long totalHandledNum = 0;
            @Override
            public void handleEntityOperationContents(List<? extends EntityValueOperationPayload> infoObjectValueOperationPayloads) {
                for(EntityValueOperationPayload currentEntityValueOperationPayload:infoObjectValueOperationPayloads){

                    EntityValueOperationContent conceptionEntityValueOperationContent = currentEntityValueOperationPayload.getEntityValueOperationContent();
                    EntityValue entityValue = currentEntityValueOperationPayload.getEntityValue();

                    System.out.println(currentEntityValueOperationPayload.getPayloadOffset());
                    System.out.println(currentEntityValueOperationPayload.getPayloadKey());
                    System.out.println(entityValue.getEntityAttributesValue());
                    System.out.println(entityValue.getEntityUID());

                    System.out.println(conceptionEntityValueOperationContent.getEntityUID());
                    System.out.println(conceptionEntityValueOperationContent.getEntityAttributesValue());
                    System.out.println(conceptionEntityValueOperationContent.getOperationType());
                    System.out.println(conceptionEntityValueOperationContent.getConceptionKindName());
                    System.out.println(conceptionEntityValueOperationContent.getCoreRealmName());
                    System.out.println(conceptionEntityValueOperationContent.getSenderId());
                    System.out.println(conceptionEntityValueOperationContent.getSenderIP());
                    System.out.println(conceptionEntityValueOperationContent.getSendTime());
                    System.out.println(conceptionEntityValueOperationContent.isAddPerDefinedRelation());

                    System.out.println("=----------------------------------=");
                    totalHandledNum++;
                }
                System.out.println("totalHandledNum = "+totalHandledNum);
            }
        };

        EntityValueOperationsMessageReceiver conceptionEntityValueOperationsMessageReceiver = new EntityValueOperationsMessageReceiver(conceptionEntityValueOperationsMessageHandler);
        conceptionEntityValueOperationsMessageReceiver.startMessageReceive(new String[]{"DefaultCoreRealm"});
    }
}
