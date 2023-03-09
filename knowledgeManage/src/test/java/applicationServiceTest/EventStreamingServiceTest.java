package applicationServiceTest;

import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.EventStreamingService;
import com.github.tgda.knowledgeManage.applicationService.eventStreaming.kafka.sender.MessageSentEventHandler;

import java.util.*;

public class EventStreamingServiceTest {

    public static void main(String[] args){

        String coreRealmName = "DefaultCoreRealm";
        String conceptionKindName = "ConceptionKindA";
        boolean addPerDefinedRelation = true;
        MessageSentEventHandler messageSentEventHandler = new MessageSentEventHandler() {

            @Override
            public void operateMetaData(long offset, long timestamp, String topic, int partition) {
                System.out.println(offset + timestamp +topic+partition);
            }
        };

        List<EntityValue> entityValueList = new ArrayList<>();

        for(int i=0;i<10;i++){
            EntityValue entityValue =new EntityValue();
            Map<String,Object> entityPropertyMap = new HashMap<>();
            entityPropertyMap.put("baseprop1","hello world");
            entityPropertyMap.put("baseprop2",100);
            entityPropertyMap.put("baseprop3",new Date());
            entityPropertyMap.put("generalprop1","hello world again");
            entityPropertyMap.put("ProjectId","ProjectId-"+i);
            entityValue.setEntityAttributesValue(entityPropertyMap);
            entityValueList.add(entityValue);
        }

        EventStreamingService.newConceptionEntities(coreRealmName,conceptionKindName, entityValueList,addPerDefinedRelation,messageSentEventHandler);

        entityValueList.clear();
        for(int i=0;i<10;i++){
            EntityValue entityValue =new EntityValue();
            entityValue.setEntityUID(""+new Date().getTime());
            Map<String,Object> entityPropertyMap = new HashMap<>();
            entityPropertyMap.put("baseprop3",new Date());
            entityValue.setEntityAttributesValue(entityPropertyMap);
            entityValueList.add(entityValue);
        }
        EventStreamingService.updateConceptionEntities(coreRealmName,conceptionKindName, entityValueList,messageSentEventHandler);

        List<String> conceptionEntityUIDList = new ArrayList<>();
        for(int i=0;i<10;i++){
            conceptionEntityUIDList.add(""+new Date().getTime());
        }
        EventStreamingService.deleteConceptionEntities(coreRealmName,conceptionKindName,conceptionEntityUIDList,messageSentEventHandler);
    }
}
