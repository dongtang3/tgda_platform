package com.github.tgda.knowledgeManage.applicationCapacity.relationExtraction.ruleEngineFactsGenerators;

import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.knowledgeManage.applicationService.ruleEngine.RuleFactsGenerator;
import org.kie.api.runtime.KieSession;

import java.util.Map;

public class RelationExtractionRulesGenerator implements RuleFactsGenerator {

    public static class Message {
        public static final int HELLO   = 0;
        public static final int GOODBYE = 1;
        public String          message;
        public int             status;
        public String getMessage() {
            return message;
        }
        public void setMessage(String message) {
            this.message = message;
        }
        public int getStatus() {
            return status;
        }
        public void setStatus(int status) {
            this.status = status;
        }
    }

    @Override
    public void generateRuleFacts(KieSession kSession, Engine coreRealm, Map<Object, Object> commandContextDataMap, String extractionId, String linkerId) {
        final Message message = new Message();
        message.setMessage( "Hello World" );
        message.setStatus( Message.HELLO );
        kSession.insert( message );
    }
}
