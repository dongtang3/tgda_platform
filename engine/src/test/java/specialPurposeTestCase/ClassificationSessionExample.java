package specialPurposeTestCase;

import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.payload.AttributeValue;
import com.github.tgda.engine.core.term.Classification;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEngineImpl;
import com.github.tgda.engine.core.util.factory.EngineFactory;
import org.testng.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClassificationSessionExample {


    public static void main(String[] args) throws EngineServiceRuntimeException {
        manner1();
        //manner2();
    }

    public static void manner1() throws EngineServiceRuntimeException {
        Engine coreRealm = EngineFactory.getDefaultEngine();
        coreRealm.openGlobalSession();

        String classificationName01 = "classification1";
        Classification _Classification01 = coreRealm.getClassification(classificationName01);

        Assert.assertFalse(coreRealm.removeClassification(null));
        if(_Classification01 != null){
            coreRealm.removeClassificationWithOffspring(classificationName01);
        }
        coreRealm.createClassification(classificationName01,classificationName01+"Desc");

        for(int i=0;i<100;i++){
            Classification currentChildClassification = coreRealm.createClassification("childClassificationName"+i,classificationName01+"Desc",classificationName01);
            currentChildClassification.addAttribute("color","color"+i);
        }

        Classification classification = coreRealm.getClassification("classification1");
        List<Classification> childClassificationList = classification.getChildClassifications();

        for (Classification childClassification : childClassificationList) {
          //String color = childClassification.getAttribute("color").getAttributeValue().toString();
            String color = "";
            List<AttributeValue> AttributeValues = childClassification.getAttributes();
            for (AttributeValue attributeValue : AttributeValues) {
                String attributeValueName = attributeValue.getAttributeName();
                if ("color".equals(attributeValueName)) {
                    color = attributeValue.getAttributeValue().toString();
                    System.out.println(color);
                    break;
                }
            }
            String childClassificationName = childClassification.getClassificationName();
        }
        coreRealm.closeGlobalSession();
    }


    public static void manner2() throws EngineServiceRuntimeException {

        Engine coreRealm = EngineFactory.getDefaultEngine();

        GraphOperationExecutor graphOperationExecutor = new GraphOperationExecutor();
        ((Neo4JEngineImpl)coreRealm).setGlobalGraphOperationExecutor(graphOperationExecutor);

        String classificationName01 = "classification1";
        Classification _Classification01 = coreRealm.getClassification(classificationName01);

        Assert.assertFalse(coreRealm.removeClassification(null));
        if(_Classification01 != null){
            coreRealm.removeClassificationWithOffspring(classificationName01);
        }
        coreRealm.createClassification(classificationName01,classificationName01+"Desc");

        for(int i=0;i<100;i++){
            Classification currentChildClassification = coreRealm.createClassification("childClassificationName"+i,classificationName01+"Desc",classificationName01);
            currentChildClassification.addAttribute("color","color"+i);
        }

        Classification classification = coreRealm.getClassification("classification1");
        List<Classification> childClassificationList = classification.getChildClassifications();

        for (Classification childClassification : childClassificationList) {
            //String color = childClassification.getAttribute("color").getAttributeValue().toString();
            String color = "";
            List<AttributeValue> AttributeValues = childClassification.getAttributes();
            for (AttributeValue attributeValue : AttributeValues) {
                String attributeValueName = attributeValue.getAttributeName();
                if ("color".equals(attributeValueName)) {
                    color = attributeValue.getAttributeValue().toString();
                    System.out.println(color);
                    break;
                }
            }
            String childClassificationName = childClassification.getClassificationName();
        }

        graphOperationExecutor.close();
    }

    public Map<String, String> getZoneColor(String zone) {
        Map<String, String> map = new HashMap<>();
        char[] ch = zone.toCharArray();
        ch[0] = (char) (ch[0] - 32);
        String classificationName = new String(ch);

        Engine coreRealm = EngineFactory.getDefaultEngine();

        Classification classification = coreRealm.getClassification(classificationName);
        if (null == classification) {
            return map;
        }
        List<Classification> childClassificationList = classification.getChildClassifications();

        for (Classification childClassification : childClassificationList) {
//            String color = childClassification.getAttribute("color").getAttributeValue().toString();
            String color = "";
            List<AttributeValue> AttributeValues = childClassification.getAttributes();
            for (AttributeValue attributeValue : AttributeValues) {
                String attributeValueName = attributeValue.getAttributeName();
                if ("color".equals(attributeValueName)) {
                    color = attributeValue.getAttributeValue().toString();
                    break;
                }
            }
            String childClassificationName = childClassification.getClassificationName();
            map.put(childClassificationName, color);
        }
        return map;
    }

}
