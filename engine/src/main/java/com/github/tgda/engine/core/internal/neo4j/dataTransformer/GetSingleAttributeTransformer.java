package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.github.tgda.engine.core.term.Attribute;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JAttributeImpl;
import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.AttributeDataType;
import com.github.tgda.engine.core.util.Constant;

import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.util.List;

public class GetSingleAttributeTransformer implements DataTransformer<Attribute>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String currentCoreRealmName;

    public GetSingleAttributeTransformer(String currentCoreRealmName, GraphOperationExecutor workingGraphOperationExecutor){
        this.currentCoreRealmName= currentCoreRealmName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public Attribute transformResult(Result result) {
        if(result.hasNext()){
            Record nodeRecord = result.next();
            if(nodeRecord != null){
                Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                List<String> allLabelNames = Lists.newArrayList(resultNode.labels());
                boolean isMatchedKind = true;
                if(allLabelNames.size()>0){
                    isMatchedKind = allLabelNames.contains(Constant.AttributeClass);
                }
                if(isMatchedKind){
                    long nodeUID = resultNode.id();
                    String coreRealmName = this.currentCoreRealmName;
                    String attributeKindName = resultNode.get(Constant._NameProperty).asString();
                    String attributeKindNameDesc = null;
                    if(resultNode.get(Constant._DescProperty) != null){
                        attributeKindNameDesc = resultNode.get(Constant._DescProperty).asString();
                    }
                    String attributesViewKindDataForm = resultNode.get(Constant._attributeDataType).asString();
                    AttributeDataType attributeDataType = null;
                    switch(attributesViewKindDataForm){
                        case "BOOLEAN":attributeDataType = AttributeDataType.BOOLEAN;
                            break;
                        case "INT":attributeDataType = AttributeDataType.INT;
                            break;
                        case "SHORT":attributeDataType = AttributeDataType.SHORT;
                            break;
                        case "LONG":attributeDataType = AttributeDataType.LONG;
                            break;
                        case "FLOAT":attributeDataType = AttributeDataType.FLOAT;
                            break;
                        case "DOUBLE":attributeDataType = AttributeDataType.DOUBLE;
                            break;
                        case "TIMESTAMP":attributeDataType = AttributeDataType.TIMESTAMP;
                            break;
                        case "STRING":attributeDataType = AttributeDataType.STRING;
                            break;
                        case "BINARY":attributeDataType = AttributeDataType.BINARY;
                            break;
                        case "BYTE":attributeDataType = AttributeDataType.BYTE;
                            break;
                        case "DECIMAL":attributeDataType = AttributeDataType.DECIMAL;
                            break;
                        case "BOOLEAN_ARRAY":attributeDataType = AttributeDataType.BOOLEAN_ARRAY;
                            break;
                        case "INT_ARRAY":attributeDataType = AttributeDataType.INT_ARRAY;
                            break;
                        case "SHORT_ARRAY":attributeDataType = AttributeDataType.SHORT_ARRAY;
                            break;
                        case "LONG_ARRAY":attributeDataType = AttributeDataType.LONG_ARRAY;
                            break;
                        case "FLOAT_ARRAY":attributeDataType = AttributeDataType.FLOAT_ARRAY;
                            break;
                        case "DOUBLE_ARRAY":attributeDataType = AttributeDataType.DOUBLE_ARRAY;
                            break;
                        case "TIMESTAMP_ARRAY":attributeDataType = AttributeDataType.TIMESTAMP_ARRAY;
                            break;
                        case "STRING_ARRAY":attributeDataType = AttributeDataType.STRING_ARRAY;
                            break;
                        case "BYTE_ARRAY":attributeDataType = AttributeDataType.BYTE_ARRAY;
                            break;
                        case "DECIMAL_ARRAY":attributeDataType = AttributeDataType.DECIMAL_ARRAY;
                            break;
                        case "DATE":attributeDataType = AttributeDataType.DATE;
                            break;
                        case "DATETIME":attributeDataType = AttributeDataType.DATETIME;
                            break;
                        case "TIME":attributeDataType = AttributeDataType.TIME;
                            break;
                        case "DATE_ARRAY":attributeDataType = AttributeDataType.DATE_ARRAY;
                            break;
                        case "DATETIME_ARRAY":attributeDataType = AttributeDataType.DATETIME_ARRAY;
                            break;
                        case "TIME_ARRAY":attributeDataType = AttributeDataType.TIME_ARRAY;
                    }
                    String attributeKindUID = ""+nodeUID;
                    Neo4JAttributeImpl Neo4jAttributeKindImpl =
                            new Neo4JAttributeImpl(coreRealmName,attributeKindName,attributeKindNameDesc,attributeDataType,attributeKindUID);
                    Neo4jAttributeKindImpl.setGlobalGraphOperationExecutor(this.workingGraphOperationExecutor);
                    return Neo4jAttributeKindImpl;
                }
            }
        }
        return null;
    }
}
