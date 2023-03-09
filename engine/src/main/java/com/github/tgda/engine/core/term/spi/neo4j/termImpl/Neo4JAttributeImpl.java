package com.github.tgda.engine.core.term.spi.neo4j.termImpl;

import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetListAttributesViewTransformer;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.GetSingleAttributeValueTransformer;
import com.github.tgda.engine.core.internal.neo4j.util.CommonOperationUtil;
import com.github.tgda.engine.core.internal.neo4j.util.GraphOperationExecutorHelper;
import com.github.tgda.engine.core.payload.AttributeValue;
import com.github.tgda.engine.core.term.AttributeDataType;
import com.github.tgda.engine.core.term.AttributesView;
import com.github.tgda.engine.core.term.Direction;
import com.github.tgda.engine.core.term.spi.neo4j.termInf.Neo4JAttribute;

import com.github.tgda.engine.core.util.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Neo4JAttributeImpl implements Neo4JAttribute {

    private static Logger logger = LoggerFactory.getLogger(Neo4JAttributeImpl.class);
    private String coreRealmName;
    private String attributeKindName;
    private String attributeKindDesc;
    private String attributeKindUID;
    private AttributeDataType attributeDataType;

    public Neo4JAttributeImpl(String coreRealmName, String attributeKindName, String attributeKindDesc, AttributeDataType attributeDataType, String attributeKindUID){
        this.coreRealmName = coreRealmName;
        this.attributeKindName = attributeKindName;
        this.attributeKindDesc = attributeKindDesc;
        this.attributeDataType = attributeDataType;
        this.attributeKindUID = attributeKindUID;
        this.graphOperationExecutorHelper = new GraphOperationExecutorHelper();
    }

    @Override
    public String getAttributeKindName() {
        return attributeKindName;
    }

    @Override
    public String getAttributeKindUID() {
        return attributeKindUID;
    }

    @Override
    public String getAttributeKindDesc() {
        return attributeKindDesc;
    }

    @Override
    public boolean updateAttributeKindDesc(String kindDesc) {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try {
            Map<String,Object> attributeDataMap = new HashMap<>();
            attributeDataMap.put(Constant._DescProperty, kindDesc);
            String updateCql = CypherBuilder.setNodePropertiesWithSingleValueEqual(CypherBuilder.CypherFunctionType.ID,Long.parseLong(this.attributeKindUID),attributeDataMap);
            GetSingleAttributeValueTransformer getSingleAttributeValueTransformer = new GetSingleAttributeValueTransformer(Constant._DescProperty);
            Object updateResultRes = workingGraphOperationExecutor.executeWrite(getSingleAttributeValueTransformer,updateCql);
            CommonOperationUtil.updateEntityMetaAttributes(workingGraphOperationExecutor,this.attributeKindUID,false);
            AttributeValue resultAttributeValue =  updateResultRes != null ? (AttributeValue) updateResultRes : null;
            if(resultAttributeValue != null && resultAttributeValue.getAttributeValue().toString().equals(kindDesc)){
                this.attributeKindDesc = kindDesc;
                return true;
            }else{
                return false;
            }
        } finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    @Override
    public AttributeDataType getAttributeDataType() {
        return attributeDataType;
    }

    @Override
    public List<AttributesView> getContainerAttributesViewKinds() {
        GraphOperationExecutor workingGraphOperationExecutor = this.graphOperationExecutorHelper.getWorkingGraphOperationExecutor();
        try{
            String queryCql = CypherBuilder.matchRelatedNodesFromSpecialStartNodes(
                    CypherBuilder.CypherFunctionType.ID, Long.parseLong(attributeKindUID),
                    Constant.AttributesViewKindClass, Constant.AttributesViewKind_AttributeKindRelationClass, Direction.FROM, null);
            GetListAttributesViewTransformer getListAttributesViewTransformer =
                    new GetListAttributesViewTransformer(Constant.AttributesViewKind_AttributeKindRelationClass,this.graphOperationExecutorHelper.getGlobalGraphOperationExecutor());
            Object attributesViewKindsRes = workingGraphOperationExecutor.executeWrite(getListAttributesViewTransformer,queryCql);
            return attributesViewKindsRes != null ? (List<AttributesView>) attributesViewKindsRes : null;
        }finally {
            this.graphOperationExecutorHelper.closeWorkingGraphOperationExecutor();
        }
    }

    //internal graphOperationExecutor management logic
    private GraphOperationExecutorHelper graphOperationExecutorHelper;

    public void setGlobalGraphOperationExecutor(GraphOperationExecutor graphOperationExecutor) {
        this.graphOperationExecutorHelper.setGlobalGraphOperationExecutor(graphOperationExecutor);
    }

    @Override
    public String getEntityUID() {
        return this.attributeKindUID;
    }

    @Override
    public GraphOperationExecutorHelper getGraphOperationExecutorHelper() {
        return this.graphOperationExecutorHelper;
    }
}
