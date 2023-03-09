package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import com.google.common.collect.Lists;
import com.github.tgda.engine.core.internal.neo4j.CypherBuilder;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.term.AttributesView;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JAttributesViewImpl;
import com.github.tgda.engine.core.util.Constant;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.types.Node;

import java.util.ArrayList;
import java.util.List;

public class GetListAttributesViewTransformer implements DataTransformer<List<AttributesView>>{

    private GraphOperationExecutor workingGraphOperationExecutor;
    private String currentCoreRealmName;

    public GetListAttributesViewTransformer(String currentCoreRealmName, GraphOperationExecutor workingGraphOperationExecutor){
        this.currentCoreRealmName= currentCoreRealmName;
        this.workingGraphOperationExecutor = workingGraphOperationExecutor;
    }

    @Override
    public List<AttributesView> transformResult(Result result) {
        List<AttributesView> attributesViewList = new ArrayList<>();
        if(result.hasNext()){
            while(result.hasNext()){
                Record nodeRecord = result.next();
                if(nodeRecord != null){
                    Node resultNode = nodeRecord.get(CypherBuilder.operationResultName).asNode();
                    List<String> allLabelNames = Lists.newArrayList(resultNode.labels());
                    boolean isMatchedKind = true;
                    if(allLabelNames.size()>0){
                        isMatchedKind = allLabelNames.contains(Constant.AttributesViewKindClass);
                    }
                    if(isMatchedKind){
                        long nodeUID = resultNode.id();
                        String coreRealmName = this.currentCoreRealmName;
                        String attributesViewKindName = resultNode.get(Constant._NameProperty).asString();
                        String attributesViewKindNameDesc = null;
                        if(resultNode.get(Constant._DescProperty) != null){
                            attributesViewKindNameDesc = resultNode.get(Constant._DescProperty).asString();
                        }
                        String attributesViewKindDataForm = resultNode.get(Constant._viewKindDataForm).asString();

                        AttributesView.AttributesViewKindDataForm currentAttributesViewKindDataForm = AttributesView.AttributesViewKindDataForm.SINGLE_VALUE;
                        switch(attributesViewKindDataForm){
                            case "SINGLE_VALUE":currentAttributesViewKindDataForm = AttributesView.AttributesViewKindDataForm.SINGLE_VALUE;
                                break;
                            case "LIST_VALUE":currentAttributesViewKindDataForm = AttributesView.AttributesViewKindDataForm.LIST_VALUE;
                                break;
                            case "RELATED_VALUE":currentAttributesViewKindDataForm = AttributesView.AttributesViewKindDataForm.RELATED_VALUE;
                                break;
                            case "EXTERNAL_VALUE":currentAttributesViewKindDataForm = AttributesView.AttributesViewKindDataForm.EXTERNAL_VALUE;
                        }

                        String attributesViewKindUID = ""+nodeUID;
                        Neo4JAttributesViewImpl neo4JAttributesViewImpl =
                                new Neo4JAttributesViewImpl(coreRealmName,attributesViewKindName,attributesViewKindNameDesc,currentAttributesViewKindDataForm,attributesViewKindUID);
                        neo4JAttributesViewImpl.setGlobalGraphOperationExecutor(this.workingGraphOperationExecutor);
                        attributesViewList.add(neo4JAttributesViewImpl);
                    }
                }
            }
        }
        return attributesViewList;
    }
}
