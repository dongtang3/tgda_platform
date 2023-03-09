package com.github.tgda.example.generator;

import com.github.tgda.engine.core.analysis.query.QueryParameters;
import com.github.tgda.engine.core.analysis.query.filteringItem.NullValueFilteringItem;
import com.github.tgda.engine.core.exception.EngineServiceEntityExploreException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.payload.EntitiesRetrieveResult;
import com.github.tgda.engine.core.payload.EntityValue;
import com.github.tgda.engine.core.payload.RelationshipAttachInfo;
import com.github.tgda.engine.core.term.Entity;
import com.github.tgda.engine.core.term.Type;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.term.Direction;
import com.github.tgda.engine.core.util.factory.EngineFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChemicalProducts_Realm_Generator {

    private static final String CompoundConceptionType = "Compound";
    private static final String CompoundId = "id";
    private static final String CompoundName = "name";
    private static final String CompoundCASNumber = "CASNumber";

    private static final String IngredientConceptionType = "Ingredient";
    private static final String IngredientId = "id";
    private static final String IngredientName = "name";
    private static final String IngredientCategory = "category";

    public static void main(String[] args) throws EngineServiceRuntimeException, EngineServiceEntityExploreException {
        Engine coreRealm = EngineFactory.getDefaultEngine();

        Type _CompoundType = coreRealm.getType(CompoundConceptionType);
        if(_CompoundType != null){
            coreRealm.removeType(CompoundConceptionType,true);
        }
        _CompoundType = coreRealm.getType(CompoundConceptionType);
        if(_CompoundType == null){
            _CompoundType = coreRealm.createType(CompoundConceptionType,"化合物");
        }

        Type _IngredientType = coreRealm.getType(IngredientConceptionType);
        if(_IngredientType != null){
            coreRealm.removeType(IngredientConceptionType,true);
        }
        _IngredientType = coreRealm.getType(IngredientConceptionType);
        if(_IngredientType == null){
            _IngredientType = coreRealm.createType(IngredientConceptionType,"原料");
        }

        List<EntityValue> compoundEntityValueList = new ArrayList<>();
        File file = new File("realmExampleData/ingr_comp/comp_info.tsv");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempStr;
            while ((tempStr = reader.readLine()) != null) {

                String currentLine = !tempStr.startsWith("#")? tempStr : null;
                if(currentLine != null){
                    String[] dataItems =  currentLine.split("\t");
                    String compoundConceptionId = dataItems[0];
                    String compoundConceptionName = dataItems[1];
                    String compoundConceptionCASNumber = dataItems[2];

                    Map<String,Object> newEntityValueMap = new HashMap<>();
                    newEntityValueMap.put(CompoundId,compoundConceptionId);
                    newEntityValueMap.put(CompoundName,compoundConceptionName);
                    newEntityValueMap.put(CompoundCASNumber,compoundConceptionCASNumber);

                    EntityValue entityValue = new EntityValue(newEntityValueMap);
                    compoundEntityValueList.add(entityValue);
                }
            }
            reader.close();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        _CompoundType.newEntities(compoundEntityValueList,false);

        List<EntityValue> ingredientEntityValueList = new ArrayList<>();
        File file2 = new File("realmExampleData/ingr_comp/ingr_info.tsv");
        BufferedReader reader2 = null;
        try {
            reader2 = new BufferedReader(new FileReader(file2));
            String tempStr;
            while ((tempStr = reader2.readLine()) != null) {

                String currentLine = !tempStr.startsWith("#")? tempStr : null;
                if(currentLine != null){
                    String[] dataItems =  currentLine.split("\t");
                    String ingredientConceptionId = dataItems[0];
                    String ingredientConceptionName = dataItems[1];
                    String ingredientConceptionCategory = dataItems[2];

                    Map<String,Object> newEntityValueMap = new HashMap<>();
                    newEntityValueMap.put(IngredientId,ingredientConceptionId);
                    newEntityValueMap.put(IngredientName,ingredientConceptionName);
                    newEntityValueMap.put(IngredientCategory,ingredientConceptionCategory);

                    EntityValue entityValue = new EntityValue(newEntityValueMap);
                    ingredientEntityValueList.add(entityValue);
                }
            }
            reader2.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader2 != null) {
                try {
                    reader2.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        _IngredientType.newEntities(ingredientEntityValueList,false);

        coreRealm.openGlobalSession();

        Type _CompoundType1 = coreRealm.getType(CompoundConceptionType);
        QueryParameters queryParameters2 = new QueryParameters();
        NullValueFilteringItem defaultFilterItem = new NullValueFilteringItem(CompoundId);
        defaultFilterItem.reverseCondition();
        queryParameters2.setDefaultFilteringItem(defaultFilterItem);
        EntitiesRetrieveResult _CompoundResult= _CompoundType1.getEntities(queryParameters2);
        Map<String,String> idUIDMapping_Compound = new HashMap();
        for(Entity currentCompoundEntity : _CompoundResult.getConceptionEntities()){
            String uid = currentCompoundEntity.getEntityUID();
            String idValue = currentCompoundEntity.getAttribute(CompoundId).getAttributeValue().toString();
            idUIDMapping_Compound.put(idValue,uid);
        }

        RelationshipAttachInfo relationshipAttachInfo = new RelationshipAttachInfo();
        relationshipAttachInfo.setRelationKind("belongsToCategory");
        relationshipAttachInfo.setDirection(Direction.FROM);
        List<String> existClassificationList = new ArrayList<>();

        Type _IngredientType1 = coreRealm.getType(IngredientConceptionType);
        QueryParameters queryParameters3 = new QueryParameters();
        NullValueFilteringItem defaultFilterItem2 = new NullValueFilteringItem(IngredientId);
        defaultFilterItem2.reverseCondition();
        queryParameters3.setDefaultFilteringItem(defaultFilterItem2);
        EntitiesRetrieveResult _IngredientResult= _IngredientType1.getEntities(queryParameters3);
        Map<String,String> idUIDMapping_Ingredient = new HashMap();
        for(Entity currentIngredientEntity : _IngredientResult.getConceptionEntities()){
            String uid = currentIngredientEntity.getEntityUID();
            String idValue = currentIngredientEntity.getAttribute(IngredientId).getAttributeValue().toString();
            idUIDMapping_Ingredient.put(idValue,uid);

            String categoryName = currentIngredientEntity.getAttribute(IngredientCategory).getAttributeValue().toString().trim();
            if (!existClassificationList.contains(categoryName)) {
                if (coreRealm.getClassification(categoryName) == null) {
                    coreRealm.createClassification(categoryName, "");
                }
                existClassificationList.add(categoryName);
            }
            currentIngredientEntity.attachClassification(relationshipAttachInfo,categoryName);
        }

        File file3 = new File("realmExampleData/ingr_comp/ingr_comp.tsv");
        BufferedReader reader3 = null;
        try {
            reader3 = new BufferedReader(new FileReader(file3));
            String tempStr;
            while ((tempStr = reader3.readLine()) != null) {

                String currentLine = !tempStr.startsWith("#")? tempStr : null;
                if(currentLine != null){
                    String[] dataItems =  currentLine.split("\t");
                    String ingredientId = dataItems[0].trim();
                    String compoundId = dataItems[1].trim();
                    linkItem(_IngredientType1,idUIDMapping_Ingredient,idUIDMapping_Compound,ingredientId,compoundId);
                }
            }
            reader3.close();
        } catch (IOException | EngineServiceEntityExploreException e) {
            e.printStackTrace();
        } finally {
            if (reader3 != null) {
                try {
                    reader3.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        coreRealm.closeGlobalSession();
    }

    private static void linkItem(Type _IngredientType, Map<String,String> idUIDMapping_Ingredient, Map<String,String> idUIDMapping_Compound,
                                 String ingredientId, String compoundId) throws EngineServiceEntityExploreException, EngineServiceRuntimeException {
        String ingredientEntityUID = idUIDMapping_Ingredient.get(ingredientId);
        Entity _IngredientEntity = _IngredientType.getEntityByUID(ingredientEntityUID);
        String _CompoundEntityUID = idUIDMapping_Compound.get(compoundId);
        if(_CompoundEntityUID != null){
            _IngredientEntity.attachFromRelation(_CompoundEntityUID,"isUsedIn",null,false);
        }
    }
}
