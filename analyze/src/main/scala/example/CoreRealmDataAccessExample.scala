package example

import query.analysis.com.github.tgda.engine.core.QueryParameters
import com.github.tgda.coreRealm.realmServiceCore.payload.{EntityValue, EntitiesRetrieveStatistics, RelationshipEntityValue}
import com.github.tgda.coreRealm.realmServiceCore.term.{Entity, RelationshipEntity}
import com.github.tgda.dataAnalyze.util.coreRealm.{EntityHandler, CoreRealmDataAccessor, RelationshipEntityHandler}

import scala.collection.mutable

object CoreRealmDataAccessExample {

  def main(args: Array[String]):Unit = {
    val coreRealmDataAccessor = new CoreRealmDataAccessor

    val resultRelationshipEntity: Option[RelationshipEntity] = coreRealmDataAccessor.getRelationshipEntityByUID("belongsToCategory", "28985571")
    println(resultRelationshipEntity.get.getRelationKindName+" - "+resultRelationshipEntity.get.getRelationshipEntityUID)

    val resultEntity: Option[Entity] = coreRealmDataAccessor.getEntityByUID("Ingredient", "14617129")
    println(resultEntity.get.getAllConceptionKindNames+" - "+resultEntity.get.getConceptionKindName+" - "+resultEntity.get.getEntityUID)

    case class EntityInfo(entityUID: String, entityKind: String)
    val queryParameters = new QueryParameters
    queryParameters.setResultNumber(5)

    val conceptionEntityHandler = new EntityHandler(){
      override def handleEntity(conceptionEntity: Entity, entitiesRetrieveStatistics: EntitiesRetrieveStatistics): Any = {
        EntityInfo(conceptionEntity.getEntityUID,conceptionEntity.getConceptionKindName)
      }
    }
    val conceptionEntityInfoBuffer: mutable.Buffer[EntityInfo] = coreRealmDataAccessor.
      getConceptionEntities("Ingredient",queryParameters,conceptionEntityHandler).asInstanceOf[mutable.Buffer[EntityInfo]]
    conceptionEntityInfoBuffer.foreach(item =>{
      println(item.entityKind + " - " + item.entityUID)
    })

    val relationEntityHandler = new RelationshipEntityHandler {
      override def handleRelationshipEntity(relationshipEntity: RelationshipEntity, entitiesRetrieveStatistics: EntitiesRetrieveStatistics): Any = {
        EntityInfo(relationshipEntity.getRelationshipEntityUID,relationshipEntity.getRelationKindName)
      }
    }
    val relationEntityInfoBuffer : mutable.Buffer[EntityInfo] = coreRealmDataAccessor.
      getRelationEntities("belongsToCategory",queryParameters,relationEntityHandler).asInstanceOf[mutable.Buffer[EntityInfo]]
    relationEntityInfoBuffer.foreach(item=>{
        println(item.entityKind + " - " + item.entityUID)
      })

    val attributesNameList = mutable.Buffer[String]("category","name")
    val conceptionEntityValueBuffer:mutable.Buffer[EntityValue] = coreRealmDataAccessor.getEntityRowsWithAttributes("Ingredient",attributesNameList,queryParameters)
    conceptionEntityValueBuffer.foreach(item => {
      println(item.getEntityAttributesValue)
      println(item.getEntityUID)
    })

    val attributesNameList2 = mutable.Buffer[String]("createDate","dataOrigin")
    val relationEntityValueBuffer:mutable.Buffer[RelationshipEntityValue] = coreRealmDataAccessor.getRelationshipEntityRowsWithAttributes("isUsedIn",attributesNameList2,queryParameters)
    relationEntityValueBuffer.foreach(item => {
      println(item.getEntityAttributesValue)
      println(item.getRelationshipEntityUID)
    })

  }

}
