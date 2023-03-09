package com.github.tgda.supplier.fundamental.coreRealm

import query.analysis.com.github.tgda.engine.core.QueryParameters
import com.github.tgda.coreRealm.realmServiceCore.payload._
import com.github.tgda.coreRealm.realmServiceCore.term._
import factory.util.com.github.tgda.engine.core.RealmTermFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class CoreRealmDataAccessor {

  def getConceptionEntities(conceptionKindName: String, queryParameters: QueryParameters, conceptionEntityHandler: EntityHandler): mutable.Buffer[Any] = {
    val resultBuffer =  mutable.Buffer[Any]()
    val coreRealm:CoreRealm = RealmTermFactory.getDefaultCoreRealm()
    try {
      coreRealm.openGlobalSession()
      val type :ConceptionKind = coreRealm.getConceptionKind(conceptionKindName)
      if(type != null) {
        val resultConceptionEntities = type.getEntities(queryParameters)
        val entityList: Iterable[Entity] = resultConceptionEntities.getConceptionEntities.asScala
        val entitiesRetrieveStatistics:EntitiesRetrieveStatistics = resultConceptionEntities.getOperationStatistics
        entityList.foreach(item => {
          val currentResult = conceptionEntityHandler.handleEntity(item,entitiesRetrieveStatistics)
          resultBuffer+=currentResult
        })
      }
    } finally {
      if (coreRealm != null) {
        coreRealm.closeGlobalSession()
      }
    }
    resultBuffer
  }

  def getEntityRowsWithAttributes(conceptionKindName: String, attributeList: mutable.Buffer[String], queryParameters: QueryParameters): mutable.Buffer[EntityValue] = {
    val coreRealm :CoreRealm = RealmTermFactory.getDefaultCoreRealm()
    try {
      coreRealm.openGlobalSession()
      val type :ConceptionKind = coreRealm.getConceptionKind(conceptionKindName)
      if(type != null) {
        val resultEntitiesWithAttributes : ConceptionEntitiesAttributesRetrieveResult = type.getSingleValueEntityAttributesByAttributeNames(attributeList.asJava,queryParameters)
        if(resultEntitiesWithAttributes != null){
          resultEntitiesWithAttributes.getEntityValues.asScala
        }else{
          val resultBuffer = mutable.Buffer[EntityValue]()
          resultBuffer
        }
      }else{
        val resultBuffer = mutable.Buffer[EntityValue]()
        resultBuffer
      }
    } finally {
      if (coreRealm != null) {
        coreRealm.closeGlobalSession()
      }
    }
  }

  def getEntityByUID(conceptionKindName: String,entityUID:String):Option[Entity] = {
    val coreRealm :CoreRealm = RealmTermFactory.getDefaultCoreRealm()
    val type :ConceptionKind = coreRealm.getConceptionKind(conceptionKindName)
    if(type == null) Option(null)
    else Option(type.getEntityByUID(entityUID))
  }

  def getRelationEntities(relationKindName: String, queryParameters: QueryParameters, relationEntityHandler: RelationshipEntityHandler): mutable.Buffer[Any] = {
    val resultBuffer =  mutable.Buffer[Any]()
    val coreRealm :CoreRealm = RealmTermFactory.getDefaultCoreRealm()
    try {
      coreRealm.openGlobalSession()
      val relationKind : RelationKind = coreRealm.getRelationKind(relationKindName)
      if(relationKind != null) {
        val resultRelationEntities = relationKind.getRelationEntities(queryParameters)
        val relationshipEntityList: Iterable[RelationshipEntity] = resultRelationEntities.getRelationEntities.asScala
        val entitiesRetrieveStatistics:EntitiesRetrieveStatistics = resultRelationEntities.getOperationStatistics
        relationshipEntityList.foreach(item => {
          val currentResult = relationEntityHandler.handleRelationshipEntity(item,entitiesRetrieveStatistics)
          resultBuffer += currentResult
        })
      }
    } finally {
      if (coreRealm != null) {
        coreRealm.closeGlobalSession()
      }
    }
    resultBuffer
  }

  def getRelationshipEntityRowsWithAttributes(relationKindName: String, attributeList : mutable.Buffer[String], queryParameters: QueryParameters): mutable.Buffer[RelationshipEntityValue] = {
    val coreRealm :CoreRealm = RealmTermFactory.getDefaultCoreRealm()
    try {
      coreRealm.openGlobalSession()
      val relationKind : RelationKind = coreRealm.getRelationKind(relationKindName)
      if(relationKind != null) {
        val relationEntitiesAttributesRetrieveResult : RelationEntitiesAttributesRetrieveResult = relationKind.getEntityAttributesByAttributeNames(attributeList.asJava,queryParameters)
        if(relationEntitiesAttributesRetrieveResult != null){
          relationEntitiesAttributesRetrieveResult.getRelationshipEntityValues.asScala
        }else{
          val resultBuffer = mutable.Buffer[RelationshipEntityValue]()
          resultBuffer
        }
      }else{
        val resultBuffer = mutable.Buffer[RelationshipEntityValue]()
        resultBuffer
      }
    } finally {
      if (coreRealm != null) {
        coreRealm.closeGlobalSession()
      }
    }
  }

  def getRelationshipEntityByUID(relationKindName: String,entityUID:String): Option[RelationshipEntity] = {
    // Wrap the Java result in an Option (this will become a Some or a None)
    val coreRealm :CoreRealm = RealmTermFactory.getDefaultCoreRealm()
    val relationKind :RelationKind = coreRealm.getRelationKind(relationKindName)
    if(relationKind == null) Option(null)
    else Option(relationKind.getEntityByUID(entityUID))
  }

}
