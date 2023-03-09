package com.github.tgda.dataAnalyze.util.coreRealm

import payload.com.github.tgda.engine.core.EntitiesRetrieveStatistics
import term.com.github.tgda.engine.core.RelationshipEntity

abstract class RelationshipEntityHandler {
  def handleRelationshipEntity(relationshipEntity:RelationshipEntity,entitiesRetrieveStatistics:EntitiesRetrieveStatistics):Any
}
