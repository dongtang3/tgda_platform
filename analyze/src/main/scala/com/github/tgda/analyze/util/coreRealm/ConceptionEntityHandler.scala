package com.github.tgda.dataAnalyze.util.coreRealm

import payload.com.github.tgda.engine.core.EntitiesRetrieveStatistics
import term.com.github.tgda.engine.core.Entity

abstract class EntityHandler {
  def handleEntity(conceptionEntity:Entity, entitiesRetrieveStatistics:EntitiesRetrieveStatistics):Any
}
