package com.github.tgda.engine.core.term.spi.neo4j.termInf;

import com.github.tgda.engine.core.feature.spi.neo4j.featureInf.Neo4JClassificationAttachable;
import com.github.tgda.engine.core.feature.spi.neo4j.featureInf.Neo4JMetaAttributeFeatureSupportable;
import com.github.tgda.engine.core.feature.spi.neo4j.featureInf.Neo4JMetaConfigItemFeatureSupportable;
import com.github.tgda.engine.core.feature.spi.neo4j.featureInf.Neo4JStatisticalAndEvaluable;
import com.github.tgda.engine.core.term.RelationshipType;

public interface Neo4JRelationshipType extends RelationshipType, Neo4JMetaConfigItemFeatureSupportable, Neo4JMetaAttributeFeatureSupportable, Neo4JClassificationAttachable, Neo4JStatisticalAndEvaluable {
}
