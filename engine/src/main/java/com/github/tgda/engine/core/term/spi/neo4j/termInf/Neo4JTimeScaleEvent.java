package com.github.tgda.engine.core.term.spi.neo4j.termInf;

import com.github.tgda.engine.core.feature.AttributesMeasurable;
import com.github.tgda.engine.core.feature.ClassificationAttachable;
import com.github.tgda.engine.core.term.TimeScaleEvent;

public interface Neo4JTimeScaleEvent extends TimeScaleEvent, ClassificationAttachable, AttributesMeasurable {
}
