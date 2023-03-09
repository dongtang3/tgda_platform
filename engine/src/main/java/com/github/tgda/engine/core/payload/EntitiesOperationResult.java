package com.github.tgda.engine.core.payload;

import java.util.List;

public interface EntitiesOperationResult {
    public List<String> getSuccessEntityUIDs();
    public EntitiesOperationStatistics getOperationStatistics();
}
