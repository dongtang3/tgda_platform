package com.github.tgda.engine.core.analysis.query.filteringItem;

public interface FilteringItem {

    public void reverseCondition();
    public String getAttributeName();
    public boolean isReversedCondition();

}
