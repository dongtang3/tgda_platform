package com.github.tgda.engine.core.util.cache;

public interface ResourceCacheQueryLogic <K,V> {
    public boolean filterLogic(K _kValue, V _VValue);
}
