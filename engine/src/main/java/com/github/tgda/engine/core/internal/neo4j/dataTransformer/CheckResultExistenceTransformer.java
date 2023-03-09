package com.github.tgda.engine.core.internal.neo4j.dataTransformer;

import org.neo4j.driver.Result;

public class CheckResultExistenceTransformer implements DataTransformer<Boolean>{
    @Override
    public Boolean transformResult(Result result) {
        if(result.hasNext()){
            return true;
        }else{
            return false;
        }
    }
}
