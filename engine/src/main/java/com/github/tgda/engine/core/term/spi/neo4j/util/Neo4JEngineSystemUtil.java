package com.github.tgda.engine.core.term.spi.neo4j.util;

import com.github.tgda.engine.core.exception.EngineFunctionNotSupportedException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.internal.neo4j.GraphOperationExecutor;
import com.github.tgda.engine.core.internal.neo4j.dataTransformer.DataTransformer;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEngineImpl;
import com.github.tgda.engine.core.util.config.PropertiesHandler;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;

import java.util.HashSet;
import java.util.Set;

public class Neo4JEngineSystemUtil {

    private static boolean supportMultiNeo4JGraph =
            Boolean.parseBoolean(PropertiesHandler.getPropertyValue(PropertiesHandler.NEO4J_SUPPORT_MULTI_GRAPH));
    private static String defaultCoreRealmName = PropertiesHandler.getPropertyValue(PropertiesHandler.DEFAULT_REALM_NAME);

    public static Engine getDefaultCoreRealm(){
        return new Neo4JEngineImpl();
    }

    public static Engine createCoreRealm(String coreRealmName) throws EngineServiceRuntimeException, EngineFunctionNotSupportedException {
        if(supportMultiNeo4JGraph){
            Set<String> existCoreRealms = listCoreRealms();
            // in Neo4j all database name is in lowercase
            if(existCoreRealms.contains(coreRealmName.toLowerCase())){
                EngineServiceRuntimeException engineServiceRuntimeException = new EngineServiceRuntimeException();
                engineServiceRuntimeException.setCauseMessage("Core Realm with name "+coreRealmName+" already exist.");
                throw engineServiceRuntimeException;
            }else{
                //only Enterprise edition neo4j support create database XXX command
                String queryCQL = "create database "+coreRealmName;
                GraphOperationExecutor _GraphOperationExecutor = new GraphOperationExecutor();

                DataTransformer dataTransformer = new DataTransformer() {
                    @Override
                    public Object transformResult(Result result) {
                        if(result.hasNext()){
                            Record currentRecord = result.next();
                        }
                        return null;
                    }
                };
                _GraphOperationExecutor.executeWrite(dataTransformer,queryCQL);
                _GraphOperationExecutor.close();
            }
            return new Neo4JEngineImpl(coreRealmName);
        }else{
            EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
            exception.setCauseMessage("Current Neo4J storage implements doesn't support multi Realm");
            throw exception;
        }
    }

    public static Set<String> listCoreRealms() throws EngineFunctionNotSupportedException {
        if(supportMultiNeo4JGraph){
            Set<String> coreRealmsSet = new HashSet<>();
            String queryCQL = "show databases";
            GraphOperationExecutor _GraphOperationExecutor = new GraphOperationExecutor();
            DataTransformer dataTransformer = new DataTransformer() {
                @Override
                public Object transformResult(Result result) {
                    while(result.hasNext()){
                        Record currentRecord = result.next();
                        String currentCoreRealm = currentRecord.get("name").asString();
                        boolean isDefaultCoreRealm = currentRecord.get("default").asBoolean();
                        if(isDefaultCoreRealm){
                            coreRealmsSet.add(defaultCoreRealmName);
                        }else{
                            if(!currentCoreRealm.equals("system")){
                                coreRealmsSet.add(currentCoreRealm);
                            }
                        }
                    }
                    return null;
                }
            };
            _GraphOperationExecutor.executeRead(dataTransformer,queryCQL);
            _GraphOperationExecutor.close();
            return coreRealmsSet;
        }else{
            EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
            exception.setCauseMessage("Current Neo4J storage implements doesn't support multi Realm");
            throw exception;
        }
    }
}
