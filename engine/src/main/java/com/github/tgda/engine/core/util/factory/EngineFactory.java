package com.github.tgda.engine.core.util.factory;

import com.github.tgda.engine.core.exception.EngineFunctionNotSupportedException;
import com.github.tgda.engine.core.exception.EngineServiceRuntimeException;
import com.github.tgda.engine.core.term.Engine;
import com.github.tgda.engine.core.term.spi.neo4j.termImpl.Neo4JEngineImpl;
import com.github.tgda.engine.core.term.spi.neo4j.util.Neo4JEngineSystemUtil;
import com.github.tgda.engine.core.util.StorageImplTech;
import com.github.tgda.engine.core.util.config.PropertiesHandler;

import java.util.Set;

public class EngineFactory {

    private static String _CORE_REALM_STORAGE_IMPL_TECH = PropertiesHandler.getPropertyValue(PropertiesHandler.CORE_REALM_STORAGE_IMPL_TECH);
    private static boolean supportMultiNeo4JGraph =
            Boolean.parseBoolean(PropertiesHandler.getPropertyValue(PropertiesHandler.NEO4J_SUPPORT_MULTI_GRAPH));

    public static Engine getCoreRealm(String coreRealmName) throws EngineFunctionNotSupportedException {
        if (StorageImplTech.NEO4J.toString().equals(_CORE_REALM_STORAGE_IMPL_TECH)) {
            if (supportMultiNeo4JGraph) {
                return new Neo4JEngineImpl(coreRealmName);
            } else {
                EngineFunctionNotSupportedException exception = new EngineFunctionNotSupportedException();
                exception.setCauseMessage("Current Neo4J storage implements doesn't support multi Realm");
                throw exception;
            }
        }
            return null;

        }

        public static Engine getDefaultEngine() {
            if (StorageImplTech.NEO4J.toString().equals(_CORE_REALM_STORAGE_IMPL_TECH)) {
                return Neo4JEngineSystemUtil.getDefaultCoreRealm();
            }
            return null;

        }

        public static Set<String> listCoreRealms () throws EngineFunctionNotSupportedException {
            if (StorageImplTech.NEO4J.toString().equals(_CORE_REALM_STORAGE_IMPL_TECH)) {
                return Neo4JEngineSystemUtil.listCoreRealms();
            }
            return null;
        }

        public static Engine createCoreRealm (String coreRealmName) throws
        EngineServiceRuntimeException, EngineFunctionNotSupportedException {
            if (StorageImplTech.NEO4J.toString().equals(_CORE_REALM_STORAGE_IMPL_TECH)) {
                return Neo4JEngineSystemUtil.createCoreRealm(coreRealmName);
            }
            return null;
        }
    }
