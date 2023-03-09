package com.github.tgda.engine.core.analysis.query;

public class MatchAllConceptionKindLogic extends ConceptionKindMatchLogic{

    public MatchAllConceptionKindLogic(){
        super("*",ConceptionKindExistenceRule.MUST_HAVE);
    }
}
