package com.github.tgda.engine.core.exception;

public class ResourceCacheRuntimeException extends Exception{

    public void setCauseMessage(String message){
        Throwable throwable=new Throwable("[ "+ message + " ]");
        this.initCause(throwable);
    }
}
