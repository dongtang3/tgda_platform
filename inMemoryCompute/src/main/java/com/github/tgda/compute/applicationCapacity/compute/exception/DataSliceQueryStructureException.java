package com.github.tgda.compute.applicationCapacity.compute.exception;

public class DataSliceQueryStructureException extends Exception{

    public void setCauseMessage(String message){
        Throwable throwable=new Throwable("[ "+ message + " ]");
        this.initCause(throwable);
    }
}
