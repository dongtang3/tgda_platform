package com.github.tgda.supplier.client.exception;

public class AnalyseRequestFormatException extends Exception{

    public void setCauseMessage(String message){
        Throwable throwable=new Throwable("[ "+ message + " ]");
        this.initCause(throwable);
    }
}
