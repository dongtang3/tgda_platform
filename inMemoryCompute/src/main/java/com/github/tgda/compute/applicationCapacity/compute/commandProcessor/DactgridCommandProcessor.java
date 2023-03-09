package com.github.tgda.compute.applicationCapacity.compute.commandProcessor;

import com.github.tgda.compute.consoleApplication.feature.BaseCommandProcessor;
import org.apache.ignite.Ignite;

public class DactgridCommandProcessor implements BaseCommandProcessor {
    private Ignite nodeIgnite;

    public DactgridCommandProcessor(Ignite nodeIgnite){
        this.nodeIgnite=nodeIgnite;
    }

    @Override
    public void processCommand(String command, String[] commandOptions) {
        StringBuffer appInfoStringBuffer=new StringBuffer();
        appInfoStringBuffer.append("\n\r");
        appInfoStringBuffer.append("================================================================");
        appInfoStringBuffer.append("\n\r");
        appInfoStringBuffer.append("Start deactive global grid......");
        appInfoStringBuffer.append("\n\r");
        this.nodeIgnite.active(false);
        appInfoStringBuffer.append("Deactive global grid finish.");
        appInfoStringBuffer.append("\n\r");
        appInfoStringBuffer.append("================================================================");
        System.out.println(appInfoStringBuffer.toString());
    }
}
