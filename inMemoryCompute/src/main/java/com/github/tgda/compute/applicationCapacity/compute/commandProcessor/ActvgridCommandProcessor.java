package com.github.tgda.compute.applicationCapacity.compute.commandProcessor;

import com.github.tgda.compute.consoleApplication.feature.BaseCommandProcessor;
import org.apache.ignite.Ignite;

public class ActvgridCommandProcessor implements BaseCommandProcessor {
    private Ignite nodeIgnite;

    public ActvgridCommandProcessor(Ignite nodeIgnite){
        this.nodeIgnite=nodeIgnite;
    }

    @Override
    public void processCommand(String command, String[] commandOptions) {
        StringBuffer appInfoStringBuffer=new StringBuffer();
        appInfoStringBuffer.append("\n\r");
        appInfoStringBuffer.append("================================================================");
        appInfoStringBuffer.append("\n\r");
        appInfoStringBuffer.append("Start active global grid......");
        appInfoStringBuffer.append("\n\r");
        this.nodeIgnite.active(true);
        appInfoStringBuffer.append("Active global grid finish.");
        appInfoStringBuffer.append("\n\r");
        appInfoStringBuffer.append("================================================================");
        System.out.println(appInfoStringBuffer.toString());
    }

}
