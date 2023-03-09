package com.github.tgda.compute.applicationCapacity.compute.commandProcessor;

import com.github.tgda.compute.consoleApplication.feature.BaseCommandProcessor;
import com.github.tgda.compute.consoleApplication.util.ApplicationLauncherUtil;

public class ClearCommandProcessor implements BaseCommandProcessor {
    @Override
    public void processCommand(String command, String[] commandOptions) {
        ApplicationLauncherUtil.printApplicationConsoleBanner();
    }
}
