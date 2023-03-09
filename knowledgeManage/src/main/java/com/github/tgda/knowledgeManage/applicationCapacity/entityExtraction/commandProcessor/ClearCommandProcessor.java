package com.github.tgda.knowledgeManage.applicationCapacity.entityExtraction.commandProcessor;

import com.github.tgda.knowledgeManage.consoleApplication.feature.BaseCommandProcessor;
import com.github.tgda.knowledgeManage.consoleApplication.util.ApplicationLauncherUtil;

public class ClearCommandProcessor implements BaseCommandProcessor {
    @Override
    public void processCommand(String command, String[] commandOptions) {
        ApplicationLauncherUtil.printApplicationConsoleBanner();
    }
}
