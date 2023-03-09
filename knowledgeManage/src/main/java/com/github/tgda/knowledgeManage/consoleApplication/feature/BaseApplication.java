package com.github.tgda.knowledgeManage.consoleApplication.feature;

public interface BaseApplication {

    public boolean isDaemonApplication();

    public boolean initApplication();

    public boolean shutdownApplication();

    public void executeConsoleCommand(String consoleCommand);

    public void executeDaemonLogic();
}
