@echo off
setlocal EnableDelayedExpansion
set EXTCLASS=extCLASS\*
set LIB=lib\*
"java" -XX:+UseG1GC -XX:+DisableExplicitGC -Xmx10G -Xrs -cp TGDA_realmKnowledgeManage-0.5.jar;%EXTCLASS%;%LIB% "consoleApplication.knowledgeManage.com.github.tgda.KnowledgeManagementApplicationLauncher"