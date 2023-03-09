@echo off
setlocal EnableDelayedExpansion
set EXTCLASS=extCLASS\*
set LIB=lib\*
"java" -XX:+UseG1GC -XX:+DisableExplicitGC -Xmx10G -Xrs -cp TGDA_dataCompute-0.1.0.jar;%EXTCLASS%;%LIB% "consoleApplication.dataCompute.com.github.tgda.DataComputeApplicationLauncher"