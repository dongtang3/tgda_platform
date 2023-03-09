package com.github.tgda.compute.applicationCapacity.compute.commandProcessor;

import com.github.tgda.compute.applicationCapacity.compute.dataComputeUnit.util.DataComputeConfigurationHandler;
import com.github.tgda.compute.applicationCapacity.compute.dataComputeUnit.util.UnitOperationResult;
import com.github.tgda.compute.applicationCapacity.compute.dataComputeUnit.util.UnitIgniteOperationUtil;
import com.github.tgda.compute.consoleApplication.feature.BaseCommandProcessor;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;

import java.util.Collection;

public class LsunitCommandProcessor implements BaseCommandProcessor {

    private Ignite nodeIgnite;

    public LsunitCommandProcessor(Ignite nodeIgnite){
        this.nodeIgnite=nodeIgnite;
    }

    @Override
    public void processCommand(String command, String[] commandOptions){
        UnitOperationResult unitOperationResult = UnitIgniteOperationUtil.isGridActive(this.nodeIgnite);
        if(!unitOperationResult.getResult()){
            System.out.println(unitOperationResult.getResultMessage());
            return;
        }

        String unitScopeName= DataComputeConfigurationHandler.getConfigPropertyValue("unitScopeAttributeName");
        String unitScopeValue= DataComputeConfigurationHandler.getConfigPropertyValue("unitScopeAttributeValue");
        ClusterGroup nodes = nodeIgnite.cluster().forAttribute(unitScopeName, unitScopeValue);
        Collection<ClusterNode> appNodes = nodes.nodes();
        StringBuffer lsAppsMessageStringBuffer=new StringBuffer();
        lsAppsMessageStringBuffer.append("\n\r");
        lsAppsMessageStringBuffer.append("================================================================");
        lsAppsMessageStringBuffer.append("\n\r");
        lsAppsMessageStringBuffer.append("Total running unit number:        "+appNodes.size());
        lsAppsMessageStringBuffer.append("\n\r");
        lsAppsMessageStringBuffer.append("Unit scope:       " + DataComputeConfigurationHandler.getConfigPropertyValue("unitScopeAttributeValue"));
        lsAppsMessageStringBuffer.append("\n\r");
        for(ClusterNode clusterNode:appNodes){
            lsAppsMessageStringBuffer.append("-------------------------------------------------------------");
            lsAppsMessageStringBuffer.append("\n\r");
            lsAppsMessageStringBuffer.append("Unit Id:            " + clusterNode.id().toString());
            lsAppsMessageStringBuffer.append("\n\r");
            lsAppsMessageStringBuffer.append("Unit type:          " + clusterNode.attribute(DataComputeConfigurationHandler.getConfigPropertyValue("unitRoleAttributeName")));
            lsAppsMessageStringBuffer.append("\n\r");
            lsAppsMessageStringBuffer.append("Host name:          " + clusterNode.hostNames());
            lsAppsMessageStringBuffer.append("\n\r");
            lsAppsMessageStringBuffer.append("IP address:         " + clusterNode.addresses());
            lsAppsMessageStringBuffer.append("\n\r");
            if(clusterNode.isClient()){
                lsAppsMessageStringBuffer.append("Client unit:        " + "YES");
            }else{
                lsAppsMessageStringBuffer.append("Client unit:        " + "NO");
            }
            lsAppsMessageStringBuffer.append("\n\r");
            if(clusterNode.isLocal()){
                lsAppsMessageStringBuffer.append("Local unit:         " + "YES");
            }else{
                lsAppsMessageStringBuffer.append("Local unit:         " + "NO");
            }
            lsAppsMessageStringBuffer.append("\n\r");
        }
        lsAppsMessageStringBuffer.append("-------------------------------------------------------------");
        lsAppsMessageStringBuffer.append("\n\r");
        lsAppsMessageStringBuffer.append("================================================================");
        System.out.println(lsAppsMessageStringBuffer.toString());
    }
}
