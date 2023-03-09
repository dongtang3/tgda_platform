package com.github.tgda.supplier.feature.communicationRouter

import akka.actor.ActorRef
import com.github.tgda.supplier.feature.communication.messagePayload.{AnalyseRequest, AnalyseResponse}
import com.github.tgda.supplier.providerApplication.communication.CommunicationMessageHandler
import com.github.tgda.supplier.feature.common.GlobalDataAccessor
import com.github.tgda.supplier.feature.communication.messagePayload.spatialAnalysis.{AdministrativeDivisionSpatialCalculateRequest, SpatialPropertiesAggregateStatisticRequest}
import com.github.tgda.supplier.feature.functionalFeatures.{AdministrativeDivisionBasedSpatialAnalysis, SpatialPropertiesStatisticAndAnalysis}

class AnalysisProviderCommunicationMessageHandler(globalDataAccessor :GlobalDataAccessor) extends CommunicationMessageHandler{
  override def handleMessage(communicationMessage: Any, communicationActor: ActorRef, senderActor: ActorRef): Unit = {

    var analyseResponse:AnalyseResponse=null

    communicationMessage match {
      case communicationMessage:AnalyseRequest =>
        analyseResponse = new AnalyseResponse(communicationMessage.getRequestUUID)
        analyseResponse.setResponseDataForm(communicationMessage.getResponseDataForm)
    }
    if(analyseResponse!=null){
      communicationMessage match {
        case communicationMessage: String =>
          println(s" $communicationMessage")
        /*
        case communicationMessage: messagePayload.communication.feature.analysisProvider.com.github.tgda.AnalyzeTreesCrownAreaInSection =>
          //senderActor.tell("Reply for AnalyzeTreesCrownAreaInSection Executed "+communicationMessage.getRequestUUID , communicationActor)
          println(communicationMessage.getTreeCrownType+" "+communicationMessage.getRequestUUID+" "+communicationMessage.getRequestDateTime)
          val result = EcologicalEnvironmentAnalysis.executeSparkTreesCrownAreaCal2(globalDataAccessor,"TreeCanopy","CommunityReportingArea")
          val analyseResponse = new AnalyseResponse(communicationMessage.getRequestUUID)
          analyseResponse.generateMetaInfo()
          analyseResponse.setResponseData(result)
          analyseResponse.setResponseDataForm(communicationMessage.getResponseDataForm)
          senderActor.tell(analyseResponse,communicationActor)
        */
        case communicationMessage: SpatialPropertiesAggregateStatisticRequest =>
          SpatialPropertiesStatisticAndAnalysis.executeSpatialPropertiesAggregateStatistic(globalDataAccessor,communicationMessage.asInstanceOf[SpatialPropertiesAggregateStatisticRequest])
        case communicationMessage: AdministrativeDivisionSpatialCalculateRequest =>
          AdministrativeDivisionBasedSpatialAnalysis.doExecuteDataSliceAdministrativeDivisionSpatialCalculation(
              globalDataAccessor,analyseResponse,communicationMessage.asInstanceOf[AdministrativeDivisionSpatialCalculateRequest])
      }
    }

    if(analyseResponse!=null){
      analyseResponse.generateMetaInfo()
      senderActor.tell(analyseResponse,communicationActor)
    }
  }
}
