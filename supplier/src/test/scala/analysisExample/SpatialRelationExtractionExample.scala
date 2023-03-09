package analysisExample

import com.github.tgda.supplier.feature.common.GlobalDataAccessor
import com.github.tgda.supplier.feature.techImpl.spark.spatial
import com.github.tgda.supplier.feature.techImpl.spark.spatial.{SpatialQueryMetaFunction, SpatialQueryParam}
import com.github.tgda.supplier.fundamental.spatial.{GeospatialScaleLevel, SpatialPredicateType}
import com.github.tgda.supplier.providerApplication.AnalysisProviderApplicationUtil
import util.neo4j.internal.com.github.tgda.engine.core.BatchDataOperationUtil
import payload.com.github.tgda.engine.core.RelationshipEntityValue
import util.com.github.tgda.engine.core.RealmConstant
import dataService.dataComputeUnit.dataCompute.applicationCapacity.dataCompute.com.github.tgda.DataSlicePropertyType
import result.dataService.dataComputeUnit.dataCompute.applicationCapacity.dataCompute.com.github.tgda.DataSliceOperationResult
import util.dataComputeUnit.dataCompute.applicationCapacity.dataCompute.com.github.tgda.CoreRealmOperationUtil
import org.apache.spark.sql.Row

import java.util
import scala.collection.mutable

object SpatialRelationExtractionExample extends App {

  /*
  loadMainlineConnectionPointDS
  loadMainlineEndPointDS
  loadPermittedUseMainlineDS
  */

  //spatialRelationExtract
  //loadSpatialConnectionRelationDS
  testSpatialSupportDataSliceConvert

  def testSpatialSupportDataSliceConvert():Unit = {
    val sparkApplicationName = AnalysisProviderApplicationUtil.getApplicationProperty("sparkApplicationName")
    val sparkMasterLocation = AnalysisProviderApplicationUtil.getApplicationProperty("sparkMasterLocation")
    val globalDataAccessor = new GlobalDataAccessor(sparkApplicationName,sparkMasterLocation)
    val mainlineConnectionPointSpDF = globalDataAccessor.getDataFrameWithSpatialSupportFromDataSlice("MainlineConnectionPoint",
      CoreRealmOperationUtil.defaultSliceGroup, GeospatialScaleLevel.GlobalLevel,"mainlineConnectionPointSpDF","geo_ConnectionPointLocation")
    mainlineConnectionPointSpDF.printSchema()
    mainlineConnectionPointSpDF.show(10)
    globalDataAccessor.close()
  }

  def spatialRelationExtract():Unit = {
    val sparkApplicationName = AnalysisProviderApplicationUtil.getApplicationProperty("sparkApplicationName")
    val sparkMasterLocation = AnalysisProviderApplicationUtil.getApplicationProperty("sparkMasterLocation")
    val globalDataAccessor = new GlobalDataAccessor(sparkApplicationName,sparkMasterLocation)

    val mainlineConnectionPointSpDF = globalDataAccessor.getDataFrameWithSpatialSupportFromDataSlice("MainlineConnectionPoint",CoreRealmOperationUtil.defaultSliceGroup, RealmConstant._GeospatialGLGeometryContent,"mainlineConnectionPointSpDF","geo_ConnectionPointLocation")
    val mainlineEndPointSpDF = globalDataAccessor.getDataFrameWithSpatialSupportFromDataSlice("MainlineEndPoint",CoreRealmOperationUtil.defaultSliceGroup, RealmConstant._GeospatialGLGeometryContent,"mainlineEndPointSpDF","geo_EndPointLocation")
    val permittedUseMainlineSpDF = globalDataAccessor.getDataFrameWithSpatialSupportFromDataSlice("PermittedUseMainline",CoreRealmOperationUtil.defaultSliceGroup, RealmConstant._GeospatialGLGeometryContent,"permittedUseMainlineSpDF","geo_LineLocation")

    val spatialQueryMetaFunction = new SpatialQueryMetaFunction

    val mainlineEndPoint_spatialQueryParam = spatial.SpatialQueryParam("mainlineEndPointSpDF","geo_EndPointLocation",mutable.Buffer[String]("REALMGLOBALUID","MNLEP_FE_1"))
    val permittedUseMainline_spatialQueryParam = spatial.SpatialQueryParam("permittedUseMainlineSpDF","geo_LineLocation",mutable.Buffer[String]("REALMGLOBALUID","MNL_LIFE_1"))
    val mainlineEndPoint_permittedUseMainlineJoinDF = spatialQueryMetaFunction.spatialJoinQuery(globalDataAccessor,mainlineEndPoint_spatialQueryParam,SpatialPredicateType.Touches,permittedUseMainline_spatialQueryParam,"mainlineEndPoint_permittedUseMainlineJoinDF")

    val mainlineConnectionPoint_spatialQueryParam = spatial.SpatialQueryParam("mainlineConnectionPointSpDF","geo_ConnectionPointLocation",mutable.Buffer[String]("REALMGLOBALUID","MNLCP_DEPT"))
    val mainlineConnectionPoint_permittedUseMainlineJoinDF = spatialQueryMetaFunction.spatialWithinDistanceJoinQuery(globalDataAccessor,mainlineConnectionPoint_spatialQueryParam, permittedUseMainline_spatialQueryParam,spatialQueryMetaFunction.transferMeterValueToDegree(0.01),"distanceValue","mainlineEndPoint_permittedUseMainlineJoinDF")

    val schemas= Seq("uid0","MNLCP_DEPT", "uid1","MNL_LIFE_1","distanceValue")
    val dfRenamed1 = mainlineConnectionPoint_permittedUseMainlineJoinDF.toDF(schemas: _*)

    val resultRow1:Array[Row] = dfRenamed1.collect()
    generateSpatialRelation(resultRow1)
    generateSpatialRelation(mainlineEndPoint_permittedUseMainlineJoinDF.collect())
    globalDataAccessor.close()
  }

  def loadMainlineConnectionPointDS():Unit = {
    val dataSlicePropertyMap: util.HashMap[String, DataSlicePropertyType] = new util.HashMap[String, DataSlicePropertyType]()
    dataSlicePropertyMap.put("MNLCP_DEPT",DataSlicePropertyType.STRING)
    dataSlicePropertyMap.put("MNLCP_FEA_",DataSlicePropertyType.INT)
    dataSlicePropertyMap.put("MNLCP_ROTA",DataSlicePropertyType.INT)
    dataSlicePropertyMap.put("MNLCP_CV_2",DataSlicePropertyType.DOUBLE)

    /*
    val mainlineConnectionPointDataSlice: DataSliceOperationResult =
      CoreRealmOperationUtil.syncConceptionKindToDataSlice("MainlineConnectionPoint", "MainlineConnectionPoint", CoreRealmOperationUtil.defaultSliceGroup,dataSlicePropertyMap,CoreRealmOperationUtil.GeospatialScaleLevel.GlobalLevel)
    println(mainlineConnectionPointDataSlice.getSuccessItemsCount)
    */

  }

  def loadMainlineEndPointDS():Unit = {
    val dataSlicePropertyMap: util.HashMap[String, DataSlicePropertyType] = new util.HashMap[String, DataSlicePropertyType]()
    dataSlicePropertyMap.put("MNLEP_FE_1",DataSlicePropertyType.STRING)
    dataSlicePropertyMap.put("MNLEP_FEA_",DataSlicePropertyType.INT)
    dataSlicePropertyMap.put("MNLEP_X_CO",DataSlicePropertyType.DOUBLE)
    dataSlicePropertyMap.put("MNLEP_Y_CO",DataSlicePropertyType.DOUBLE)
    dataSlicePropertyMap.put("MNLEP_ELEV",DataSlicePropertyType.DOUBLE)

    /*
    val mainlineEndPointDataSlice: DataSliceOperationResult =
      CoreRealmOperationUtil.syncConceptionKindToDataSlice("MainlineEndPoint", "MainlineEndPoint", CoreRealmOperationUtil.defaultSliceGroup,dataSlicePropertyMap,CoreRealmOperationUtil.GeospatialScaleLevel.GlobalLevel)
    (mainlineEndPointDataSlice.getSuccessItemsCount)
    */

  }

  def loadPermittedUseMainlineDS():Unit = {
    val dataSlicePropertyMap: util.HashMap[String, DataSlicePropertyType] = new util.HashMap[String, DataSlicePropertyType]()
    dataSlicePropertyMap.put("MNL_LENGTH",DataSlicePropertyType.DOUBLE)
    dataSlicePropertyMap.put("MNL_FCNLEN",DataSlicePropertyType.DOUBLE)
    dataSlicePropertyMap.put("MNL_DNS_FE",DataSlicePropertyType.INT)
    dataSlicePropertyMap.put("MNL_PIPE_1",DataSlicePropertyType.STRING)
    dataSlicePropertyMap.put("MNL_LIFE_1",DataSlicePropertyType.STRING)

    /*
    val mainlineEndPointDataSlice: DataSliceOperationResult =
      CoreRealmOperationUtil.syncConceptionKindToDataSlice("PermittedUseMainline", "PermittedUseMainline", CoreRealmOperationUtil.defaultSliceGroup,dataSlicePropertyMap,CoreRealmOperationUtil.GeospatialScaleLevel.GlobalLevel)
    println(mainlineEndPointDataSlice.getSuccessItemsCount)
    */

  }

  def generateMainlineConnectionPoint_PermittedUseMainlineSpatialRelation(relationInfoArray:Array[Row]):Unit = {
    val relationshipEntityValueList = new util.ArrayList[RelationshipEntityValue]
    relationInfoArray.foreach(row=>{
      val _MainlineConnectionPointEntityUID = row.getString(0)
      val _PermittedUseMainlineUID = row.getString(2)
      val currentRelationshipEntityValue = new RelationshipEntityValue(null,_MainlineConnectionPointEntityUID,_PermittedUseMainlineUID,null)
      relationshipEntityValueList.add(currentRelationshipEntityValue)
    })
    BatchDataOperationUtil.batchAttachNewRelations(relationshipEntityValueList,RealmConstant.GeospatialScale_SpatialConnectRelationClass,16)
  }

  def generateSpatialRelation(relationInfoArray:Array[Row]):Unit = {
    val relationshipEntityValueList = new util.ArrayList[RelationshipEntityValue]
    relationInfoArray.foreach(row=>{
      val _MainlineConnectionPointEntityUID = row.getString(0)
      val _PermittedUseMainlineUID = row.getString(2)
      val currentRelationshipEntityValue = new RelationshipEntityValue(null,_MainlineConnectionPointEntityUID,_PermittedUseMainlineUID,null)
      relationshipEntityValueList.add(currentRelationshipEntityValue)
    })
    BatchDataOperationUtil.batchAttachNewRelations(relationshipEntityValueList,RealmConstant.GeospatialScale_SpatialConnectRelationClass,16)
  }

  def loadSpatialConnectionRelationDS:Unit = {
    /*
    val _GS_SpatialConnectDataSlice: DataSliceOperationResult =
      CoreRealmOperationUtil.syncRelationKindToDataSlice("TGDA_GS_SpatialConnect","GS_SpatialConnect",CoreRealmOperationUtil.defaultSliceGroup,null)
    println(_GS_SpatialConnectDataSlice.getSuccessItemsCount)
    */
  }

}
