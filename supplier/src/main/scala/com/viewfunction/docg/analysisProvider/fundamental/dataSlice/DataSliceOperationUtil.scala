package com.github.tgda.supplier.fundamental.dataSlice

import messagePayload.communication.feature.analysisProvider.com.github.tgda.ResponseDataset
import com.github.tgda.supplier.fundamental.dataSlice.ResponseDataSourceTech.ResponseDataSourceTech
import com.github.tgda.supplier.providerApplication.AnalysisProviderApplicationUtil
import result.dataService.dataComputeUnit.dataCompute.applicationCapacity.dataCompute.com.github.tgda.DataSliceOperationResult
import com.github.tgda.compute.applicationCapacity.compute.dataComputeUnit.dataService.{DataServiceInvoker, DataSlice, DataSlicePropertyType}
import util.dataComputeUnit.dataCompute.applicationCapacity.dataCompute.com.github.tgda.MassDataOperationUtil
import org.apache.spark.sql.types.DataTypes

import java.util

object DataSliceOperationUtil {

  val massDataOperationParallelism = AnalysisProviderApplicationUtil.getApplicationProperty("massDataOperationParallelism")

  def createDataSliceAccordingToResponseDataSourceTech(dataServiceInvoker:DataServiceInvoker, dataSliceName:String, dataSliceGroup:String,
                                                       dataSlicePropertiesDefinitions:java.util.Map[String,String], responseDataSourceTech:ResponseDataSourceTech):Unit = {
    val dataSlicePK:java.util.ArrayList[String] = new java.util.ArrayList[String]()
    dataSlicePK.add(DataSliceOperationConstant.TempResponseDataSlicePK)
    val dataSliceProperties:java.util.ArrayList[String] = new java.util.ArrayList[String]()
    dataSliceProperties.add(DataSliceOperationConstant.TempResponseDataSlicePK)

    val dataSlicePropertyMap: util.HashMap[String, DataSlicePropertyType] = new util.HashMap[String, DataSlicePropertyType]()
    dataSlicePropertiesDefinitions.forEach((propertyName,propertyType) =>{
      dataSlicePropertyMap.put(propertyName,
        getDataSlicePropertyType(propertyType,responseDataSourceTech)
      )
      dataSliceProperties.add(propertyName)
    })
    dataSlicePropertyMap.put(DataSliceOperationConstant.TempResponseDataSlicePK,DataSlicePropertyType.STRING)
    val resultDataSlice:DataSlice = dataServiceInvoker.createGridDataSlice(dataSliceName,dataSliceGroup,dataSlicePropertyMap,dataSlicePK)
  }

  def syncDataSliceFromResponseDataset(dataServiceInvoker:DataServiceInvoker, dataSliceName:String, dataSliceGroup:String,
                                       responseDataset:ResponseDataset, responseDataSourceTech:ResponseDataSourceTech):Unit = {
    val dataSlicePropertiesDefinitions:java.util.Map[String,String] = responseDataset.getPropertiesInfo
    val dataSlicePK:java.util.ArrayList[String] = new java.util.ArrayList[String]()
    dataSlicePK.add(DataSliceOperationConstant.TempResponseDataSlicePK)
    val dataSliceProperties:java.util.ArrayList[String] = new java.util.ArrayList[String]()
    dataSliceProperties.add(DataSliceOperationConstant.TempResponseDataSlicePK)

    val dataSlicePropertyMap: util.HashMap[String, DataSlicePropertyType] = new util.HashMap[String, DataSlicePropertyType]()
    dataSlicePropertiesDefinitions.forEach((propertyName,propertyType) =>{
      dataSlicePropertyMap.put(propertyName,
        getDataSlicePropertyType(propertyType,responseDataSourceTech)
      )
      dataSliceProperties.add(propertyName)
    })
    dataSlicePropertyMap.put(DataSliceOperationConstant.TempResponseDataSlicePK,DataSlicePropertyType.STRING)
    val resultDataSlice:DataSlice = dataServiceInvoker.createGridDataSlice(dataSliceName,dataSliceGroup,dataSlicePropertyMap,dataSlicePK)
    val dataList: java.util.ArrayList[java.util.HashMap[String,Object]]  = responseDataset.getDataList
    val dataSliceOperationResult:DataSliceOperationResult = MassDataOperationUtil.massInsertSliceData(dataServiceInvoker,dataSliceName,dataList.asInstanceOf[util.List[util.Map[String,Object]]],
      dataSliceProperties,DataSliceOperationConstant.TempResponseDataSlicePK,massDataOperationParallelism.toInt)
  }

  def getDataSlicePropertyType(propertyType:String,responseDataSourceTech:ResponseDataSourceTech):DataSlicePropertyType = {
    var resultType = DataSlicePropertyType.STRING
    if(responseDataSourceTech.equals(ResponseDataSourceTech.SPARK)){
      if(propertyType.equals(DataTypes.StringType.typeName)){resultType = DataSlicePropertyType.STRING}
      if(propertyType.equals(DataTypes.DateType.typeName)){resultType = DataSlicePropertyType.DATE}
      if(propertyType.equals(DataTypes.ShortType.typeName)){resultType = DataSlicePropertyType.SHORT}
      if(propertyType.equals(DataTypes.ByteType.typeName)){resultType = DataSlicePropertyType.BYTE}
      if(propertyType.equals(DataTypes.LongType.typeName)){resultType = DataSlicePropertyType.LONG}
      if(propertyType.equals(DataTypes.BinaryType.typeName)){resultType = DataSlicePropertyType.BINARY}
      if(propertyType.equals(DataTypes.BooleanType.typeName)){resultType = DataSlicePropertyType.BOOLEAN}
      if(propertyType.equals(DataTypes.CalendarIntervalType.typeName)){resultType = DataSlicePropertyType.LONG}
      if(propertyType.equals(DataTypes.DoubleType.typeName)){resultType = DataSlicePropertyType.DOUBLE}
      if(propertyType.equals(DataTypes.FloatType.typeName)){resultType = DataSlicePropertyType.FLOAT}
      if(propertyType.equals(DataTypes.IntegerType.typeName)){resultType = DataSlicePropertyType.INT}
      if(propertyType.equals(DataTypes.TimestampType.typeName)){resultType = DataSlicePropertyType.TIMESTAMP}
      if(propertyType.equals(DataTypes.NullType.typeName)){}
    }
    resultType
  }
}
