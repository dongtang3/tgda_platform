package toolsTest

import com.github.tgda.supplier.feature.common.GlobalDataAccessor
import com.github.tgda.supplier.fundamental.dataMaintenance.SpatialDataMaintainUtil
import com.github.tgda.supplier.providerApplication.AnalysisProviderApplicationUtil

import java.io.File

object SpatialDataMaintainUtilTestCase01 {

  def main(args:Array[String]):Unit ={
    val spatialDataMaintainUtil = new SpatialDataMaintainUtil
    val shpFile = new File("/home/wangychu/Desktop/SEATTLE_GIS_DATA/Transportation/Seattle_Streets/Seattle_Streets.shp")

    val shpParseResult = spatialDataMaintainUtil.parseSHPData(shpFile,null)

    val sparkApplicationName = AnalysisProviderApplicationUtil.getApplicationProperty("sparkApplicationName")
    val sparkMasterLocation = AnalysisProviderApplicationUtil.getApplicationProperty("sparkMasterLocation")
    val globalDataAccessor = new GlobalDataAccessor(sparkApplicationName,sparkMasterLocation)


    val targetMemoryTable = spatialDataMaintainUtil.duplicateSpatialDataInfoToDataSlice(globalDataAccessor._getDataSliceServiceInvoker(),shpParseResult,"Streets","defaultGroup",true,null)

    val memoryTableMetaInfo = targetMemoryTable.getDataSliceMetaInfo

    println(memoryTableMetaInfo.getDataSliceName)
    println(memoryTableMetaInfo.getSliceGroupName)
    println(memoryTableMetaInfo.getPrimaryDataCount)

    Thread.sleep(3000)
    globalDataAccessor.close()
  }

}
