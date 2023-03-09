package globalDataAccessorTest

import com.github.tgda.supplier.feature.common.GlobalDataAccessor
import com.github.tgda.supplier.providerApplication.AnalysisProviderApplicationUtil

object GlobalDataAccessorTestCase01 extends App{

  val sparkApplicationName = AnalysisProviderApplicationUtil.getApplicationProperty("sparkApplicationName")
  val sparkMasterLocation = AnalysisProviderApplicationUtil.getApplicationProperty("sparkMasterLocation")
  val globalDataAccessor = new GlobalDataAccessor(sparkApplicationName,sparkMasterLocation)


  val dataSliceServiceInvoker = globalDataAccessor._getDataSliceServiceInvoker()

  dataSliceServiceInvoker.listDataSlices().forEach(println(_))


  /*
  val wetLandDataSlice = globalDataAccessor.getDataSlice("Streets")
  println(wetLandDataSlice.getDataSliceMetaInfo.getTotalDataCount)

  val targetDF = globalDataAccessor.getDataFrameFromDataSlice("Streets","defaultGroup")
  targetDF.printSchema()
  targetDF.take(50).foreach(println(_))
  targetDF.persist()
  */


  Thread.sleep(5000)
  globalDataAccessor.close()
}
