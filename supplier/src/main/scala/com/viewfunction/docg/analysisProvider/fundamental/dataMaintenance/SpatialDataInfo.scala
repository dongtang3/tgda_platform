package com.github.tgda.supplier.fundamental.dataMaintenance

import dataService.dataComputeUnit.dataCompute.applicationCapacity.dataCompute.com.github.tgda.DataSlicePropertyType

import java.util

case class SpatialDataInfo(spatialDataPropertiesDefinition: util.HashMap[String, DataSlicePropertyType],
                           spatialDataValue: util.ArrayList[util.HashMap[String, Any]])
