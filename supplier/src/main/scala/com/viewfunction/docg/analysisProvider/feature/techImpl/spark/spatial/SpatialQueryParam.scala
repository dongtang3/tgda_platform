package com.github.tgda.supplier.feature.techImpl.spark.spatial

import scala.collection.mutable

case class SpatialQueryParam(spatialDataFrameName: String, spatialAttributeName: String, resultAttributes: mutable.Buffer[String]) {}
