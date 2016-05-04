package org.apache.spark.util

import scala.collection.mutable.Map

/**
 * Created by zengdan on 15-10-15.
 */
object Stats {
  val statistics = new ThreadLocal[Map[Int, Array[Int]]]()
  val initialTimes = new ThreadLocal[Map[Int, Int]]()
}
