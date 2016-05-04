package org.apache.spark.sql.auto.cache

import java.io.{ObjectInput, ObjectOutput, Externalizable}
import java.nio.ByteBuffer

import org.apache.spark.sql.auto.cache.QGUtils.PlanDesc
import org.apache.spark.sql.auto.cache.QGUtils.PlanDesc
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan}
import org.apache.spark.storage.{StorageLevel, BlockId, BlockManagerId}
import org.apache.spark.util.{SerializableBuffer, Utils}

import scala.collection.mutable.{ArrayBuffer, Map, HashMap}

/**
 * Created by zengdan on 15-3-23.
 */

sealed trait QGMasterMessages extends Serializable

/** Contains messages seen only by the Master and its associated entities. */
private object QGMasterMessages {

  // LeaderElectionAgent to Master
  case class MatchSerializedPlan(plan: PlanDesc)
  case class UpdateInfo(cachedNode: Array[Int], reusedNode: Array[Int], statistics: Map[Int, Array[Long]])
  case class CacheFailed(operatorId: Int)
  case class SaveSchema(output: Seq[Attribute], id: Int)
  case class GetSchema(id: Int)

}

