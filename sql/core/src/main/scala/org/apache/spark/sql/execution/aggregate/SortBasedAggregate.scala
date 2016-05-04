/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.aggregate

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.physical.{UnspecifiedDistribution, ClusteredDistribution, AllTuples, Distribution}
import org.apache.spark.sql.execution.{UnsafeFixedWidthAggregationMap, SparkPlan, UnaryNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.StructType

case class SortBasedAggregate(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    nonCompleteAggregateExpressions: Seq[AggregateExpression2],
    nonCompleteAggregateAttributes: Seq[Attribute],
    completeAggregateExpressions: Seq[AggregateExpression2],
    completeAggregateAttributes: Seq[Attribute],
    initialInputBufferOffset: Int,
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
  extends UnaryNode {

  override def operatorMatch(plan: SparkPlan):Boolean = plan match{
    case agg: SortBasedAggregate =>
      initialInputBufferOffset == agg.initialInputBufferOffset &&
      compareOptionExpressions(requiredChildDistributionExpressions, agg.requiredChildDistributionExpressions) &&
      compareExpressions(groupingExpressions.map(_.transformExpression()),
        agg.groupingExpressions.map(_.transformExpression())) &&
      compareExpressions(nonCompleteAggregateExpressions.map(_.transformExpression()),
        agg.nonCompleteAggregateExpressions.map(_.transformExpression())) &&
      compareExpressions(nonCompleteAggregateAttributes.map(_.transformExpression()),
        agg.nonCompleteAggregateAttributes.map(_.transformExpression())) &&
      compareExpressions(completeAggregateExpressions.map(_.transformExpression()),
        agg.completeAggregateExpressions.map(_.transformExpression())) &&
      compareExpressions(completeAggregateAttributes.map(_.transformExpression()),
        agg.completeAggregateAttributes.map(_.transformExpression())) &&
      compareExpressions(resultExpressions.map(_.transformExpression()),
        agg.resultExpressions.map(_.transformExpression()))
    case _ => false
  }

  def compareOptionExpressions(expr1: Option[Seq[Expression]], expr2: Option[Seq[Expression]]): Boolean = {
    if(expr1.isDefined && expr2.isDefined){
      compareExpressions(expr1.get.map(_.transformExpression()), expr2.get.map(_.transformExpression()))
    }else{
      !expr1.isDefined && !expr2.isDefined
    }
  }

  override private[sql] lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputsUnsafeRows: Boolean = false

  override def canProcessUnsafeRows: Boolean = false

  override def canProcessSafeRows: Boolean = true

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.length == 0 => AllTuples :: Nil
      case Some(exprs) if exprs.length > 0 => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    groupingExpressions.map(SortOrder(_, Ascending)) :: Nil
  }

  override def outputOrdering: Seq[SortOrder] = {
    groupingExpressions.map(SortOrder(_, Ascending))
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitions { iter =>
      // Because the constructor of an aggregation iterator will read at least the first row,
      // we need to get the value of iter.hasNext first.
      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator[InternalRow]()
      } else {
        val outputIter = SortBasedAggregationIterator.createFromInputIterator(
          groupingExpressions,
          nonCompleteAggregateExpressions,
          nonCompleteAggregateAttributes,
          completeAggregateExpressions,
          completeAggregateAttributes,
          initialInputBufferOffset,
          resultExpressions,
          newMutableProjection _,
          newProjection _,
          child.output,
          iter,
          outputsUnsafeRows,
          numInputRows,
          numOutputRows)
        if (!hasInput && groupingExpressions.isEmpty) {
          // There is no input and there is no grouping expressions.
          // We need to output a single row as the output.
          numOutputRows += 1
          Iterator[InternalRow](outputIter.outputForEmptyGroupingKeyWithoutInput())
        } else {
          outputIter
        }
      }
    }.mapPartitions{iter=> mapWithReuse[InternalRow](iter, row => row, true)  //zengdan
    }
  }

  override def simpleString: String = {
    val allAggregateExpressions = nonCompleteAggregateExpressions ++ completeAggregateExpressions

    val keyString = groupingExpressions.mkString("[", ",", "]")
    val functionString = allAggregateExpressions.mkString("[", ",", "]")
    val outputString = output.mkString("[", ",", "]")
    s"SortBasedAggregate(key=$keyString, functions=$functionString, output=$outputString)"
  }
}
