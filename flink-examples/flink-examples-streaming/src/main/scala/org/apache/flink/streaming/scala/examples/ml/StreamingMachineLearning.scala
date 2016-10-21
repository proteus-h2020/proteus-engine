/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.scala.examples.ml

import java.util.concurrent.TimeUnit

import org.apache.flink.ml.common.{FlinkMLTools, LabeledVector}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.streaming.StreamingLinearRegressionSGD
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

class LinearTimestamps extends AssignerWithPeriodicWatermarks[LabeledVector] {
  var counter = 0L

  override def getCurrentWatermark: Watermark = new Watermark(counter - 1L)

  override def extractTimestamp(element: LabeledVector,
                                previousElementTimestamp: Long): Long = {
    counter += 10L
    counter
  }
}

object StreamingMachineLearning {

  def parseLine(line: String): LabeledVector = {
    val splits = line.split(" ").map(x => x.toDouble)
//    val itm = 1
//    val label = splits(itm)
//    val data = splits.patch(itm, Nil, 1)
    val label = splits(5)
    val data = Array(splits(0), splits(1))
    LabeledVector(label, DenseVector(data))
  }


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    FlinkMLTools.registerFlinkMLTypes(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val batchDataSet = env.readTextFile(
      "hdfs://vm-cluster-node1:8020/user/ventura/proteus/batch.dataset")
      .map(line => parseLine(line))

    val regressor = new StreamingLinearRegressionSGD()
      .withInitWeights(DenseVector.zeros(2), 0)
      .withIterationNum(100)

    val streamingTrainingSet = env.readTextFile(
      "hdfs://vm-cluster-node1:8020/user/ventura/proteus/stream.dataset")
      .map(line => {
        parseLine(line)
      }).assignTimestampsAndWatermarks(new LinearTimestamps())
      .timeWindowAll(Time.of(50, TimeUnit.MILLISECONDS))

    val streamingTestingSet = env.readTextFile(
      "hdfs://vm-cluster-node1:8020/user/ventura/proteus/validation.dataset").map(x=>{
      parseLine(x)
    })

    val model = regressor.fit(streamingTrainingSet, env.newBroadcastedSideInput(batchDataSet))
    regressor.predict(model, streamingTestingSet)

    env.execute()

  }


}
