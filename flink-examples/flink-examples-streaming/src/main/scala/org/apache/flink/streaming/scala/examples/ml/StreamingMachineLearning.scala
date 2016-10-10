package org.apache.flink.streaming.scala.examples.ml

import java.util.concurrent.TimeUnit

import org.apache.flink.ml.common.{FlinkMLTools, LabeledVector}
import org.apache.flink.ml.math.DenseVector
import org.apache.flink.ml.streaming.StreamingLinearRegressionSGD
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object StreamingMachineLearning {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    FlinkMLTools.registerFlinkMLTypes(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val batchDataSet = env.readTextFile("hdfs://vm-cluster-node1:8020/user/ventura/proteus/dataset")
      .map(line => {
        val splits = line.split(" ").map(x => x.toDouble)
        val label = splits(1)
        val data = splits.drop(1)
        LabeledVector(label, DenseVector(data))
      })

    val regressor = new StreamingLinearRegressionSGD().withInitWeights(DenseVector.zeros(8), 0)

    val streamingTrainingSet = env.readTextFile("hdfs://vm-cluster-node1:8020/user/ventura/proteus/dataset")
      .map(line => {
        val splits = line.split(" ").map(x => x.toDouble)
        val label = splits(1)
        val data = splits.drop(1)
        LabeledVector(label, DenseVector(data))
      }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[LabeledVector] {

      var counter = 0L

      override def getCurrentWatermark: Watermark = new Watermark(counter - 1L)

      override def extractTimestamp(element: LabeledVector, previousElementTimestamp: Long): Long = {
        counter += 10L
        counter
      }
    }).keyBy("label").timeWindow(Time.of(5000, TimeUnit.MILLISECONDS))

    regressor.fit(streamingTrainingSet, env.newForwardedSideInput(batchDataSet))

    env.execute()

  }


}
