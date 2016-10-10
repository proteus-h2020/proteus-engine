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

//    val batchDataSet = env.readTextFile("hdfs://vm-cluster-node1:8020/user/ventura/proteus/batch.dataset")
//        .map(line => new LabeledVector(1, DenseVector(1, 2, 3)))

    val batchDataSet = env.fromCollection(Seq(new LabeledVector(0, DenseVector(1, 2, 3)), new LabeledVector(1, DenseVector(1, 2, 3))))
    val regressor = new StreamingLinearRegressionSGD().withInitWeights(DenseVector.zeros(3), 0)

    val streamingTrainingSet = env.addSource(new SourceFunction[LabeledVector]() {

      var counter = 0

      override def run(ctx: SourceContext[LabeledVector]): Unit = {
        while (counter < 8000) {
          ctx.collect(LabeledVector(counter % 6, DenseVector(1, 2, 3)))
          counter += 1
        }
      }

      override def cancel(): Unit = {}
    }).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[LabeledVector] {

      var counter = 0L

      override def getCurrentWatermark: Watermark = new Watermark(counter - 1L)

      override def extractTimestamp(element: LabeledVector, previousElementTimestamp: Long): Long = {
        counter += 10L
        counter
      }
    }).keyBy("label").timeWindow(Time.of(5000, TimeUnit.MILLISECONDS))

    regressor.fit(streamingTrainingSet, env.newBroadcastedSideInput(batchDataSet))

    env.execute()

  }


}
