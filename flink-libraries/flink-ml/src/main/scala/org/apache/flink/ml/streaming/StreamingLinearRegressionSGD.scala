package org.apache.flink.ml.streaming

import org.apache.flink.api.common.functions.util.SideInput
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.{BLAS, DenseVector, SparseVector, Vector}
import org.apache.flink.ml.optimization._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.JavaConverters._

class StreamingLinearRegressionSGD (
 private var stepSize: Double,
 private var numEpochs: Int,
 private var regularizationConstant: Double
 ) extends Serializable {

  def this() = this(0.1, 10, 0.0)

  var model: Option[WeightVector] = None

  def withInitWeights(w: Vector, bias: Double): this.type = {
    this.model = Option(new WeightVector(w, bias))
    this
  }

  def fit(input: WindowedStream[LabeledVector, Tuple, TimeWindow], historicalDataHandle: SideInput[LabeledVector]): Unit = {
    input.withSideInput(historicalDataHandle).apply(new RichWindowFunction[LabeledVector, WeightVector, Tuple, TimeWindow] {

      override def apply(key: Tuple, window: TimeWindow, input: Iterable[LabeledVector], out: Collector[WeightVector]): Unit = {

        val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)
        val learningRateMethod = LearningRateMethod.Default

        val historicalData = getRuntimeContext.getSideInput(historicalDataHandle).asScala
        val miniBatch: Iterable[LabeledVector] = historicalData ++ input
        var currModel = model.get

        var i = 0
        while (i < numEpochs) {
          val gradientCount = miniBatch.map(sample => {
            val gradient = lossFunction.gradient(sample, currModel)
            (gradient, 1)
          }).reduce((left, right) => {
            val (leftGradVector, leftCount) = left
            val (rightGradVector, rightCount) = right

            val result = leftGradVector.weights match {
              case d: DenseVector => d
              case s: SparseVector => s.toDenseVector
            }

            // Add the right gradient to the result
            BLAS.axpy(1.0, rightGradVector.weights, result)
            val gradients = WeightVector(result, leftGradVector.intercept + rightGradVector.intercept)

            (gradients , leftCount + rightCount)
          })

          val (WeightVector(weights, intercept), count) = gradientCount

          BLAS.scal(1.0/count, weights)

          val gradient = WeightVector(weights, intercept/count)
          val effectiveLearningRate = learningRateMethod.calculateLearningRate(
            stepSize,
            i,
            regularizationConstant)

          val newWeights = currModel.weights
          // add the gradient of the L2 regularization
          BLAS.axpy(regularizationConstant, newWeights, gradient.weights)

          // update the weights according to the learning rate
          BLAS.axpy(-effectiveLearningRate, gradient.weights, newWeights)

          currModel = WeightVector(
            newWeights,
            currModel.intercept - effectiveLearningRate * gradient.intercept)
          i += 1
        }

        out.collect(currModel)

      }

    }).map(x => {
      println(x)
    })

  }



}
