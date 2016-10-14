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

package org.apache.flink.ml.streaming

import breeze.linalg.norm
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.functions.util.SideInput
import org.apache.flink.configuration.Configuration
import org.apache.flink.ml.common._
import org.apache.flink.ml.math.{BLAS, Breeze, DenseVector, SparseVector, Vector}
import org.apache.flink.ml.optimization._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.RichAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.{Collector, XORShiftRandom}

import scala.collection.JavaConverters._

class StreamingLinearRegressionSGD (
  private var stepSize: Double,
  private var numEpochs: Int,
  private var regularizationConstant: Double,
  private var tol: Double
  ) extends Serializable {

  def this() = this(0.1, 100, 0.0, 0.001)

  var model: Option[WeightVector] = None

  def withInitWeights(w: Vector, bias: Double): this.type = {
    this.model = Option(new WeightVector(w, bias))
    this
  }

  def fit(input: AllWindowedStream[LabeledVector, TimeWindow],
          historicalDataHandle: SideInput[LabeledVector]): DataStream[WeightVector] = {
    input.withSideInput(historicalDataHandle)
    .apply(new RichAllWindowFunction[LabeledVector, WeightVector, TimeWindow] {

      var currModel: WeightVector = _
      var wcnt = 0

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        if (model.isEmpty) {
          throw new RuntimeException("model weights not initialized")
        } else {
          currModel = model.get
        }
      }

      def createRegParam(regularizationConstant: Double, weights: Vector) = {
        import Breeze._
        val zeros = DenseVector.zeros(weights.size)
        BLAS.axpy(regularizationConstant, weights, zeros)
        // update the weights according to the learning rate
        BLAS.axpy(0, zeros, weights)
        val n = norm(weights.asBreeze, 2.0)
        0.5 * regularizationConstant * n * n
      }

      override def apply(window: TimeWindow,
          input: Iterable[LabeledVector],
          out: Collector[WeightVector]): Unit = {

        val lossFunction = GenericLossFunction(SquaredLoss, LinearPrediction)
        val learningRateMethod = LearningRateMethod.Default

        val historicalData = getRuntimeContext.getSideInput(historicalDataHandle).asScala
//        val miniBatch: Iterable[LabeledVector] = historicalData ++ input
        var converged = false
        var i = 1

        var regParam = createRegParam(regularizationConstant, currModel.weights.copy)

        while (!converged && i <= numEpochs) {
          val r = new XORShiftRandom(91 + i)
          val miniBatch: Iterable[LabeledVector] = historicalData.filter(_ => {
            r.nextDouble <= 0.15
          }) ++ input
          val gradientCount = miniBatch.map(sample => {
            val (loss, gradient) = lossFunction.lossGradient(sample, currModel)
            (gradient, loss, 1)
          }).reduce((left, right) => {
            val (leftGradVector, leftLoss, leftCount) = left
            val (rightGradVector, rightLoss, rightCount) = right

            val result = leftGradVector.weights match {
              case d: DenseVector => d
              case s: SparseVector => s.toDenseVector
            }

            // Add the right gradient to the result
            BLAS.axpy(1.0, rightGradVector.weights, result)
            val gradients = WeightVector(result,
              leftGradVector.intercept + rightGradVector.intercept)

            (gradients, leftLoss + rightLoss, leftCount + rightCount)
          })

          val (WeightVector(weights, intercept), loss, count) = gradientCount

          if (count > 0) {
            BLAS.scal(1.0/count.toDouble, weights)

            val gradient = WeightVector(weights, intercept/count.toDouble)
            val effectiveLearningRate = learningRateMethod.
              calculateLearningRate(stepSize, i, regParam)

            val oldWeights = currModel.weights.copy
            val newWeights = currModel.weights
            // add the gradient of the L2 regularization
            BLAS.axpy(regParam, newWeights, gradient.weights)

            // update the weights according to the learning rate
            BLAS.axpy(-effectiveLearningRate, gradient.weights, newWeights)
            import Breeze._
            val n2 = norm(newWeights.asBreeze, 2.0)

            regParam = 0.5 * regParam * n2 * n2
            currModel = WeightVector(newWeights,
              currModel.intercept -
                effectiveLearningRate * gradient.intercept)


            val diff = norm(oldWeights.asBreeze - newWeights.asBreeze)
            converged = diff < tol * Math.max(norm(newWeights.asBreeze), 1.0)
            println(wcnt, count, i, diff, loss / count + regParam)
          }

          i += 1
        }

        out.collect(currModel)
        wcnt += 1
      }

    })

  }

  def predict(model: DataStream[WeightVector], testingSet: DataStream[LabeledVector]): Unit = {
    val handle = testingSet.executionEnvironment.newBroadcastedSideInput(model)
    testingSet.map(new RichMapFunction[LabeledVector, (Double, Double)] {

      override def map(point: LabeledVector): (Double, Double) = {
        import Breeze._
        val WeightVector(weights, weight0) = getRuntimeContext
          .getSideInput(handle).asScala.last
        val dotProduct = point.vector.asBreeze.dot(weights.asBreeze) + weight0
        (point.label, dotProduct)
      }

    }).withSideInput(handle).map(x => {
      var (expected, real) = x
      real = Math.abs(real)
      println(s"expected value: $expected current value $real")
    })
  }

}
