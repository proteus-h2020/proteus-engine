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

package org.apache.flink.streaming.runtime.tasks;

import com.google.common.collect.Maps;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.util.SideInputInformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.StreamSideInputsProcessor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Internal
public class MultipleInputsStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

	private StreamSideInputsProcessor inputProcessor;

	private OperatorWrapper[] wrappers;

	private volatile boolean running = true;

	@Override
	public void init() throws Exception {
		StreamConfig configuration = getConfiguration();
		ClassLoader userClassLoader = getUserCodeClassLoader();

		int numberOfInputs = configuration.getNumberOfInputs();
		int numberOfSideInputs = configuration.getNumberOfSideInputs();

		if (numberOfInputs > 0) {
			Environment env = getEnvironment();
			InputGate[] inputGates = new InputGate[env.getInputGatesCount()];

			Map<Integer, SideInputInformation<?>> sideInfos = configuration.getSideInputsTypeSerializers(userClassLoader);
			TypeSerializer<?>[] serializers = new TypeSerializer<?>[1 + sideInfos.size()];
			wrappers = new OperatorWrapper[serializers.length];

			serializers[0] = configuration.getTypeSerializerIn1(userClassLoader);

			List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);

			int i;
			final Map<InputGate, GatesPriority> prio = Maps.newHashMapWithExpectedSize(inputGates.length);
			Map<InputGate, Integer> inputMapping = Maps.newHashMapWithExpectedSize(inputGates.length);

			for (i = 0; i < inputGates.length - numberOfSideInputs; i++) {
				inputGates[i] = env.getInputGate(i);
				inputMapping.put(inputGates[i], 0);
				prio.put(inputGates[i], GatesPriority.NORMAL);
			}

			for (; i < numberOfInputs; i++) {
				int inputType = inEdges.get(i).getTypeNumber();
				InputGate reader = env.getInputGate(i);
				SideInputInformation<?> info = sideInfos.get(inputType);
				inputGates[i] = reader;
				serializers[i] = info.getSerializer();
				inputMapping.put(inputGates[i], inputType);
				prio.put(inputGates[i], GatesPriority.HIGH);
			}

			wrappers[0] = new OperatorWrapper<IN>() {
				@Override
				public void processElement(StreamRecord<IN> record) throws Exception {
					headOperator.setKeyContextElement1(record);
					headOperator.processElement(record);
				}

				@Override
				public void processWatermark(Watermark mark) throws Exception {
					headOperator.processWatermark(mark);
				}

				@Override
				public Counter getNumberRecordsInCounter() {
					return headOperator.getMetricGroup().counter("numRecordsIn");
				}
			};

			for (i = 1; i < wrappers.length; i++) {
				wrappers[i] = new OperatorWrapper<IN>() {
					@Override
					public void processElement(StreamRecord<IN> record) throws Exception {
						System.out.println(record);
					}

					@Override
					public void processWatermark(Watermark mark) throws Exception {
					}
				};
			}

			Arrays.sort(inputGates, new Comparator<InputGate>() {
				@Override
				public int compare(InputGate o1, InputGate o2) {
					return prio.get(o1).compareTo(prio.get(o2));
				}
			});

			int[] realInputMapping = new int[inputGates.length];

			for (i = 0; i < inputGates.length; i++) {
				realInputMapping[i] = inputMapping.get(inputGates[i]);
			}

			inputProcessor = new StreamSideInputsProcessor(inputGates,
				serializers,
				realInputMapping,
				this,
				configuration.getCheckpointMode(),
				getEnvironment().getIOManager(),
				isSerializingTimestamps());

			// make sure that stream tasks report their I/O statistics
			AccumulatorRegistry registry = getEnvironment().getAccumulatorRegistry();
			AccumulatorRegistry.Reporter reporter = registry.getReadWriteReporter();
			inputProcessor.setReporter(reporter);
			inputProcessor.setMetricGroup(getEnvironment().getMetricGroup().getIOMetricGroup());
		}
	}

	@Override
	protected void run() throws Exception {
		// cache some references on the stack, to make the code more JIT friendly
		final OperatorWrapper[] wrapper = this.wrappers;
		final StreamSideInputsProcessor inputProcessor = this.inputProcessor;
		final Object lock = getCheckpointLock();

		while (running && inputProcessor.processInput(wrapper, lock)) {
			checkTimerException();
		}
	}

	@Override
	protected void cleanup() throws Exception {
		inputProcessor.cleanup();
	}

	@Override
	protected void cancelTask() {
		running = false;
	}



	public abstract class OperatorWrapper <TYPE> implements Serializable {

		public abstract void processElement(StreamRecord<TYPE> record) throws Exception;
		public abstract void processWatermark(Watermark mark) throws Exception;

		public Counter getNumberRecordsInCounter() {
			return null;
		}

	}


	public enum GatesPriority {
		HIGH,
		NORMAL,
		LOW
	}
}
