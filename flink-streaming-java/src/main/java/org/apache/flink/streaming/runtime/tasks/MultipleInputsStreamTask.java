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
import org.apache.flink.runtime.io.network.partition.consumer.PriorityUnionInputGate;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.util.SideInputInformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.MultipleStreamInputsProcessor;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Internal
public class MultipleInputsStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

	private static final Logger LOG = LoggerFactory.getLogger(MultipleInputsStreamTask.class);

	private MultipleStreamInputsProcessor inputProcessor;

	private OperatorWrapper[] wrappers;

	private volatile boolean running = true;

	private Map<UUID, List> sideInputsCollector;

	@Override
	@SuppressWarnings("checked")
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

			List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);

			int i;
			final Map<InputGate, PriorityUnionInputGate.GatesPriority> prio = Maps.newHashMapWithExpectedSize(inputGates.length);
			Map<InputGate, Integer> inputMapping = Maps.newHashMapWithExpectedSize(inputGates.length);


			for (i = 0; i < inEdges.size(); i++) {
				int inputType = inEdges.get(i).getTypeNumber();
				InputGate reader = env.getInputGate(i);
				inputGates[inputType] = reader;
				inputMapping.put(reader, inputType);
				if (inputType == 0) {
					prio.put(inputGates[inputType], PriorityUnionInputGate.GatesPriority.NORMAL);
					serializers[inputType] = configuration.getTypeSerializerIn1(userClassLoader);
				} else {
					SideInputInformation<?> info = sideInfos.get(inputType);
					serializers[inputType] = info.getSerializer();
					prio.put(reader, PriorityUnionInputGate.GatesPriority.HIGH);
				}

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

			wrappers[0] = new OperatorWrapper<IN>() {

				private final Counter counter = headOperator.getMetricGroup().counter("numRecordsIn");

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
				public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
					headOperator.processLatencyMarker(latencyMarker);
				}

				@Override
				public Counter getNumberRecordsInCounter() {
					return counter;
				}
			};

			sideInputsCollector = Maps.newHashMapWithExpectedSize(numberOfSideInputs);

			for (i = 1; i < wrappers.length; i++) {
				final UUID id = sideInfos.get(i).getId();
				final int typeId = sideInfos.get(i).getTypeId();
				sideInputsCollector.put(id, new ArrayList<>());
				wrappers[i] = new OperatorWrapper() {

					private final Counter counter = headOperator.getMetricGroup().counter("numRecordsIn" + typeId);

					@Override
					public void processElement(StreamRecord record) throws Exception {
						sideInputsCollector.get(id).add(record.getValue());
					}

					@Override
					public void processWatermark(Watermark mark) throws Exception {
					}

					@Override
					public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
					}

					@Override
					public Counter getNumberRecordsInCounter() {
						return counter;
					}
				};
			}

			inputProcessor = new MultipleStreamInputsProcessor(new PriorityUnionInputGate(inputGates, prio),
				serializers,
				realInputMapping,
				this,
				configuration.getCheckpointMode(),
				getEnvironment().getIOManager());

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
		final MultipleStreamInputsProcessor inputProcessor = this.inputProcessor;
		final Object lock = getCheckpointLock();

		while (running && inputProcessor.processInput(wrapper, lock)) {
			// all the work happens in the "processInput" method
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

	@SuppressWarnings("unchecked")
	public <T> List<T> getSideInput(UUID id) {
		if (sideInputsCollector.containsKey(id)) {
			return sideInputsCollector.get(id);
		}
		throw new RuntimeException("side input lookup failed - missing item?");
	}


	public abstract class OperatorWrapper <TYPE> implements Serializable {

		public abstract void processElement(StreamRecord<TYPE> record) throws Exception;
		public abstract void processWatermark(Watermark mark) throws Exception;
		public abstract void processLatencyMarker(LatencyMarker latencyMarker) throws Exception;

		public Counter getNumberRecordsInCounter() {
			return null;
		}

	}



}
