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
import eu.proteus.flink.annotation.Proteus;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.transformations.utils.SideInputInformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.MultipleStreamInputProcessor;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Internal
@Proteus
public class MultipleInputStreamTask<IN, OUT> extends StreamTask<OUT, OneInputStreamOperator<IN, OUT>> {

	private static final Logger LOG = LoggerFactory.getLogger(MultipleInputStreamTask.class);

	private MultipleStreamInputProcessor<IN> inputProcessor;

	private OperatorWrapper[] wrappers;

	private volatile boolean running = true;

	@Override
	public void init() throws Exception {
		StreamConfig configuration = getConfiguration();
		ClassLoader userClassLoader = getUserCodeClassLoader();

		TypeSerializer<IN> inSerializer = configuration.getTypeSerializerIn1(userClassLoader);
		int numberOfInputs = configuration.getNumberOfInputs();
		int numberOfSideInputs = configuration.getNumberOfSideInputs();


		if (numberOfInputs > 0) {
			Environment env = getEnvironment();
			InputGate[] inputGates = new InputGate[env.getInputGatesCount()];

			Map<Integer, SideInputInformation<?>> sideInfos = configuration.getSideInputsTypeSerializers(userClassLoader);
			TypeSerializer<?>[] serializers = new TypeSerializer<?>[1 + sideInfos.size()];
			wrappers = new OperatorWrapper[serializers.length];


			List<StreamEdge> inEdges = configuration.getInPhysicalEdges(userClassLoader);

			final Map<InputGate, Integer> inputMapping = Maps.newHashMapWithExpectedSize(inputGates.length);
			final Map<InputGate, Boolean> sideMap = Maps.newHashMapWithExpectedSize(inputGates.length);

			for (int i = 0; i < inEdges.size(); i++) {
				int inputType = inEdges.get(i).getTypeNumber();
				InputGate reader = env.getInputGate(i);
				inputGates[inputType] = reader;
				inputMapping.put(reader, inputType);
				if (inputType == 0) {
					serializers[inputType] = inSerializer;
					sideMap.put(reader, false);
				} else {
					SideInputInformation<?> info = sideInfos.get(inputType);
					serializers[inputType] = info.getSerializer();
					sideMap.put(reader, true);
				}
			}

			int[] realInputMapping = new int[inputGates.length];

			for (int i = 0; i < inputGates.length; i++) {
				realInputMapping[i] = inputMapping.get(inputGates[i]);
			}

			wrappers[0] = new OperatorWrapper<IN>() {

				private final Counter counter = ((OperatorMetricGroup) headOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();

				@Override
				public void processElement(StreamRecord<IN> record) throws Exception {
					if (disabled) {
						numRecordsInInc();
						headOperator.setKeyContextElement1(record);
						headOperator.processElement(record);
					} else {
						headOperator.bufferMainElement(record);
					}
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
				public void numRecordsInInc() {
					counter.inc();
				}

				@Override
				public void disableWrapper() throws Exception {
					disabled = true;
					Iterator<StreamRecord<?>> it = headOperator.getBufferedElements();
					while (it.hasNext()) {
						numRecordsInInc();
						StreamRecord<IN> record = (StreamRecord<IN>) it.next();
						headOperator.setKeyContextElement1(record);
						headOperator.processElement(record);
					}
				}
			};


			for (int i = 1; i < wrappers.length; i++) {
				final UUID id = sideInfos.get(i).getId();
				wrappers[i] = new OperatorWrapper() {
					//private final Counter counter = ((OperatorMetricGroup) headOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();

					@Override
					public void processElement(StreamRecord record) throws Exception {
						headOperator.processSideInputElement(id, record);
					}

					@Override
					public void processWatermark(Watermark mark) throws Exception {
					}

					@Override
					public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
					}
				};
			}

			inputProcessor = new MultipleStreamInputProcessor<>(
					inputGates,
					serializers,
					realInputMapping,
					sideMap,
					this,
					configuration.getCheckpointMode(),
					getCheckpointLock(),
					getEnvironment().getIOManager(),
					getEnvironment().getTaskManagerInfo().getConfiguration(),
					getStreamStatusMaintainer(),
					this.wrappers);

			// make sure that stream tasks report their I/O statistics
			inputProcessor.setMetricGroup(getEnvironment().getMetricGroup().getIOMetricGroup());

		}
	}

	@Override
	protected void run() throws Exception {
		// cache some references on the stack, to make the code more JIT friendly
		final MultipleStreamInputProcessor<IN> inputProcessor = this.inputProcessor;

		while (running && inputProcessor.processInput()) {
			// all the work happens in the "processInput" method
		}
	}

	@Override
	protected void cleanup() throws Exception {
		if (inputProcessor != null) {
			inputProcessor.cleanup();
		}
	}

	@Override
	protected void cancelTask() {
		running = false;
	}


	public abstract class OperatorWrapper <TYPE> implements Serializable {

		public abstract void processElement(StreamRecord<TYPE> record) throws Exception;
		public abstract void processWatermark(Watermark mark) throws Exception;
		public abstract void processLatencyMarker(LatencyMarker latencyMarker) throws Exception;
		public void numRecordsInInc() { }

		protected boolean disabled = false;
		public void disableWrapper() throws Exception { disabled = true; }
	}

}
