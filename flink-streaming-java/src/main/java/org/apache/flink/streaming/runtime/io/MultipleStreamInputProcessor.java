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

package org.apache.flink.streaming.runtime.io;

import java.io.IOException;
import java.util.Map;

import eu.proteus.flink.annotation.Proteus;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.io.network.partition.consumer.InputGateListener;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult;
import org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatus;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.MultipleInputStreamTask;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input reader for {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTask}.
 *
 * <p>
 * This also keeps track of {@link Watermark} events and forwards them to event subscribers
 * once the {@link Watermark} from all inputs advances.
 *
 * <p>
 * Forwarding elements or watermarks must be protected by synchronizing on the given lock
 * object. This ensures that we don't call methods on a {@link OneInputStreamOperator} concurrently
 * with the timer callback or other things.
 *
 * @param <IN> The type of the record that can be read with this record reader.
 */
@Internal
@Proteus
public class MultipleStreamInputProcessor<IN> implements InputGateListener {

	private enum WrapperStatus {
		READING_ALL,
		SWITCHING,
		ONLY_MAIN
	}

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	private final CheckpointBarrierHandler barrierHandler;

	// We need to keep track of the channel from which a buffer came, so that we can
	// appropriately map the watermarks to input channels
	private int currentChannel = -1;

	private boolean isFinished;

	private final long[] watermarks;
	private long lastEmittedWatermark;

	private final Object lock;

	private final DeserializationDelegate<StreamElement>[] deserializationDelegates;

	int numberOfSideInputs;
	private Map<InputGate, Boolean> sideMap;
	private int sideInputsLoaded;
	private volatile WrapperStatus wrapperStatus;

	private final int[] inputChannelsMapping;

	private final StreamStatusMaintainer streamStatusMaintainer;

	private final MultipleInputStreamTask.OperatorWrapper[] wrappers;

	private final StatusWatermarkValve statusWatermarkValve;
	private final ForwardingValveOutputHandler outputHandler;


	@SuppressWarnings("unchecked")
	public MultipleStreamInputProcessor(
		InputGate[] inputGates,
		TypeSerializer<?>[] serializers,
		int[] inputMapping,
		Map<InputGate, Boolean> sideMap,
		StatefulTask checkpointedTask,
		CheckpointingMode checkpointMode,
		Object lock,
		IOManager ioManager,
		Configuration taskManagerConfig,
		StreamStatusMaintainer streamStatusMaintainer,
		MultipleInputStreamTask.OperatorWrapper[] wrappers) throws IOException {

		UnionInputGate inputGate = (UnionInputGate) InputGateUtil.createInputGate(inputGates);
		inputGate.registerListener(this);

		if (checkpointMode == CheckpointingMode.EXACTLY_ONCE) {
			long maxAlign = taskManagerConfig.getLong(TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT);
			if (!(maxAlign == -1 || maxAlign > 0)) {
				throw new IllegalConfigurationException(
					TaskManagerOptions.TASK_CHECKPOINT_ALIGNMENT_BYTES_LIMIT.key()
						+ " must be positive or -1 (infinite)");
			}
			this.barrierHandler = new BarrierBuffer(inputGate, ioManager, maxAlign);
		}
		else if (checkpointMode == CheckpointingMode.AT_LEAST_ONCE) {
			this.barrierHandler = new BarrierTracker(inputGate);
		}
		else {
			throw new IllegalArgumentException("Unrecognized Checkpointing Mode: " + checkpointMode);
		}

		if (checkpointedTask != null) {
			this.barrierHandler.registerCheckpointEventHandler(checkpointedTask);
		}

		this.lock = checkNotNull(lock);

		this.sideMap = sideMap;

		this.deserializationDelegates = new NonReusingDeserializationDelegate[serializers.length];

		for (int i = 0; i < deserializationDelegates.length; i++) {
			StreamElementSerializer<?> ser = new StreamElementSerializer<>(serializers[i]);
			this.deserializationDelegates[i] = new NonReusingDeserializationDelegate<>(ser);
		}

		int channelsCount = inputGate.getNumberOfInputChannels();
		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[channelsCount];

		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		int low = 0;
		this.inputChannelsMapping = new int[channelsCount];
		for (int i = 0; i < inputMapping.length; i++) {
			int idx = inputMapping[i];
			int currentGateChannelsCount = inputGate.getNumberOfInputChannelsForGate(i);
			for (int j = 0; j < currentGateChannelsCount; j++) {
				inputChannelsMapping[low + j] = idx;
			}
			low += currentGateChannelsCount;
		}

		watermarks = new long[inputGate.getNumberOfInputChannels()];
		for (int i = 0; i < inputGate.getNumberOfInputChannels(); i++) {
			watermarks[i] = Long.MIN_VALUE;
		}
		lastEmittedWatermark = Long.MIN_VALUE;

		sideInputsLoaded = 0;
		wrapperStatus = WrapperStatus.READING_ALL;
		numberOfSideInputs = serializers.length - 1;

		this.lastEmittedWatermark = Long.MIN_VALUE;

		this.streamStatusMaintainer = checkNotNull(streamStatusMaintainer);
		this.wrappers = checkNotNull(wrappers);

		this.outputHandler = new ForwardingValveOutputHandler(wrappers, lock);
		this.statusWatermarkValve = new StatusWatermarkValve(low, outputHandler);
	}

	private void checkWrappers(final MultipleInputStreamTask.OperatorWrapper wrapper) throws Exception {
		if (wrapperStatus == WrapperStatus.SWITCHING) {
			wrapper.disableWrapper();
			wrapperStatus = WrapperStatus.ONLY_MAIN;
		}
	}

	@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
	public boolean processInput() throws Exception {
		if (isFinished) {
			checkWrappers(wrappers[0]);
			return false;
		}
		checkWrappers(wrappers[0]);
		while (true) {
			if (currentRecordDeserializer != null) {
				final int inputIndex = inputChannelsMapping[currentChannel];
				final DeserializationDelegate<StreamElement> deserializationDelegate = deserializationDelegates[inputIndex];
				final MultipleInputStreamTask.OperatorWrapper wrapper = wrappers[inputIndex];
				outputHandler.setCurrentOperatorIndex(inputIndex);
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycle();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrMark = deserializationDelegate.getInstance();

					if (recordOrMark.isWatermark()) {
						long watermarkMillis = recordOrMark.asWatermark().getTimestamp();
						if (watermarkMillis > watermarks[currentChannel]) {
							watermarks[currentChannel] = watermarkMillis;
							long newMinWatermark = Long.MAX_VALUE;
							for (long watermark: watermarks) {
								newMinWatermark = Math.min(watermark, newMinWatermark);
							}
							if (newMinWatermark > lastEmittedWatermark) {
								lastEmittedWatermark = newMinWatermark;
								synchronized (lock) {
									wrapper.processWatermark(new Watermark(lastEmittedWatermark));
								}
							}
						}
						continue;
					} else if (recordOrMark.isStreamStatus()) {
						// handle stream status
						statusWatermarkValve.inputStreamStatus(recordOrMark.asStreamStatus(), currentChannel);
						continue;
					} else if(recordOrMark.isLatencyMarker()) {
						// handle latency marker
						synchronized (lock) {
							wrapper.processLatencyMarker(recordOrMark.asLatencyMarker());
						}
						continue;
					} else {
						// now we can do the actual processing
						StreamRecord<?> record = recordOrMark.asRecord();
						synchronized (lock) {
							wrapper.processElement(record);
						}
						return true;
					}
				}
			}

			final BufferOrEvent bufferOrEvent = barrierHandler.getNextNonBlocked();
			if (bufferOrEvent != null) {
				if (bufferOrEvent.isBuffer()) {
					currentChannel = bufferOrEvent.getChannelIndex();
					currentRecordDeserializer = recordDeserializers[currentChannel];
					currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());
				}
				else {
					// Event received
					final AbstractEvent event = bufferOrEvent.getEvent();
					if (event.getClass() != EndOfPartitionEvent.class) {
						throw new IOException("Unexpected event: " + event);
					}
				}
			}
			else {
				isFinished = true;
				if (!barrierHandler.isEmpty()) {
					throw new IllegalStateException("Trailing data in checkpoint barrier handler.");
				}
				return true;
			}
		}
	}

	/**
	 * Sets the metric group for this StreamInputProcessor.
	 *
	 * @param metrics metric group
	 */
	public void setMetricGroup(TaskIOMetricGroup metrics) {
		metrics.gauge("currentLowWatermark", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return lastEmittedWatermark;
			}
		});

		metrics.gauge("checkpointAlignmentTime", new Gauge<Long>() {
			@Override
			public Long getValue() {
				return barrierHandler.getAlignmentDurationNanos();
			}
		});
	}

	public void cleanup() throws IOException {
		// clear the buffers first. this part should not ever fail
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			Buffer buffer = deserializer.getCurrentBuffer();
			if (buffer != null && !buffer.isRecycled()) {
				buffer.recycle();
			}
		}

		// cleanup the barrier handler resources
		barrierHandler.cleanup();
	}


	@Override
	public void onInputGateConsumed(InputGate inputGate) {
		if (sideMap.containsKey(inputGate) && sideMap.get(inputGate)) {
			sideInputsLoaded++;
		}
		if (numberOfSideInputs == sideInputsLoaded && wrapperStatus == WrapperStatus.READING_ALL) {
			wrapperStatus = WrapperStatus.SWITCHING;
		}
	}

	@Override
	public void notifyInputGateNonEmpty(InputGate inputGate) {

	}

	private class ForwardingValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {

		private final MultipleInputStreamTask.OperatorWrapper[] wrappers;
		private final Object lock;

		private int currentOperatorIndex;

		public ForwardingValveOutputHandler(final MultipleInputStreamTask.OperatorWrapper[] wrappers, final Object lock) {
			this.wrappers = checkNotNull(wrappers);
			this.lock = checkNotNull(lock);
			this.currentOperatorIndex = 0;
		}

		public void setCurrentOperatorIndex(int idx) {
			this.currentOperatorIndex = idx;
		}

		@Override
		public void handleWatermark(Watermark watermark) {
			try {
				synchronized (lock) {
					lastEmittedWatermark = watermark.getTimestamp();
					wrappers[currentOperatorIndex].processWatermark(watermark);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output watermark: ", e);
			}
		}

		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			try {
				synchronized (lock) {
					streamStatusMaintainer.toggleStreamStatus(streamStatus);
				}
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while processing valve output stream status: ", e);
			}
		}
	}
}
