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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.jobgraph.tasks.StatefulTask;
import org.apache.flink.runtime.metrics.groups.IOMetricGroup;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
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
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.runtime.tasks.MultipleInputsStreamTask;

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
 */
@Internal
public class StreamSideInputsProcessor {

	private final RecordDeserializer<DeserializationDelegate<StreamElement>>[] recordDeserializers;

	private RecordDeserializer<DeserializationDelegate<StreamElement>> currentRecordDeserializer;

	private final CheckpointBarrierHandler barrierHandler;

	// We need to keep track of the channel from which a buffer came, so that we can
	// appropriately map the watermarks to input channels
	private int currentChannel = -1;

	private boolean isFinished;

	private final long[] watermarks;
	private long lastEmittedWatermark;

	private final DeserializationDelegate<StreamElement>[] deserializationDelegate;

	//private Counter numRecordsIn;

	private final int[] inputMapping;

	@SuppressWarnings("unchecked")
	public StreamSideInputsProcessor(InputGate[] inputGates,
								TypeSerializer<?>[] inputsSerializers,
								int[] inputMapping,
								StatefulTask checkpointedTask,
								CheckpointingMode checkpointMode,
								IOManager ioManager,
								boolean enableWatermarkMultiplexing) throws IOException {

		InputGate inputGate = InputGateUtil.createInputGate(inputGates);

		if (checkpointMode == CheckpointingMode.EXACTLY_ONCE) {
			this.barrierHandler = new BarrierBuffer(inputGate, ioManager);
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

		this.inputMapping = inputMapping;


		this.deserializationDelegate = new NonReusingDeserializationDelegate[inputsSerializers.length];

		for (int i = 0; i < deserializationDelegate.length; i++) {
			TypeSerializer<?> inputSerializer = inputsSerializers[i];
			if (enableWatermarkMultiplexing) {
				MultiplexingStreamRecordSerializer<?> ser = new MultiplexingStreamRecordSerializer(inputSerializer);
				this.deserializationDelegate[i] = new NonReusingDeserializationDelegate<>(ser);
			} else {
				StreamRecordSerializer<?> ser = new StreamRecordSerializer(inputSerializer);
				this.deserializationDelegate[i] = (NonReusingDeserializationDelegate<StreamElement>) new NonReusingDeserializationDelegate(ser);
			}
		}



		// Initialize one deserializer per input channel
		this.recordDeserializers = new SpillingAdaptiveSpanningRecordDeserializer[inputGate.getNumberOfInputChannels()];

		for (int i = 0; i < recordDeserializers.length; i++) {
			recordDeserializers[i] = new SpillingAdaptiveSpanningRecordDeserializer<>(
				ioManager.getSpillingDirectoriesPaths());
		}

		watermarks = new long[inputGate.getNumberOfInputChannels()];
		for (int i = 0; i < inputGate.getNumberOfInputChannels(); i++) {
			watermarks[i] = Long.MIN_VALUE;
		}
		lastEmittedWatermark = Long.MIN_VALUE;
	}


	@SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter unchecked")
	public boolean processInput(MultipleInputsStreamTask.OperatorWrapper[] wrappers, final Object lock) throws Exception {
		if (isFinished) {
			return false;
		}
		//if (numRecordsIn == null) {
		//	numRecordsIn = wrappers[0].getNumberRecordsInCounter();
		//}

		while (true) {
			if (currentRecordDeserializer != null) {
				int inputIndex = inputMapping[currentChannel];
				DeserializationResult result = currentRecordDeserializer.getNextRecord(deserializationDelegate[inputIndex]);

				if (result.isBufferConsumed()) {
					currentRecordDeserializer.getCurrentBuffer().recycle();
					currentRecordDeserializer = null;
				}

				if (result.isFullRecord()) {
					StreamElement recordOrWatermark = deserializationDelegate[inputIndex].getInstance();

					if (recordOrWatermark.isWatermark()) {
						long watermarkMillis = recordOrWatermark.asWatermark().getTimestamp();
						if (watermarkMillis > watermarks[currentChannel]) {
							watermarks[currentChannel] = watermarkMillis;
							long newMinWatermark = Long.MAX_VALUE;
							for (long watermark : watermarks) {
								newMinWatermark = Math.min(watermark, newMinWatermark);
							}
							if (newMinWatermark > lastEmittedWatermark) {
								lastEmittedWatermark = newMinWatermark;
								synchronized (lock) {
									wrappers[inputIndex].processWatermark(new Watermark(lastEmittedWatermark));
								}
							}
						}
						continue;
					} else {
						// now we can do the actual processing
						StreamRecord<?> record = recordOrWatermark.asRecord();
						synchronized (lock) {
							wrappers[inputIndex].getNumberRecordsInCounter().inc();
							wrappers[inputIndex].processElement(record);
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
				return false;
			}
		}
	}

	public void setReporter(AccumulatorRegistry.Reporter reporter) {
		for (RecordDeserializer<?> deserializer : recordDeserializers) {
			deserializer.setReporter(reporter);
		}
	}

	/**
	 * Sets the metric group for this StreamInputProcessor.
	 *
	 * @param metrics metric group
	 */
	public void setMetricGroup(IOMetricGroup metrics) {
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
}
