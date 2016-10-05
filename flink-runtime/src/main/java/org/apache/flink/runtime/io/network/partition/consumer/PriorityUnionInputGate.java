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

package org.apache.flink.runtime.io.network.partition.consumer;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.util.event.EventListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Input gate wrapper to union the input from multiple input gates. Each gate has a priority.
 *
 * <p> Each input gate has input channels attached from which it reads data. At each input gate, the
 * input channels have unique IDs from 0 (inclusive) to the number of input channels (exclusive).
 *
 * <pre>
 * +---+---+      +---+---+---+
 * | 0 | 1 |      | 0 | 1 | 2 |
 * +--------------+--------------+
 * | Input gate 0 | Input gate 1 |
 * +--------------+--------------+
 * </pre>
 *
 * The union input gate maps these IDs from 0 to the *total* number of input channels across all
 * unioned input gates, e.g. the channels of input gate 0 keep their original indexes and the
 * channel indexes of input gate 1 are set off by 2 to 2--4.
 *
 * <pre>
 * +---+---++---+---+---+
 * | 0 | 1 || 2 | 3 | 4 |
 * +--------------------+
 * | Union input gate   |
 * +--------------------+
 * </pre>
 *
 * It is possible to recursively union union input gates.
 */
public class PriorityUnionInputGate implements InputGate {


	public enum GatesPriority {
		HIGH,
		NORMAL
	}

	/** The input gates to union. */
	private final InputGate[] inputGates;

	private final Set<InputGate> inputGatesWithRemainingData;

	/** Data availability listener across all unioned input gates. */
	private final PriorityInputGateListener inputGateListener;

	/** The total number of input channels across all unioned input gates. */
	private final int totalNumberOfInputChannels;

	/**
	 * A mapping from input gate to (logical) channel index offset. Valid channel indexes go from 0
	 * (inclusive) to the total number of input channels (exclusive).
	 */
	private final Map<InputGate, Integer> inputGateToIndexOffsetMap;

	/** Flag indicating whether partitions have been requested. */
	private boolean requestedPartitionsFlag;

	public PriorityUnionInputGate(final InputGate[] inputGates, final Map<InputGate, GatesPriority> priorities) { //(InputGate... inputGates) {

		int size = inputGates.length;

		checkArgument(size > 1, "Union input gate should union at least two input gates.");

		this.inputGates = inputGates;

		this.inputGateToIndexOffsetMap = Maps.newHashMapWithExpectedSize(size);
		this.inputGatesWithRemainingData = Sets.newHashSetWithExpectedSize(size);

		int currentNumberOfInputChannels = 0;
		int currentNumberOfHighPriorityGates = 0;

		for (InputGate inputGate : inputGates) {
			// The offset to use for buffer or event instances received from this input gate.
			inputGateToIndexOffsetMap.put(checkNotNull(inputGate), currentNumberOfInputChannels);
			inputGatesWithRemainingData.add(inputGate);

			currentNumberOfInputChannels += inputGate.getNumberOfInputChannels();

			if (priorities.get(inputGate) == GatesPriority.HIGH) {
				currentNumberOfHighPriorityGates++;
			}
		}

		this.totalNumberOfInputChannels = currentNumberOfInputChannels;

		this.inputGateListener = new PriorityInputGateListener(inputGates, this, priorities, currentNumberOfHighPriorityGates);
	}

	/**
	 * Returns the total number of input channels across all unioned input gates.
	 */
	@Override
	public int getNumberOfInputChannels() {
		return totalNumberOfInputChannels;
	}

	@Override
	public boolean isFinished() {
		for (InputGate inputGate : inputGates) {
			if (!inputGate.isFinished()) {
				return false;
			}
		}

		return true;
	}

	@Override
	public void requestPartitions() throws IOException, InterruptedException {
		if (!requestedPartitionsFlag) {
			for (InputGate inputGate : inputGates) {
				inputGate.requestPartitions();
			}

			requestedPartitionsFlag = true;
		}
	}

	@Override
	public BufferOrEvent getNextBufferOrEvent() throws IOException, InterruptedException {

		if (inputGatesWithRemainingData.isEmpty()) {
			return null;
		}

		// Make sure to request the partitions, if they have not been requested before.
		requestPartitions();

		//final InputGate inputGate = inputGateListener.getNextInputGateToReadFrom();

		InputGate inputGate;

		do {
			inputGate = inputGateListener.getNextInputGateToReadFrom();
		} while (inputGate == null);

		final BufferOrEvent bufferOrEvent = inputGate.getNextBufferOrEvent();

		if (bufferOrEvent.isEvent()
			&& bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class
			&& inputGate.isFinished()) {

			if (!inputGatesWithRemainingData.remove(inputGate)) {
				throw new IllegalStateException("Couldn't find input gate in set of remaining " +
					"input gates.");
			}
			inputGateListener.notifyConsumedInputGate(inputGate);
		}

		// Set the channel index to identify the input channel (across all unioned input gates)
		final int channelIndexOffset = inputGateToIndexOffsetMap.get(inputGate);

		bufferOrEvent.setChannelIndex(channelIndexOffset + bufferOrEvent.getChannelIndex());

		return bufferOrEvent;
	}

	@Override
	public void sendTaskEvent(TaskEvent event) throws IOException {
		for (InputGate inputGate : inputGates) {
			inputGate.sendTaskEvent(event);
		}
	}

	@Override
	public void registerListener(EventListener<InputGate> listener) {
		// This method is called from the consuming task thread.
		inputGateListener.registerListener(listener);
	}

	@Override
	public int getPageSize() {
		int pageSize = -1;
		for (InputGate gate : inputGates) {
			if (pageSize == -1) {
				pageSize = gate.getPageSize();
			} else if (gate.getPageSize() != pageSize) {
				throw new IllegalStateException("Found input gates with different page sizes.");
			}
		}
		return pageSize;
	}

	/**
	 * Data availability listener at all unioned input gates.
	 *
	 * <p> The listener registers itself at each input gate and is notified for *each incoming
	 * buffer* at one of the unioned input gates.
	 */
	private static class PriorityInputGateListener implements EventListener<InputGate> {

		private final PriorityUnionInputGate parent;

		private final BlockingQueue<InputGate> inputGatesWithDataNormal = new LinkedBlockingQueue<>();
		private final BlockingQueue<InputGate> inputGatesWithDataHigh = new LinkedBlockingQueue<>();

		private final List<EventListener<InputGate>> registeredListeners = new CopyOnWriteArrayList<>();

		private final Map<InputGate, GatesPriority> priorities;

		/** the total number of input gates with high priority. */
		private int currentNumberOfHighPriorityGates;

		public PriorityInputGateListener(InputGate[] inputGates,
											PriorityUnionInputGate parent,
											final Map<InputGate, GatesPriority> priorities,
											final int currentNumberOfHighPriorityGates) {
			for (InputGate inputGate : inputGates) {
				inputGate.registerListener(this);
			}

			this.parent = parent;
			this.priorities = priorities;
			this.currentNumberOfHighPriorityGates = currentNumberOfHighPriorityGates;
		}

		@Override
		public void onEvent(InputGate inputGate) {
			// This method is called from the input channel thread, which can be either the same
			// thread as the consuming task thread or a different one.
			switch (this.priorities.get(inputGate)) {
				case HIGH:
					inputGatesWithDataHigh.add(inputGate);
					break;
				case NORMAL:
					inputGatesWithDataNormal.add(inputGate);
					break;
				default:
					throw new RuntimeException("unsupported priority");
			}

			for (int i = 0; i < registeredListeners.size(); i++) {
				registeredListeners.get(i).onEvent(parent);
			}
		}

		InputGate getNextInputGateToReadFrom() throws InterruptedException {
			if (currentNumberOfHighPriorityGates > 0) {
				return inputGatesWithDataHigh.take();
			}
			return inputGatesWithDataNormal.take();
		}

		public void notifyConsumedInputGate(InputGate consumedGate) {
			if (priorities.get(consumedGate) == GatesPriority.HIGH) {
				currentNumberOfHighPriorityGates--;
			}
		}

		public void registerListener(EventListener<InputGate> listener) {
			registeredListeners.add(checkNotNull(listener));
		}
	}
}
