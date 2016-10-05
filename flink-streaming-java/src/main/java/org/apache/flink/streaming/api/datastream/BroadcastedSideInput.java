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

package org.apache.flink.streaming.api.datastream;


import org.apache.flink.api.common.functions.util.SideInput;

import java.io.Serializable;
import java.util.UUID;

public class BroadcastedSideInput<TYPE> implements SideInput<TYPE>, Serializable {

	private UUID id;
	private transient DataStream<TYPE> stream;

	public BroadcastedSideInput(DataStream<TYPE> stream) {
		this.stream = stream;
		this.id = UUID.randomUUID();
	}

	@Override
	public UUID id() {
		return this.id;
	}

	public DataStream<TYPE> stream() {
		return this.stream;
	}


	@Override
	public final boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SideInput<?> input = (SideInput<?>) o;

		return id.equals(input.id());

	}

	@Override
	public final int hashCode() {
		return this.id.hashCode();
	}
}
