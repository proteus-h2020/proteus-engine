/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.transformations.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.Serializable;
import java.util.UUID;

public class SideInputInformation <TYPE> implements Serializable {

	private final UUID id;
	private final int typeId;
	private final TypeInformation<TYPE> typeInfo;
	private TypeSerializer<TYPE> serializer;

	public SideInputInformation(UUID id, int typeId, TypeInformation<TYPE> typeInfo) {
		this.id = id;
		this.typeId = typeId;
		this.typeInfo = typeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		SideInputInformation<?> that = (SideInputInformation<?>) o;

		return this.id.equals(that.id);

	}

	public UUID getId() {
		return id;
	}

	@Override
	public int hashCode() {
		return this.id.hashCode();
	}

	public TypeInformation<?> getType() {
		return typeInfo;
	}

	@SuppressWarnings("unchecked")
	public void setSerializer(TypeSerializer<?> serializer) {
		this.serializer = (TypeSerializer<TYPE>) serializer;
	}

	public TypeSerializer<TYPE> getSerializer() {
		return serializer;
	}
}
