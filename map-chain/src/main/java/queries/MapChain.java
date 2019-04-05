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

package queries;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sinks.DummyLatencyCountingSink;
import sources.IntegerSourceFunction;

public class MapChain {

	private static final Logger logger  = LoggerFactory.getLogger(MapChain.class);

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		final int chainLength = params.getInt("chain-length", 5);
		final boolean enableChaining = params.getBoolean("enable-chaining", true);

		if (!enableChaining) {
			env.disableOperatorChaining();
		}

		// Enable latency tracking
		env.getConfig().setLatencyTrackingInterval(params.getInt("latency-interval", 5000));

		DataStream<Integer> stream = env.addSource(new IntegerSourceFunction(
				params.getInt("source-rate", 50000),
				params.getLong("source-max-events", 1000000)))
				.name("Custom Source: Integers")
				.setParallelism(params.getInt("source-parallelism", 1));

		DataStream<Integer> mappedStream = stream;

		for (int i = 0; i < chainLength; i ++) {
			mappedStream = mappedStream.map(new MapFunction<Integer, Integer>() {

				@Override
				public Integer map(Integer i) throws Exception {
					return i;
				}
			}).name("mapper-"+i);
		}

		GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);
		mappedStream.transform("DummyLatencySink", objectTypeInfo, new DummyLatencyCountingSink<>(logger))
				.setParallelism(1)
				.name("Latency Sink");

		// Execute program
		env.execute("Map chain");
	}

}
