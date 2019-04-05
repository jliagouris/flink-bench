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

package sources;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * A naive source function that generates integers
 */
public class IntegerSourceFunction extends RichParallelSourceFunction<Integer> {

    private volatile boolean running = true;
    private long eventsCountSoFar = 0;
    private final int rate;
    private final long maxEvents;

    public IntegerSourceFunction(int srcRate, long maxEvents) {
        this.rate = srcRate;
        this.maxEvents = maxEvents;
    }

    @Override
    public void run(SourceContext<Integer> ctx) throws Exception {
        while (running && eventsCountSoFar < maxEvents) {
            long emitStartTime = System.currentTimeMillis();

            for (int i = 0; i < rate; i++) {
                ctx.collect(i);
                eventsCountSoFar++;
            }

            // Sleep for the rest of timeslice if needed
            long emitTime = System.currentTimeMillis() - emitStartTime;
            if (emitTime < 1000) {
                Thread.sleep(1000 - emitTime);
            }

        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}