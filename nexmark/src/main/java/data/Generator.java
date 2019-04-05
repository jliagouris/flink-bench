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

package data;

import sources.AuctionSourceFunction;
import sources.BidSourceFunction;
import sources.PersonSourceFunction;
import org.apache.beam.sdk.nexmark.model.Auction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.apache.beam.sdk.nexmark.model.Person;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Generator {

    private static final Logger logger  = LoggerFactory.getLogger(Generator.class);

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        final int auctionRate = params.getInt("auction-rate", 1000);
        final int bidRate = params.getInt("bid-rate", 100000);
        final int personRate = params.getInt("person-rate", 10000);

        final String repo = params.get("repo", "./");

        System.out.println("Starting data generation...");
        System.out.println("Auctions' rate: " + auctionRate);
        System.out.println("Bids' rate: " + bidRate);
        System.out.println("Persons' rate: " + personRate);
        System.out.println("Data repository: " + repo);

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.disableOperatorChaining();

        // Enable latency tracking
        env.getConfig().setLatencyTrackingInterval(5000);

        // Auctions source
        DataStream<Auction> auctions = env.addSource(new AuctionSourceFunction(auctionRate))
                .setParallelism(params.getInt("p-auction-source", 1))
                .name("Auctions Source")
                .uid("Auction-Source");

        // Bids source
        DataStream<Bid> bids = env.addSource(new BidSourceFunction(bidRate))
                .setParallelism(params.getInt("p-bid-source", 1))
                .name("Bids Source")
                .uid("Bid-Source");

        // Persons source
        DataStream<Person> persons = env.addSource(new PersonSourceFunction(personRate))
                .setParallelism(params.getInt("p-person-source", 1))
                .name("Persons Source")
                .uid("Person-Source");

        GenericTypeInfo<Object> objectTypeInfo = new GenericTypeInfo<>(Object.class);

        // Auctions sink
        auctions.writeAsText(repo+"/auctions.data")
                .setParallelism(1)
                .name("Auctions Sink")
                .uid("Auction-Sink");

        // Bids sink
        bids.writeAsText(repo+"/bids.data")
                .setParallelism(1)
                .name("Bids Sink")
                .uid("Bid-Sink");

        // Persons sink
        persons.writeAsText(repo+"/persons.data")
                .setParallelism(1)
                .name("Person Sink")
                .uid("Persons-Sink");

        // Execute program
        env.execute("Data Generation");
    }
}
