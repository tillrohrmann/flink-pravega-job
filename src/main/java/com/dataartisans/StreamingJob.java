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

package com.dataartisans;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import io.pravega.connectors.flink.FlinkPravegaReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your appliation into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	private static final String DEFAULT_SCOPE = "flinkScope";
	private static final String DEFAULT_CONTROLLER_URI = "tcp://localhost:9090";
	private static final String DEFAULT_STREAM_NAME = "flinkStream";

	private static final Option scopeOption = new Option("s", "scope", true, "The scope (namespace) of the Stream to write to.");
	private static final Option streamOption = new Option("n", "name", true, "The name of the Stream to write to.");
	private static final Option controllerOption = new Option("u", "uri", true, "The URI to the Pravega controller in the form tcp://host:port");


	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(500L);
		env.getCheckpointConfig().setCheckpointTimeout(5000);

		final CommandLine commandLine = parseCommandLineArgs(getOptions(), args);

		final URI controllerURI = new URI(commandLine.hasOption(controllerOption.getOpt()) ? commandLine.getOptionValue(controllerOption.getOpt()) : DEFAULT_CONTROLLER_URI);
		final String scope = commandLine.hasOption(scopeOption.getOpt()) ? commandLine.getOptionValue(scopeOption.getOpt()) : DEFAULT_SCOPE;
		final String streamName = commandLine.hasOption(streamOption.getOpt()) ? commandLine.getOptionValue(streamOption.getOpt()) : DEFAULT_STREAM_NAME;
		final Set<String> streamNames = Collections.singleton(streamName);

		final DataStreamSource<String> pravegaInput = env.addSource(new FlinkPravegaReader<>(
			controllerURI,
			scope,
			streamNames,
			0,
			new SimpleStringSchema()),
			"Pravega Source");

		pravegaInput.print();

		// execute program
		env.execute("Flink Pravega Job");
	}

	private static Options getOptions() {
		final Options options = new Options();

		options.addOption(scopeOption);
		options.addOption(streamOption);
		options.addOption(controllerOption);
		return options;
	}

	private static CommandLine parseCommandLineArgs(Options options, String[] args) throws ParseException {
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = parser.parse(options, args);
		return cmd;
	}
}
