/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Note:
 * this code is available in Flink 1.15+, but as of now (12/2021) Amazon Kinesis supports Flink version 1.13.2.
 * Minimal changes were done to port the code to older Flink version. In future, the goal is to use the original source.
 * Original source link:
 * https://github.com/apache/flink/tree/master/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/sink
 */

package imported.vnext.org.apache.flink.connector.base.sink.sink;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Integration tests of a baseline generic sink that implements the AsyncSinkBase. */
public class AsyncSinkBaseITCase {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void testWriteTwentyThousandRecordsToGenericSink() throws Exception {
        env.fromSequence(1, 20000).map(Object::toString).sinkTo(new ArrayListAsyncSink());
        env.execute("Integration Test: AsyncSinkBaseITCase").getJobExecutionResult();
    }

    @Test
    public void testFailuresOnPersistingToDestinationAreCaughtAndRaised() {
        env.fromSequence(999_999, 1_000_100)
                .map(Object::toString)
                .sinkTo(new ArrayListAsyncSink(1, 1, 2, 10, 1000, 10));
        Exception e =
                assertThrows(
                        JobExecutionException.class,
                        () -> env.execute("Integration Test: AsyncSinkBaseITCase"));

        final String expectedMessage = "Intentional error on persisting 1_000_000 to ArrayListDestination";
        final String actualStackTrace = printStackTraceToString(e);
        assertTrue(
                actualStackTrace.contains(expectedMessage),
                String.format("Expected exception stack trace to contain '%s', but found: '%s'",
                        expectedMessage,
                        actualStackTrace)
        );
    }

    private String printStackTraceToString(Exception e) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    @Test
    public void testThatNoIssuesOccurWhenCheckpointingIsEnabled() throws Exception {
        env.enableCheckpointing(20);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.milliseconds(200)));
        env.fromSequence(1, 10_000).map(Object::toString).sinkTo(new ArrayListAsyncSink());
        env.execute("Integration Test: AsyncSinkBaseITCase");
    }
}
