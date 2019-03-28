/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.siddhi;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.siddhi.control.MetadataControlEvent;
import org.apache.flink.streaming.siddhi.control.OperationControlEvent;
import org.apache.flink.streaming.siddhi.exception.UndefinedStreamException;
import org.apache.flink.streaming.siddhi.extension.CustomPlusFunctionExtension;
import org.apache.flink.streaming.siddhi.source.Event;
import org.apache.flink.streaming.siddhi.source.RandomEventSource;
import org.apache.flink.streaming.siddhi.source.RandomTupleSource;
import org.apache.flink.streaming.siddhi.source.RandomWordSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.siddhi.control.ControlEvent;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Flink-siddhi library integration test cases
 */
public class SiddhiCEPITCase extends AbstractTestBase implements Serializable {
    @Rule
    public transient TemporaryFolder tempFolder = new TemporaryFolder();

    @org.junit.Test
    public void testReturnsTransformRow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input = env.addSource(new RandomEventSource(5));

        SiddhiCEP cep = SiddhiCEP.getSiddhiEnvironment(env);
        SiddhiStream.SingleSiddhiStream stream = cep
                .define("inputStream", input, "id", "name", "price", "timestamp");

        DataStream output = stream
                .cql("from inputStream select timestamp, id, name, price,id as s1,id as s2,id as s3,id as s4,id as s5,id as s6," +
                        "id as s7,id as s8,id as s9,id as s10,id as s11,id as s12,id as s13,id as s14,id as s15,id as s16," +
                        "id as s17,id as s18,id as s19,id as s20,id as s21,id as s22,id as s23,id as s24,id as s25 insert into  test_stream")
                .returnsTransformRow("test_stream");
        cep.registerStream("test_stream", output, "timestamp", "id", "name", "price", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11", "s12", "s13",
                "s14", "s15", "s16", "s17", "s18", "s19", "s20", "s21", "s22", "s23", "s24", "s25");
        stream = cep.from("test_stream");

        DataStream output2 = stream.cql("from test_stream select timestamp, id, name, price insert into  outstream2")
                .returnAsRow("outstream2");
        output2.print();

        env.execute();
    }

    @Test
    public void testSimpleWriteAndRead() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input = env.fromElements(
            Event.of(1, "start", 1.0),
            Event.of(2, "middle", 2.0),
            Event.of(3, "end", 3.0),
            Event.of(4, "start", 4.0),
            Event.of(5, "middle", 5.0),
            Event.of(6, "end", 6.0)
        );

        String path = tempFolder.newFile().toURI().toString();
        input.transform("transformer", TypeInformation.of(Event.class), new StreamMap<>(new MapFunction<Event, Event>() {
            @Override
            public Event map(Event event) throws Exception {
                return event;
            }
        })).writeAsText(path);
        env.execute();
        Assert.assertEquals(6, getLineCount(path));
    }

    @Test
    public void testSimplePojoStreamAndReturnPojo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input = env.fromElements(
            Event.of(1, "start", 1.0),
            Event.of(2, "middle", 2.0),
            Event.of(3, "end", 3.0),
            Event.of(4, "start", 4.0),
            Event.of(5, "middle", 5.0),
            Event.of(6, "end", 6.0)
        );

        DataStream<Event> output = SiddhiCEP
            .define("inputStream", input, "id", "name", "price")
            .cql("from inputStream insert into  outputStream")
            .returns("outputStream", Event.class);
        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(6, getLineCount(resultPath));
    }

    @Test
    public void testSimplePojoStreamAndReturnRow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input = env.fromElements(
            Event.of(1, "start", 1.0),
            Event.of(2, "middle", 2.0),
            Event.of(3, "end", 3.0),
            Event.of(4, "start", 4.0),
            Event.of(5, "middle", 5.0),
            Event.of(6, "end", 6.0)
        );

        DataStream<Row> output = SiddhiCEP
            .define("inputStream", input, "id", "name", "price")
            .cql("from inputStream insert into  outputStream")
            .returnAsRow("outputStream");
        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(6, getLineCount(resultPath));
    }

    @Test
    public void testUnboundedPojoSourceAndReturnTuple() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input = env.addSource(new RandomEventSource(5));

        DataStream<Tuple4<Long, Integer, String, Double>> output = SiddhiCEP
            .define("inputStream", input, "id", "name", "price", "timestamp")
            .cql("from inputStream select timestamp, id, name, price insert into  outputStream")
            .returns("outputStream");

        DataStream<Integer> following = output.map(new MapFunction<Tuple4<Long, Integer, String, Double>, Integer>() {
            @Override
            public Integer map(Tuple4<Long, Integer, String, Double> value) throws Exception {
                return value.f1;
            }
        });
        String resultPath = tempFolder.newFile().toURI().toString();
        following.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(5, getLineCount(resultPath));
    }

    @Test
    public void testUnboundedTupleSourceAndReturnTuple() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple4<Integer, String, Double, Long>> input = env
            .addSource(new RandomTupleSource(5).closeDelay(1500)).keyBy(1);

        DataStream<Tuple4<Long, Integer, String, Double>> output = SiddhiCEP
            .define("inputStream", input, "id", "name", "price", "timestamp")
            .cql("from inputStream select timestamp, id, name, price insert into  outputStream")
            .returns("outputStream");

        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(5, getLineCount(resultPath));
    }

    @Test
    public void testUnboundedPrimitiveTypeSourceAndReturnTuple() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.addSource(new RandomWordSource(5).closeDelay(1500));

        DataStream<Tuple1<String>> output = SiddhiCEP
            .define("wordStream", input, "words")
            .cql("from wordStream select words insert into  outputStream")
            .returns("outputStream");

        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(5, getLineCount(resultPath));
    }

    @Test(expected = InvalidTypesException.class)
    public void testUnboundedPojoSourceButReturnInvalidTupleType() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input = env.addSource(new RandomEventSource(5).closeDelay(1500));

        DataStream<Tuple5<Long, Integer, String, Double, Long>> output = SiddhiCEP
            .define("inputStream", input, "id", "name", "price", "timestamp")
            .cql("from inputStream select timestamp, id, name, price insert into  outputStream")
            .returns("outputStream");

        DataStream<Long> following = output.map(new MapFunction<Tuple5<Long, Integer, String, Double, Long>, Long>() {
            @Override
            public Long map(Tuple5<Long, Integer, String, Double, Long> value) throws Exception {
                return value.f0;
            }
        });

        String resultPath = tempFolder.newFile().toURI().toString();
        following.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(5, getLineCount(resultPath));
        env.execute();
    }

    @Test
    public void testUnboundedPojoStreamAndReturnMap() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStream<Event> input = env.addSource(new RandomEventSource(5));

        DataStream<Map<String,Object>> output = SiddhiCEP
            .define("inputStream", input, "id", "name", "price", "timestamp")
            .cql("from inputStream select timestamp, id, name, price insert into  outputStream")
            .returnAsMap("outputStream");

        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(5, getLineCount(resultPath));
    }

    @Test
    public void testUnboundedPojoStreamAndReturnPojo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input = env.addSource(new RandomEventSource(5));
        input.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
            @Override
            public long extractAscendingTimestamp(Event element) {
                return element.getTimestamp();
            }
        });

        DataStream<Event> output = SiddhiCEP
            .define("inputStream", input, "id", "name", "price", "timestamp")
            .cql("from inputStream select timestamp, id, name, price insert into  outputStream")
            .returns("outputStream", Event.class);

        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(5, getLineCount(resultPath));
    }


    @Test
    public void testMultipleUnboundedPojoStreamSimpleUnion() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input1 = env.addSource(new RandomEventSource(10), "input1");
        DataStream<Event> input2 = env.addSource(new RandomEventSource(10), "input2");
        DataStream<Event> input3 = env.addSource(new RandomEventSource(10), "input2");
        DataStream<Event> output = SiddhiCEP
            .define("inputStream1", input1, "id", "name", "price", "timestamp")
            .union("inputStream2", input2, "id", "name", "price", "timestamp")
            .union("inputStream3", input3, "id", "name", "price", "timestamp")
            .cql(
                "from inputStream1 select timestamp, id, name, price insert into outputStream;"
                    + "from inputStream2 select timestamp, id, name, price insert into outputStream;"
                    + "from inputStream3 select timestamp, id, name, price insert into outputStream;"
            )
            .returns("outputStream", Event.class);

        final String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(30, getLineCount(resultPath));
    }

    /**
     * @see <a href="https://docs.wso2.com/display/CEP300/Joins">https://docs.wso2.com/display/CEP300/Joins</a>
     */
    @Test
    public void testMultipleUnboundedPojoStreamUnionAndJoinWithWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input1 = env.addSource(new RandomEventSource(5), "input1");
        DataStream<Event> input2 = env.addSource(new RandomEventSource(5), "input2");

        DataStream<? extends Map> output = SiddhiCEP
            .define("inputStream1", input1.keyBy("id"), "id", "name", "price", "timestamp")
            .union("inputStream2", input2.keyBy("id"), "id", "name", "price", "timestamp")
            .cql(
                "from inputStream1#window.length(5) as s1 "
                    + "join inputStream2#window.time(500) as s2 "
                    + "on s1.id == s2.id "
                    + "select s1.timestamp as t, s1.name as n, s1.price as p1, s2.price as p2 "
                    + "insert into JoinStream;"
            )
            .returnAsMap("JoinStream");

        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(5, getLineCount(resultPath));
    }

    /**
     * @see <a href="https://docs.wso2.com/display/CEP300/Joins">https://docs.wso2.com/display/CEP300/Patterns</a>
     */
    @Test
    public void testUnboundedPojoStreamSimplePatternMatch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> input1 = env.addSource(new RandomEventSource(50).closeDelay(1500), "input1");
        DataStream<Event> input2 = env.addSource(new RandomEventSource(50).closeDelay(1500), "input2");

        DataStream<Map<String, Object>> output = SiddhiCEP
            .define("inputStream1", input1.keyBy("name"), "id", "name", "price", "timestamp")
            .union("inputStream2", input2.keyBy("name"), "id", "name", "price", "timestamp")
            .cql(
                "from every s1 = inputStream1[id == 2] "
                    + " -> s2 = inputStream2[id == 3] "
                    + "select s1.id as id_1, s1.name as name_1, s2.id as id_2, s2.name as name_2 "
                    + "insert into outputStream"
            )
            .returnAsMap("outputStream");

        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(1, getLineCount(resultPath));
        compareResultsByLinesInMemory(
            "{id_1=2, id_2=3, name_1=test_event, name_2=test_event}", resultPath);
    }

    /**
     * @see <a href="https://docs.wso2.com/display/CEP300/Joins">https://docs.wso2.com/display/CEP300/Sequences</a>
     */
    @Test
    public void testUnboundedPojoStreamSimpleSequences() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input1 = env.addSource(new RandomEventSource(5).closeDelay(1500), "input1");
        DataStream<Map<String, Object>> output = SiddhiCEP
            .define("inputStream1", input1.keyBy("name"), "id", "name", "price", "timestamp")
            .union("inputStream2", input1.keyBy("name"), "id", "name", "price", "timestamp")
            .cql(
                "from every s1 = inputStream1[id == 2]+ , "
                    + "s2 = inputStream2[id == 3]? "
                    + "within 1000 second "
                    + "select s1[0].name as n1, s2.name as n2 "
                    + "insert into outputStream"
            )
            .returnAsMap("outputStream");

        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(1, getLineCount(resultPath));
    }

    private static int getLineCount(String resPath) throws IOException {
        List<String> result = new LinkedList<>();
        readAllResultLines(result, resPath);
        for (String line : result) {
            System.out.println(line);
        }
        return result.size();
    }

    @Test
    public void testCustomizeSiddhiFunctionExtension() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input = env.addSource(new RandomEventSource(5));

        SiddhiCEP cep = SiddhiCEP.getSiddhiEnvironment(env);
        cep.registerExtension("custom:plus", CustomPlusFunctionExtension.class);

        DataStream<Map<String, Object>> output = cep
            .from("inputStream", input, "id", "name", "price", "timestamp")
            .cql("from inputStream select timestamp, id, name, custom:plus(price,price) as doubled_price insert into  outputStream")
            .returnAsMap("outputStream");

        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(5, getLineCount(resultPath));
    }

    @Test
    public void testRegisterStreamAndExtensionWithSiddhiCEPEnvironment() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input1 = env.addSource(new RandomEventSource(5), "input1");
        DataStream<Event> input2 = env.addSource(new RandomEventSource(5), "input2");

        SiddhiCEP cep = SiddhiCEP.getSiddhiEnvironment(env);
        cep.registerExtension("custom:plus", CustomPlusFunctionExtension.class);

        cep.registerStream("inputStream1", input1.keyBy("id"), "id", "name", "price", "timestamp");
        cep.registerStream("inputStream2", input2.keyBy("id"), "id", "name", "price", "timestamp");

        DataStream<Tuple4<Long, String, Double, Double>> output = cep
            .from("inputStream1").union("inputStream2")
            .cql(
                "from inputStream1#window.length(5) as s1 "
                    + "join inputStream2#window.time(500) as s2 "
                    + "on s1.id == s2.id "
                    + "select s1.timestamp as t, s1.name as n, s1.price as p1, s2.price as p2 "
                    + "insert into JoinStream;"
            )
            .returns("JoinStream");

        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        assertEquals(5, getLineCount(resultPath));
    }

    @Test(expected = UndefinedStreamException.class)
    public void testTriggerUndefinedStreamException() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Event> input1 = env.addSource(new RandomEventSource(5), "input1");

        SiddhiCEP cep = SiddhiCEP.getSiddhiEnvironment(env);
        cep.registerStream("inputStream1", input1.keyBy("id"), "id", "name", "price", "timestamp");

        DataStream<Map<String, Object>> output = cep
            .from("inputStream1").union("inputStream2")
            .cql(
                "from inputStream1#window.length(5) as s1 "
                    + "join inputStream2#window.time(500) as s2 "
                    + "on s1.id == s2.id "
                    + "select s1.timestamp as t, s1.name as n, s1.price as p1, s2.price as p2 "
                    + "insert into JoinStream;"
            )
            .returnAsMap("JoinStream");

        String resultPath = tempFolder.newFile().toURI().toString();
        output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
    }

    @Test
    public void testDynamicalStreamSimplePatternMatch() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Event> input1 = env.addSource(new RandomEventSource(30).setName("event_stream_1"),
            "input1");
        DataStream<Event> input2 = env.addSource(new RandomEventSource(30).setName("event_stream_2"),
            "input2");
        DataStream<Event> input3 = env.addSource(new RandomEventSource(30).setName("event_stream_3"),
            "input3");
        DataStream<Event> input4 = env.addSource(new RandomEventSource(30).setName("event_stream_4"),
            "input4");

        DataStream<ControlEvent> controlStream = env.addSource(new SourceFunction<ControlEvent>() {
            @Override
            public void run(SourceContext<ControlEvent> sourceContext) throws Exception {
                String id1 = MetadataControlEvent.Builder.nextExecutionPlanId();

                sourceContext.collect(MetadataControlEvent.builder()
                    .addExecutionPlan(id1,
                        "from inputStream1 select timestamp, id, name, price insert into outputStream1;")
                    .build());

                String id2 = MetadataControlEvent.Builder.nextExecutionPlanId();
                sourceContext.collect(MetadataControlEvent.builder()
                    .addExecutionPlan(id2,
                        "from inputStream1 select id, timestamp, name, price group by id insert into outputStream2;")
                    .build());

                String id3 = MetadataControlEvent.Builder.nextExecutionPlanId();
                sourceContext.collect(MetadataControlEvent.builder()
                    .addExecutionPlan(id3,
                        "from inputStream1 select name, timestamp, id, price group by name insert into outputStream3;")
                    .build());

                String id4 = MetadataControlEvent.Builder.nextExecutionPlanId();
                sourceContext.collect(MetadataControlEvent.builder()
                    .addExecutionPlan(id4,
                        "from inputStream2 select timestamp, id, name, price group by name insert into outputStream4;")
                    .build());

                sourceContext.collect(OperationControlEvent.enableQuery(id1));
                sourceContext.collect(OperationControlEvent.enableQuery(id2));
                sourceContext.collect(OperationControlEvent.enableQuery(id3));
                sourceContext.collect(OperationControlEvent.enableQuery(id4));
            }

            @Override
            public void cancel() {

            }
        });

        SiddhiStream.ExecutionSiddhiStream builder = SiddhiCEP
            .define("inputStream1", input1, "id", "name", "price", "timestamp")
            .union("inputStream2", input2, "id", "name", "price", "timestamp")
            .union("inputStream3", input3, "id", "name", "price", "timestamp")
            .union("inputStream4", input4, "id", "name", "price", "timestamp")
            .cql(controlStream);

        String resultPath = tempFolder.newFile().toURI().toString();
        builder.returnAsRow(Arrays.asList("outputStream2", "outputStream3"))
            .writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
        env.execute();
        int lineCount = getLineCount(resultPath);
        assertTrue(lineCount > 0);
        assertTrue(lineCount <= 90);
    }
}
