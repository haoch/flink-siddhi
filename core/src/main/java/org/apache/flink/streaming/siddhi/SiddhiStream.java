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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.siddhi.control.ControlEvent;
import org.apache.flink.streaming.siddhi.operator.SiddhiOperatorContext;
import org.apache.flink.streaming.siddhi.router.AddRouteOperator;
import org.apache.flink.streaming.siddhi.router.DynamicPartitioner;
import org.apache.flink.streaming.siddhi.router.StreamRoute;
import org.apache.flink.streaming.siddhi.utils.GenericRecord;
import org.apache.flink.streaming.siddhi.utils.SiddhiStreamFactory;
import org.apache.flink.streaming.siddhi.utils.SiddhiTypeFactory;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;

import java.util.*;

/**
 * Siddhi CEP Stream API.
 */
@PublicEvolving
public abstract class SiddhiStream {
    private final SiddhiCEP cepEnvironment;

    /**
     * @param cepEnvironment SiddhiCEP cepEnvironment.
     */
    public SiddhiStream(SiddhiCEP cepEnvironment) {
        Preconditions.checkNotNull(cepEnvironment, "SiddhiCEP cepEnvironment is null");
        this.cepEnvironment = cepEnvironment;
    }

    /**
     * @return current SiddhiCEP cepEnvironment.
     */
    protected SiddhiCEP getCepEnvironment() {
        return this.cepEnvironment;
    }

    /**
     * @return Transform SiddhiStream to physical DataStream
     */
    protected abstract DataStream<Tuple2<StreamRoute, Object>> toDataStream();

    /**
     * Convert DataStream&lt;T&gt; to DataStream&lt;Tuple2&lt;String,T&gt;&gt;.
     * If it's KeyedStream. pass through original keySelector
     */
    protected <T> DataStream<Tuple2<StreamRoute, Object>> convertDataStream(DataStream<T> dataStream, String streamId) {
        final String streamIdInClosure = streamId;
        DataStream<Tuple2<StreamRoute, Object>> resultStream = dataStream.map(new MapFunction<T, Tuple2<StreamRoute, Object>>() {
            @Override
            public Tuple2<StreamRoute, Object> map(T value) throws Exception {
                return Tuple2.of(StreamRoute.of(streamIdInClosure), (Object) value);
            }
        });
        if (dataStream instanceof KeyedStream) {
            final KeySelector<T, Object> keySelector = ((KeyedStream<T, Object>) dataStream).getKeySelector();
            final KeySelector<Tuple2<StreamRoute, Object>, Object> keySelectorInClosure = new KeySelector<Tuple2<StreamRoute, Object>, Object>() {
                @Override
                public Object getKey(Tuple2<StreamRoute, Object> value) throws Exception {
                    return keySelector.getKey((T) value.f1);
                }
            };
            return resultStream.keyBy(keySelectorInClosure);
        } else {
            return resultStream;
        }
    }

    /**
     * ExecutableStream context to define execution logic, i.e. SiddhiCEP execution plan.
     */
    public abstract static class ExecutableStream extends SiddhiStream {
        public ExecutableStream(SiddhiCEP environment) {
            super(environment);
        }

        /**
         * Siddhi Continuous Query Language (CQL)
         *
         * @param executionPlan Siddhi SQL-Like execution plan query
         * @return ExecutionSiddhiStream context
         */
        public ExecutionSiddhiStream cql(String executionPlan) {
            Preconditions.checkNotNull(executionPlan, "executionPlan");
            return new ExecutionSiddhiStream(this.toDataStream(), executionPlan, getCepEnvironment());
        }

        /**
         * Siddhi Continuous Query Language (CQL)
         *
         * @return ExecutionSiddhiStream context
         */
        public ExecutionSiddhiStream cql(DataStream<ControlEvent> controlStream) {
            DataStream<Tuple2<StreamRoute, Object>> unionStream = controlStream
                .map(new NamedControlStream(ControlEvent.DEFAULT_INTERNAL_CONTROL_STREAM))
                .broadcast()
                .union(this.toDataStream())
                .transform("add route transform",
                    SiddhiTypeFactory.getStreamTupleTypeInformation(TypeInformation.of(Object.class)),
                    new AddRouteOperator(getCepEnvironment().getDataStreamSchemas()));

            DataStream<Tuple2<StreamRoute, Object>> partitionedStream = new DataStream<>(
                unionStream.getExecutionEnvironment(),
                new PartitionTransformation<>(unionStream.getTransformation(),
                new DynamicPartitioner()));
            return new ExecutionSiddhiStream(partitionedStream, null, getCepEnvironment());
        }

        private static class NamedControlStream
            implements MapFunction<ControlEvent, Tuple2<StreamRoute, Object>>, ResultTypeQueryable {
            private static final TypeInformation<Tuple2<StreamRoute, Object>> TYPE_INFO =
                TypeInformation.of(new TypeHint<Tuple2<StreamRoute, Object>>(){});
            private final String streamId;

            NamedControlStream(String streamId) {
                this.streamId = streamId;
            }

            @Override
            public Tuple2<StreamRoute, Object> map(ControlEvent value) throws Exception {
                return Tuple2.of(StreamRoute.of(this.streamId), value);
            }

            @Override
            public TypeInformation getProducedType() {
                return TYPE_INFO;
            }
        }
    }

    /**
     * Initial Single Siddhi Stream Context
     */
    public static class SingleSiddhiStream<T> extends ExecutableStream {
        private final String streamId;

        public SingleSiddhiStream(String streamId, SiddhiCEP environment) {
            super(environment);
            environment.checkStreamDefined(streamId);
            this.streamId = streamId;
        }


        /**
         * Define siddhi stream with streamId, source <code>DataStream</code> and stream schema and as the first stream of {@link UnionSiddhiStream}
         *
         * @param streamId   Unique siddhi streamId
         * @param dataStream DataStream to bind to the siddhi stream.
         * @param fieldNames Siddhi stream schema field names
         * @return {@link UnionSiddhiStream} context
         */
        public UnionSiddhiStream<T> union(String streamId, DataStream<T> dataStream, String... fieldNames) {
            getCepEnvironment().registerStream(streamId, dataStream, fieldNames);
            return union(streamId);
        }

        /**
         * @param streamIds Defined siddhi streamIds to union
         * @return {@link UnionSiddhiStream} context
         */
        public UnionSiddhiStream<T> union(String... streamIds) {
            Preconditions.checkNotNull(streamIds, "streamIds");
            return new UnionSiddhiStream<T>(this.streamId, Arrays.asList(streamIds), this.getCepEnvironment());
        }

        @Override
        protected DataStream<Tuple2<StreamRoute, Object>> toDataStream() {
            return convertDataStream(getCepEnvironment().getDataStream(this.streamId), this.streamId);
        }
    }

    public static class UnionSiddhiStream<T> extends ExecutableStream {
        private String firstStreamId;
        private List<String> unionStreamIds;

        public UnionSiddhiStream(String firstStreamId, List<String> unionStreamIds, SiddhiCEP environment) {
            super(environment);
            Preconditions.checkNotNull(firstStreamId, "firstStreamId");
            Preconditions.checkNotNull(unionStreamIds, "unionStreamIds");
            environment.checkStreamDefined(firstStreamId);
            for (String unionStreamId : unionStreamIds) {
                environment.checkStreamDefined(unionStreamId);
            }
            this.firstStreamId = firstStreamId;
            this.unionStreamIds = unionStreamIds;
        }

        /**
         * Define siddhi stream with streamId, source <code>DataStream</code> and stream schema and continue to union it with current stream.
         *
         * @param streamId   Unique siddhi streamId
         * @param dataStream DataStream to bind to the siddhi stream.
         * @param fieldNames Siddhi stream schema field names
         * @return {@link UnionSiddhiStream} context
         */
        public UnionSiddhiStream<T> union(String streamId, DataStream<T> dataStream, String... fieldNames) {
            Preconditions.checkNotNull(streamId, "streamId");
            Preconditions.checkNotNull(dataStream, "dataStream");
            Preconditions.checkNotNull(fieldNames, "fieldNames");
            getCepEnvironment().registerStream(streamId, dataStream, fieldNames);
            return union(streamId);
        }

        /**
         * @param streamId another defined streamId to union with.
         * @return {@link UnionSiddhiStream} context
         */
        public UnionSiddhiStream<T> union(String... streamId) {
            List<String> newUnionStreamIds = new LinkedList<>();
            newUnionStreamIds.addAll(unionStreamIds);
            newUnionStreamIds.addAll(Arrays.asList(streamId));
            return new UnionSiddhiStream<T>(this.firstStreamId, newUnionStreamIds, this.getCepEnvironment());
        }

        @Override
        protected DataStream<Tuple2<StreamRoute, Object>> toDataStream() {
            final String localFirstStreamId = firstStreamId;
            final List<String> localUnionStreamIds = this.unionStreamIds;
            DataStream<Tuple2<StreamRoute, Object>> dataStream = convertDataStream(getCepEnvironment().<T>getDataStream(localFirstStreamId), this.firstStreamId);
            for (String unionStreamId : localUnionStreamIds) {
                dataStream = dataStream.union(convertDataStream(getCepEnvironment().<T>getDataStream(unionStreamId), unionStreamId));
            }
            return dataStream;
        }
    }

    public static class ExecutionSiddhiStream {
        private final DataStream<Tuple2<StreamRoute, Object>> dataStream;

        private DataStream createdDataStream;

        private SiddhiOperatorContext siddhiContext;
        private String executionPlanId;

        public ExecutionSiddhiStream(DataStream<Tuple2<StreamRoute, Object>> dataStream, String executionPlan, SiddhiCEP environment) {
            this.dataStream = dataStream;
            siddhiContext = new SiddhiOperatorContext();
            if (executionPlan != null) {
                executionPlanId = siddhiContext.addExecutionPlan(executionPlan);
            }
            siddhiContext.setInputStreamSchemas(environment.getDataStreamSchemas());
            siddhiContext.setTimeCharacteristic(environment.getExecutionEnvironment().getStreamTimeCharacteristic());
            siddhiContext.setExtensions(environment.getExtensions());
            siddhiContext.setExecutionConfig(environment.getExecutionEnvironment().getConfig());
        }

        /**
         * @param outStreamId The <code>streamId</code> to return as data stream.
         * @param <T>         Type information should match with stream definition.
         *                    During execution phase, it will automatically build type information based on stream definition.
         * @return Return output stream as Tuple
         * @see SiddhiTypeFactory
         */
        public <T extends Tuple> DataStream<T> returns(String outStreamId) {
            TypeInformation<T> typeInformation =
                SiddhiTypeFactory.getTupleTypeInformation(siddhiContext.getAllEnrichedExecutionPlan(), outStreamId);
            return returns(Collections.singletonList(outStreamId)).map(value -> typeInformation.getTypeClass().cast(value.f1)).returns(typeInformation);
        }

        /**
         * @apiNote This function could not be used by dynamic partition, because policies are loaded dynamically
         * @param outStreamIds The <code>streamIds</code> to return as data stream.
         * @param <T>          Type information should match with stream definition.
         *                     During execution phase, it will automatically build type information based on stream definition.
         * @return Return output stream id and data as Tuple2
         * @see SiddhiTypeFactory
         */
        public <T extends Tuple> DataStream<Tuple2<String, T>> returns(List<String> outStreamIds) {
            for (String outStreamId : outStreamIds) {
                TypeInformation<T> typeInformation =
                    SiddhiTypeFactory.getTupleTypeInformation(siddhiContext.getAllEnrichedExecutionPlan(), outStreamId);
                siddhiContext.setOutputStreamType(outStreamId, typeInformation);
            }

            return returnsInternal(siddhiContext, executionPlanId);
        }

        public DataStream<org.apache.flink.types.Row> returnsTransformRow(String outStreamId){
            AbstractDefinition definition = SiddhiTypeFactory.getStreamDefinition(siddhiContext.getAllEnrichedExecutionPlan(), outStreamId, siddhiContext);
            List<TypeInformation> types = new ArrayList<>();
            for (Attribute attribute : definition.getAttributeList()) {
                types.add(TypeInformation.of(SiddhiTypeFactory.getJavaType(attribute.getType())));
            }
            TypeInformation<org.apache.flink.types.Row> typeInformation =
                    Types.ROW(types.toArray(new TypeInformation[0]));
            return returnAsRow(Collections.singletonList(outStreamId)).map(x -> typeInformation.getTypeClass().cast(x.f1)).returns(typeInformation);
        }

        /**
         * @return Return output stream as <code>DataStream&lt;Map&lt;String,Object&gt;&gt;</code>,
         * out type is <code>LinkedHashMap&lt;String,Object&gt;</code> and guarantee field order
         * as defined in siddhi execution plan
         * @see java.util.LinkedHashMap
         */
        public DataStream<Map<String, Object>> returnAsMap(String outStreamId) {
            return returnAsMap(Collections.singletonList(outStreamId)).map(new MapFunction<Tuple2<String, Map<String, Object>>, Map<String, Object>>() {
                @Override
                public Map<String, Object> map(Tuple2<String, Map<String, Object>> value) throws Exception {
                    return value.f1;
                }
            });
        }

        /**
         * @param outStreamIds The <code>streamId</code> to return as data stream.
         * @return Return output stream id and data(as map) as Tuple2
         */
        public DataStream<Tuple2<String, Map<String, Object>>> returnAsMap(List<String> outStreamIds) {
            for (String outStreamId : outStreamIds) {
                siddhiContext.setOutputStreamType(outStreamId, SiddhiTypeFactory.getMapTypeInformation());
            }

            return this.returnsInternal().map(new MapFunction<Tuple2<String, Object>, Tuple2<String, Map<String, Object>>>() {
                @Override
                public Tuple2<String, Map<String, Object>> map(Tuple2<String, Object> value) throws Exception {
                    return Tuple2.of(value.f0, ((GenericRecord)value.f1).getMap());
                }
            });
        }

        public DataStream<Row> returnAsRow(String outStreamId) {
            return returnAsRow(Collections.singletonList(outStreamId)).map(x -> x.f1);
        }

        /**
         * @param outStreamIds The <code>streamId</code> to return as data stream.
         * @return Return output stream id and {@link Row} as Tuple2
         */
        public DataStream<Tuple2<String, Row>> returnAsRow(List<String> outStreamIds) {
            for (String outStreamId : outStreamIds) {
                siddhiContext.setOutputStreamType(outStreamId, TypeExtractor.createTypeInfo(Row.class));
            }
            return this.returnsInternal();
        }

        /**
         * @param outStreamId OutStreamId
         * @param outType     Output type class
         * @param <T>         Output type
         * @return Return output stream as POJO class.
         */
        public <T> DataStream<T> returns(String outStreamId, Class<T> outType) {
            return returns(Collections.singletonList(outStreamId), outType).map(x -> x.f1);
        }

        /**
         * @param outStreamIds OutStreamId
         * @param outType     Output type class
         * @param <T>         Output type
         * @return Return output stream id and data(POJO class) as Tuple2
         */
        public <T> DataStream<Tuple2<String, T>> returns(List<String> outStreamIds, Class<T> outType) {
            for (String outStreamId : outStreamIds) {
                TypeInformation<T> typeInformation = TypeExtractor.getForClass(outType);
                siddhiContext.setOutputStreamType(outStreamId, typeInformation);
            }
            return returnsInternal();
        }

        /**
         * @param outStreamId OutStreamId
         * @param <T>         Output type
         * @return Return output stream as POJO class.
         */
        public <T> DataStream<T> returns(String outStreamId, TypeInformation<T> typeInformation) {
            return returns(Collections.singletonList(outStreamId), typeInformation).map(x -> x.f1);
        }

        /**
         * @param outStreamIds       OutStreamId
         * @param typeInformation   Output type class
         * @param <T>               Output type
         * @return Return output stream id and data(POJO class) as Tuple2
         */
        public <T> DataStream<Tuple2<String, T>> returns(List<String> outStreamIds, TypeInformation<T> typeInformation) {
            for (String outStreamId : outStreamIds) {
                siddhiContext.setOutputStreamType(outStreamId, typeInformation);
            }
            return returnsInternal();
        }

        @VisibleForTesting
        <T> DataStream<Tuple2<String, T>> returnsInternal() {
            return returnsInternal(siddhiContext, executionPlanId);
        }

        private <T> DataStream<Tuple2<String, T>> returnsInternal(SiddhiOperatorContext siddhiContext, String executionPlanId) {
            if (createdDataStream == null) {
                DataStream<Tuple2<StreamRoute, Object>> mapped = this.dataStream.map(new MapFunction<Tuple2<StreamRoute, Object>, Tuple2<StreamRoute, Object>>() {
                    @Override
                    public Tuple2<StreamRoute, Object> map(Tuple2<StreamRoute, Object> value) throws Exception {
                        if (executionPlanId != null) {
                            value.f0.addExecutionPlanId(executionPlanId);
                        }
                        return value;
                    }
                });
                createdDataStream = SiddhiStreamFactory.createDataStream(siddhiContext, mapped);
            }

            return createdDataStream;
        }
    }
}