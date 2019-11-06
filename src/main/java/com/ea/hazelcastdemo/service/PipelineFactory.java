package com.ea.hazelcastdemo.service;


import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientFlakeIdGeneratorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.BiConsumerEx;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.Map;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.concurrent.TimeUnit.MINUTES;

class PipelineFactory {

    private static ClientConfig clientConfig = new ClientConfig()
            .addFlakeIdGeneratorConfig(new ClientFlakeIdGeneratorConfig("idGenerator")
                    .setPrefetchCount(10)
                    .setPrefetchValidityMillis(MINUTES.toMillis(10)));
    private static HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
    private static FlakeIdGenerator idGenerator = client.getFlakeIdGenerator("idGenerator");

    static Pipeline getBatchPipeline1() {
        AggregateOperation1<Long, LongAccumulator, Long> summingAggregate = AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate((BiConsumerEx<LongAccumulator, Long>) LongAccumulator::add)
                .andExportFinish(LongAccumulator::get);

        Pipeline pipeline = Pipeline.create();
        BatchStage<Long> batchStage = pipeline.drawFrom(Sources.<Integer, Integer>map("integerIMap"))
                .filter(i -> i.getValue() < 50)
                .map(i -> new Long(i.getValue()))
                .aggregate(summingAggregate);
        batchStage.drainTo(Sinks.list("result1"));
        return pipeline;
    }

    static Pipeline getBatchPipeline2() {
        Pipeline pipeline = Pipeline.create();
        BatchStage<Long> batchStage = pipeline.drawFrom(Sources.<Integer, Integer>map("integerIMap"))
                .filter(i -> i.getValue() == 42)
                .aggregate(counting());
        batchStage.drainTo(Sinks.list("result2"));
        return pipeline;
    }

    static Pipeline getStreamPipeline() {
        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(Sources.<Integer, Integer>mapJournal("integerIMap", START_FROM_OLDEST))
                .withoutTimestamps()
                .filter(i -> i.getValue() == 42)
                .map(PipelineFactory::processMap)
                .drainTo(Sinks.map("result3"));
        return pipeline;
    }

    private static Map.Entry<Long, Integer> processMap(Map.Entry<Integer, Integer> i) {

        return new Map.Entry<Long, Integer>() {
            @Override
            public Long getKey() {
                return idGenerator.newId();
            }

            @Override
            public Integer getValue() {
                return i.getValue() * 2;
            }

            @Override
            public Integer setValue(Integer value) {
                return null;
            }
        };
    }
}
