package com.lentouqing.stream.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.lentouqing.stream.behavior.StreamStrategy;
import com.lentouqing.stream.metadata.KafkaStreamJob;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import java.util.List;
import java.util.Objects;

/**
 * 单个kafkaStream数据流操作
 */
public class KafkaStreamSingleJob {

    public void executeStreamJob(List<KafkaStreamJob> kafkaStreamJob, StreamsBuilder builder, Consumed<String, JsonNode> jsonConsumed) {
        if (Objects.isNull(kafkaStreamJob) || kafkaStreamJob.isEmpty()) {
            return;
        }
        kafkaStreamJob.forEach(streamJob -> {
            // 获取源数据流
            KStream<String, JsonNode> orderStream = buildKafkaStream(builder, jsonConsumed, streamJob);
            // 根据kafkaStreamJob中信息执行具体流处理任务
            streamJob.getStreamTask().forEach(streamTask -> {
                // 任务类型
                String behavior = streamTask.getBehavior();
                // 流处理任务的执行，获取到执行后的数据流
                KStream<String, JsonNode> processedStream = StreamStrategy.processDataFlow(behavior, orderStream, streamTask);
                // 将执行后的数据流写到对应的主题
                processedStream.to(streamTask.getTargetTopic());
            });
        });
    }

    private KStream<String, JsonNode> buildKafkaStream(StreamsBuilder builder, Consumed<String, JsonNode> jsonConsumed, KafkaStreamJob streamJob) {
        String sourceTopic = streamJob.getSourceTopic();
        return builder.stream(sourceTopic, jsonConsumed
                .withName("order-input"));
    }

}
