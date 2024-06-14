package com.lentouqing.stream.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.lentouqing.stream.behavior.StreamStrategy;
import com.lentouqing.stream.metadata.KafkaEnvironment;
import com.lentouqing.stream.metadata.KafkaMetadata;
import com.lentouqing.stream.metadata.KafkaStreamJob;
import com.lentouqing.stream.metadata.StreamTask;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import java.util.List;
import java.util.Properties;

/**
 * 服务核心逻辑处理类
 */
public class KafkaStreamAssistant {

    public static final String DEFAULT_KAFKA_META_NAME = "kafkaStreamMeta.json";

    public static final String KAFKA_META_NAME = "KafkaStreamMeta";
    /**
     * 用于区分jar包启动和本地调试
     */
    public static final String JAR = "jar";

    /**
     * 获取kafka服务的环境信息
     *
     * @param kafkaEnvironment 在配置文件中获取对应的kafka服务环境信息
     * @return kafka服务环境信息
     */
    public static Properties setKafkaConfig(KafkaEnvironment kafkaEnvironment) throws ClassNotFoundException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, kafkaEnvironment.getApplicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaEnvironment.getBootstrapServers());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Class.forName(kafkaEnvironment.getDefaultKeySerde()));
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Class.forName(kafkaEnvironment.getDefaultValueSerde()));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Class.forName(kafkaEnvironment.getConsumerKeyDeserializer()));
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Class.forName(kafkaEnvironment.getConsumerValueDeserializer()));
        return props;
    }

    /**
     * 构建这次Stream流数据清洗任务的方法
     *
     * @param kafkaMetadata 这次Stream流数据清洗的具体任务详情
     */
    public static StreamsBuilder getStreamsBuilder(KafkaMetadata kafkaMetadata) {
        StreamsBuilder builder = new StreamsBuilder();
        List<KafkaStreamJob> kafkaStreamJob = kafkaMetadata.getKafkaStreamJob();
        // 用于序列化和反序列化json数据
        Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        // 创建一个数据流消费者
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        Consumed<String, JsonNode> jsonConsumed = Consumed.with(Serdes.String(), jsonSerde)
                .withName("order-consumed")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST);
        // 进行具体地流处理操作任务
        for (KafkaStreamJob streamJob : kafkaStreamJob) {
            String sourceTopic = streamJob.getSourceTopic();
            KStream<String, JsonNode> orderStream = builder.stream(sourceTopic, jsonConsumed
                    .withName("order-input"));
            List<StreamTask> streamTask = streamJob.getStreamTask();
            for (StreamTask task : streamTask) {
                String behavior = task.getBehavior();
                // 流处理任务的执行，获取到执行后的数据流
                KStream<String, JsonNode> processedStream = StreamStrategy.streamOperationMap.get(behavior).processDataFlow(orderStream, task);
                // 将执行后的数据流写到对应的主题
                processedStream.to(task.getTargetTopic());
            }
        }
        return builder;
    }
}
