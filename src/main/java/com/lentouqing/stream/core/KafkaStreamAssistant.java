package com.lentouqing.stream.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lentouqing.stream.KafkaStreamAssistantApplication;
import com.lentouqing.stream.behavior.StreamStrategy;
import com.lentouqing.stream.metadata.KafkaEnvironment;
import com.lentouqing.stream.metadata.KafkaMetadata;
import com.lentouqing.stream.metadata.KafkaStreamJob;
import com.lentouqing.stream.metadata.StreamTask;
import com.lentouqing.stream.util.FileUtils;
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

import java.io.IOException;
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
        Consumed<String, JsonNode> jsonConsumed = buildStringJsonNodeConsumed();
        // 进行具体地流处理操作任务
        for (KafkaStreamJob streamJob : kafkaStreamJob) {
            // 获取源数据流
            String sourceTopic = streamJob.getSourceTopic();
            KStream<String, JsonNode> orderStream = builder.stream(sourceTopic, jsonConsumed
                    .withName("order-input"));
            List<StreamTask> streamTask = streamJob.getStreamTask();
            // 根据kafkaStreamJob中信息执行具体流处理任务
            for (StreamTask task : streamTask) {
                // 任务类型
                String behavior = task.getBehavior();
                // 流处理任务的执行，获取到执行后的数据流
                KStream<String, JsonNode> processedStream = StreamStrategy.processDataFlow(behavior, orderStream, task);
                // 将执行后的数据流写到对应的主题
                processedStream.to(task.getTargetTopic());
            }
        }
        return builder;
    }

    /**
     * 构建kafkaStream流的序列化，反序列化，以及消费者配置
     */
    private static Consumed<String, JsonNode> buildStringJsonNodeConsumed() {
        // 用于序列化和反序列化json数据
        Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        // 创建一个数据流消费者
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        return Consumed.with(Serdes.String(), jsonSerde)
                .withName("order-consumed")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST);
    }

    /**
     * 读取服务启动时配置的kafkaStreamMeta文件，包括kafka服务信息，具体的kafkaStream流处理任务信息
     */
    public static KafkaMetadata getKafkaStreamMeta() throws IOException {
        // 获取此次Stream流任务的json源文件
        KafkaMetadata kafkaMetadata;
        try {
            String resourceFileContent;
            if (KafkaStreamAssistant.JAR.equals(KafkaStreamAssistantApplication.class.getProtectionDomain().getCodeSource().getLocation().getProtocol())) {
                resourceFileContent = FileUtils.readFileByPath(System.getProperty(KafkaStreamAssistant.KAFKA_META_NAME));
            } else {
                // 本地调试时启动的环境测试
                resourceFileContent = FileUtils.readResourceToString(KafkaStreamAssistant.DEFAULT_KAFKA_META_NAME);
            }
            kafkaMetadata = new ObjectMapper().readValue(resourceFileContent, KafkaMetadata.class);
        } catch (IOException e) {
            throw new IOException("Failed to retrieve kafkaStreamMetadata file, please check the path or file format. error: {} ", e);
        }
        return kafkaMetadata;
    }
}
