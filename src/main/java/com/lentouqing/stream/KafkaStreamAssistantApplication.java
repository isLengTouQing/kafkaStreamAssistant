package com.lentouqing.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lentouqing.stream.core.KafkaStreamAssistant;
import com.lentouqing.stream.metadata.KafkaMetadata;
import com.lentouqing.stream.util.FileUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import java.io.IOException;
import java.util.Properties;

/**
 * 项目启动类，通过kafkaStreamMeta.json文件中定义的任务，完成指定topic中的数据清洗，并转发到另一个topic
 */
public class KafkaStreamAssistantApplication {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // 从配置文件中获取到对应的kafka服务信息
        KafkaMetadata kafkaStreamMeta = getKafkaStreamMeta();

        // kafka的环境信息
        Properties kafkaProps = KafkaStreamAssistant.setKafkaConfig(kafkaStreamMeta.getKafkaEnvironment());

        // 构建stream流处理应用
        StreamsBuilder builder = KafkaStreamAssistant.getStreamsBuilder(kafkaStreamMeta);

        // 构建并启动流应用
        KafkaStreams streams = new KafkaStreams(builder.build(), kafkaProps);
        streams.start();

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /**
     * 读取服务启动时配置的kafkaStreamMeta文件，包括kafka服务信息，具体的kafkaStream流处理任务信息
     */
    private static KafkaMetadata getKafkaStreamMeta() throws IOException {
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
