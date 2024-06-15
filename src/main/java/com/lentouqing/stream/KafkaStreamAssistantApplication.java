package com.lentouqing.stream;

import com.lentouqing.stream.core.KafkaStreamAssistant;
import com.lentouqing.stream.metadata.KafkaMetadata;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import java.io.IOException;
import java.util.Properties;

/**
 * 项目启动类，通过kafkaStreamMeta.json文件中定义的任务，完成指定topic中的数据清洗，并转发到指定topic
 */
public class KafkaStreamAssistantApplication {

    /**
     * 应用入口
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // 从配置文件中获取到对应的kafka服务信息
        KafkaMetadata kafkaStreamMeta = KafkaStreamAssistant.getKafkaStreamMeta();

        // kafka的环境信息
        Properties kafkaProps = KafkaStreamAssistant.setKafkaConfig(kafkaStreamMeta.getKafkaEnvironment());

        // 构建stream流处理应用
        StreamsBuilder builder = KafkaStreamAssistant.getStreamsBuilder(kafkaStreamMeta);

        // 构建并启动流应用
        KafkaStreams streams = new KafkaStreams(builder.build(), kafkaProps);
        streams.start();

        // 关闭流
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
