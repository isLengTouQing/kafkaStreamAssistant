package com.lentouqing.stream.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * kafka服务环境信息
 */
@AllArgsConstructor
@Data
public class KafkaEnvironment {
    /**
     *  应用程序的唯一标识符，用于在Kafka中标识生产者或消费者
     */
    private String applicationId;

    /**
     * kafka服务地址
     */
    private String bootstrapServers;

    /**
     * 默认的键序列化类，用于将键序列化为字节序列
     */
    private String defaultKeySerde ;

    /**
     * 默认的值序列化类，用于将值序列化为字节序列
     */
    private String defaultValueSerde ;

    /**
     * 消费者使用的键反序列化类，默认为String类，用于将字节序列反序列化为键对象
     */
    private String consumerKeyDeserializer = String.class.getName();

    /**
     * 消费者使用的值反序列化类，默认为String类，用于将字节序列反序列化为值对象
     */
    private String consumerValueDeserializer = String.class.getName();
}
