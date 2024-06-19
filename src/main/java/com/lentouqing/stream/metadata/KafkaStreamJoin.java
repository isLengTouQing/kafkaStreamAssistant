package com.lentouqing.stream.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * kafka合并数据流的类
 */
@AllArgsConstructor
@Data
public class KafkaStreamJoin {

    /**
     * 合并时左数据流来源topic
     */
    private String leftStreamSourceTopic;

    /**
     * 合并时左数据流key
     */
    private String leftKey;

    /**
     * 合并时左数据流的value
     */
    private String leftValue;

    /**
     * 合并时右数据流来源topic
     */
    private String rightStreamSourceTopic;

    /**
     * 合并时右数据流Key
     */
    private String rightKey;

    /**
     * 合并时右数据流的value
     */
    private String rightValue;

    /**
     * 合并两个数据流主干数据流的定义
     */
    private KeyValueSource outMainStream;

    /**
     * 合并后数据流的目标topic
     */
    private String targetTopic;

    /**
     * 合并后数据流
     */
    private List<KeyValueSchema> keyValueSchema;

}
