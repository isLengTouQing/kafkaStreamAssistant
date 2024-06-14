package com.lentouqing.stream.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * kafkaStream元数据信息，包括kafka服务的环境信息，kafkaStream任务信息
 */
@AllArgsConstructor
@Data
public class KafkaMetadata {

    /**
     * kafka环境信息
     */
    private KafkaEnvironment kafkaEnvironment;

    /**
     * kafkaStream任务信息
     */
    private List<KafkaStreamJob> kafkaStreamJob;
}
