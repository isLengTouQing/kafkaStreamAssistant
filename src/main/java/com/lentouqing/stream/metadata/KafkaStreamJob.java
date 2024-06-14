package com.lentouqing.stream.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * kafkaStream任务信息
 */
@AllArgsConstructor
@Data
public class KafkaStreamJob {
    /**
     * 数据源Topic
     */
    private String sourceTopic;

    /**
     * 数据清洗任务
     */
    private List<StreamTask> streamTask;
}
