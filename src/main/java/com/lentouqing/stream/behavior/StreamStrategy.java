package com.lentouqing.stream.behavior;

import com.fasterxml.jackson.databind.JsonNode;
import com.lentouqing.stream.metadata.StreamTask;
import org.apache.kafka.streams.kstream.KStream;

import java.util.HashMap;

/**
 * 当前kafkaStream支持的数据操作策略，包括过滤，映射，聚合，连接
 */
public class StreamStrategy {

    /**
     * 构建处理策略的Map, key:数据操作类型，value:处理操作的实现
     */
    public static HashMap<String, StreamOperation> streamOperationMap = new HashMap<>();

    static {
        // 过滤
        streamOperationMap.put("filter", new StreamFilter());
        // 映射
        streamOperationMap.put("map", new StreamMap());
    }

    /**
     * 根据处理策略，对数据流进行处理
     *
     * @param behavior 处理策略
     * @param sourceStream 数据流
     * @param streamTask 流任务
     */
    public static KStream<String, JsonNode> processDataFlow(String behavior, KStream<String, JsonNode> sourceStream, StreamTask streamTask) {
        return streamOperationMap.get(behavior).processDataFlow(sourceStream, streamTask);
    }
}
