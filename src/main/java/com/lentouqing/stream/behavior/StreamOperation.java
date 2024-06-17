package com.lentouqing.stream.behavior;

import com.fasterxml.jackson.databind.JsonNode;
import com.lentouqing.stream.metadata.StreamTask;
import org.apache.kafka.streams.kstream.KStream;

/**
 * 流处理操作接口
 */
public interface StreamOperation {

    /**
     * 处理数据流
     *
     * @param sourceStream 待处理的数据流
     * @param streamTask 包含对这个数据流具体处理任务的信息
     * @return  处理后的数据流
     */
    KStream<String, JsonNode> processDataFlow(KStream<String, JsonNode> sourceStream, StreamTask streamTask);
}
