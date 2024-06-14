package com.lentouqing.stream.behavior;

import com.fasterxml.jackson.databind.JsonNode;
import com.lentouqing.stream.metadata.StreamTask;
import org.apache.kafka.streams.kstream.KStream;

/**
 * 数据流的映射操作
 */
public class StreamMap implements StreamOperation{
    @Override
    public KStream<String, JsonNode> processDataFlow(KStream<String, JsonNode> sourceStream, StreamTask streamTask) {
        return null;
    }
}
