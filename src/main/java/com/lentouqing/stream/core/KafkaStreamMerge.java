package com.lentouqing.stream.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lentouqing.stream.metadata.KafkaStreamJoin;
import com.lentouqing.stream.metadata.KeyValueSchema;
import com.lentouqing.stream.metadata.KeyValueSource;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.List;

/**
 * 两个kafkaStream流做合并输出
 */
public class KafkaStreamMerge {

    private KStream<String, String> leftStreamKeyValue;

    private KStream<String, String> rightStreamKeyValue;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private void init(KafkaStreamJoin kafkaStreamJoin, StreamsBuilder builder, Consumed<String, JsonNode> jsonConsumed) {
        String leftStreamSourceTopic = kafkaStreamJoin.getLeftStreamSourceTopic();
        String rightStreamSourceTopic = kafkaStreamJoin.getRightStreamSourceTopic();
        KStream<String, JsonNode> orderLeftStream = builder.stream(leftStreamSourceTopic, jsonConsumed.withName("order-left"));
        KStream<String, JsonNode> orderRightStream = builder.stream(rightStreamSourceTopic, jsonConsumed.withName("order-right"));
        // 构建这两个流中的数据key-value信息
        this.leftStreamKeyValue = orderLeftStream.map((k, v) ->
                KeyValue.pair(v.get("after").get(kafkaStreamJoin.getLeftKey()).asText(), v.get("after").toString()),
                Named.as("order-ext-transform"));
        this.rightStreamKeyValue = orderRightStream.map((k, v) ->
                KeyValue.pair(v.get("after").get(kafkaStreamJoin.getRightKey()).asText(), v.get("after").toString()),
                Named.as("order-transform"));
    }

    public void executeMergeStream(KafkaStreamJoin kafkaStreamJoin, StreamsBuilder builder, Consumed<String, JsonNode> jsonConsumed) {
        init(kafkaStreamJoin, builder, jsonConsumed);
        // 构建合并后输出数据流的key-value信息
        List<KeyValueSchema> keyValueSchemaList = kafkaStreamJoin.getKeyValueSchema();
        KeyValueSource outMainStream = kafkaStreamJoin.getOutMainStream();
        KStream<String, String> outKStream = leftStreamKeyValue.join(rightStreamKeyValue, (leftValue, rightValue) -> {
            try {
                ObjectNode orderLeft = (ObjectNode) objectMapper.readTree(leftValue);
                ObjectNode orderRight = (ObjectNode) objectMapper.readTree(rightValue);

                for (KeyValueSchema keyValueSchema : keyValueSchemaList) {
                    // 以右数据流为主干数据流
                    if (outMainStream == KeyValueSource.RIGHT) {
                        String key = keyValueSchema.getKey();
                        String value = keyValueSchema.getValue();
                        KeyValueSource valueSource = keyValueSchema.getValueSource();
                        orderRight.put(key, buildKeyOrValue(valueSource, value, orderLeft, orderRight));
                    }

                    // 以左数据流为主干数据流
                    if (outMainStream == KeyValueSource.LEFT) {
                        String key = keyValueSchema.getKey();
                        String value = keyValueSchema.getValue();
                        KeyValueSource valueSource = keyValueSchema.getValueSource();
                        orderLeft.put(key, buildKeyOrValue(valueSource, value, orderLeft, orderRight));
                    }
                }
                orderRight.put("hello","world");
                return outMainStream == KeyValueSource.RIGHT ? orderRight : orderLeft;
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }, JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1))).mapValues((k, v) -> v.toString());
        // 输出的目标topic
        outKStream.to(kafkaStreamJoin.getTargetTopic(), Produced.with(Serdes.String(), Serdes.String()));
    }

    private static JsonNode buildKeyOrValue(KeyValueSource keyValueSource,
                                            String value, ObjectNode orderLeft, ObjectNode orderRight) {
        if (keyValueSource == KeyValueSource.LEFT) {
            return orderLeft.get(value);
        }

        if (keyValueSource == KeyValueSource.RIGHT) {
            return orderRight.get(value);
        }
        return orderLeft.get(value);
    }
}
