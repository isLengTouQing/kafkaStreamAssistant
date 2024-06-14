package com.lentouqing.stream;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import java.util.Properties;

public class KafkaDemo {

    public static void main(String[] args) {
        // 设置应用的配置
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "rong-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "172.20.0.170:19092,172.20.0.170:29092,172.20.0.170:39092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, String.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, String.class);

        StreamsBuilder builder = new StreamsBuilder();

        // 定义输入流
        //KStream<String, String> sourceStream = builder.stream("mp.hk_erp2_sd.sd_pos_order");
        // 用于序列化和反序列化json数据
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        final Consumed<String, JsonNode> jsonConsumed = Consumed.with(Serdes.String(), jsonSerde)
                .withName("order-consumed")
                .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST);

        final KStream<String, JsonNode> orderStream = builder.stream("mp.hk_erp2_sd.sd_pos_order", jsonConsumed
                .withName("order-input"));

        // 构建过滤出payamount小于0的值
        KStream<String, JsonNode> filterStream = orderStream.filter(((key, value) -> {
            JsonNode after = value.get("after");
            double payamount = after.get("payamount").asDouble();
            return payamount < 0;
        }));

        // 将过滤后的数据写回到另一个主题
        filterStream.to("test-haitao");

        // 构建并启动流应用
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
