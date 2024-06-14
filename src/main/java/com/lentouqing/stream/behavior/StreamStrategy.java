package com.lentouqing.stream.behavior;

import java.util.HashMap;

/**
 * 当前kafkaStream支持的数据清洗操作策略，包括过滤，映射，聚合，连接
 */
public class StreamStrategy {

    /**
     * 构建清洗策略的Map, key:数据操作类型，value:清洗操作的实现
     */
    public static HashMap<String, StreamOperation> streamOperationMap = new HashMap<>();

    static {
        // 过滤
        streamOperationMap.put("filter", new StreamFilter());
        // 映射
        streamOperationMap.put("map", new StreamMap());
    }
}
