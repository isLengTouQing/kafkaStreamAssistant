package com.lentouqing.stream.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * 数据清洗任务具体信息
 */
@AllArgsConstructor
@Data
public class StreamTask {
    /**
     * 数据清洗策略，当前只支持了Filter,后续会支持映射，聚合等
     */
    private String behavior ;

    /**
     * 数据清洗具体操作数据字段信息
     */
    private List<Operation> operation;

    /**
     * 数据清洗完毕后的目标topic
     */
    private String targetTopic ;
}
