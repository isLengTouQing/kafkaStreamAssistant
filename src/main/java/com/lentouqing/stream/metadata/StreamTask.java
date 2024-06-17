package com.lentouqing.stream.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * 数据处理任务具体信息
 */
@AllArgsConstructor
@Data
public class StreamTask {
    /**
     * 数据处理策略，当前只支持了Filter
     */
    private String behavior ;

    /**
     * 具体操作数据字段信息
     */
    private List<Operation> operation;

    /**
     * 数据处理完毕后的目标topic
     */
    private String targetTopic ;
}
