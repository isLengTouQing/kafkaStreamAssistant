package com.lentouqing.stream.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 清洗的数据字段信息
 */
@AllArgsConstructor
@Data
public class Operation {
    /**
     * 数据字段类型
     */
    private String type;

    /**
     * 数据字段名
     */
    private String field;

    /**
     * 具体对该字段进行的操作
     */
    private String operator;

    /**
     * 比较值
     */
    private String value;
}
