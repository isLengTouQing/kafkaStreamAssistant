package com.lentouqing.stream.metadata;

/**
 * 合并两条数据流时表明数据来源
 */
public enum KeyValueSource {
    /**
     * 表明数据来自左数据流
     */
    LEFT,
    /**
     * 表明数据来自右数据流
     */
    RIGHT,
    /**
     * 字符串，非通过数据流而来
     */
    STRING;
}
