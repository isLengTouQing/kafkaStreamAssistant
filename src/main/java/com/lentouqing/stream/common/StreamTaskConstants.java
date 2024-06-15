package com.lentouqing.stream.common;

/**
 * 流处理任务的常量类
 */
public class StreamTaskConstants {

    /**
     * 字段类型double
     */
    public final static String TYPE_DOUBLE = "double";

    /**
     * 字段类型int
     */
    public final static String TYPE_INT = "int";

    /**
     * 字段类型long
     */
    public final static String TYPE_LONG = "long";

    /**
     * 字段类型String
     */
    public final static String STING_TYPE = "String";

    /**
     * 字段类型Boolean
     */
    public final static String BOOLEAN_TYPE = "Boolean";

    /**
     * 当前seatunnel同步数据到kafka时采用debezium json做序列化和反序列化，其中after关键字为表字段信息的key
     */
    public final static String DEBEZIUM_JSON_AFTER = "after";

    /**
     * 字符串相等判断
     */
    public final static String STRING_EQUALS = "equals";

    /**
     * 以指定字符串开头
     */
    public final static String STARTS_WITH = "startsWith";

    /**
     * 以指定字符串结尾
     */
    public final static String ENDS_WITH = "endsWith";

    /**
     * 正则匹配校验
     */
    public final static String MATCHES = "matches";
}
