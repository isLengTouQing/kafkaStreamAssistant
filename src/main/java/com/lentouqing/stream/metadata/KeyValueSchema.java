package com.lentouqing.stream.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 合并后数据流的key-value信息
 */
@AllArgsConstructor
@Data
public class KeyValueSchema {
    /**
     * key name
     */
    private String key;

    /**
     * value name
     */
    private String value;

    /**
     * value 来源
     */
    private KeyValueSource valueSource;
}
