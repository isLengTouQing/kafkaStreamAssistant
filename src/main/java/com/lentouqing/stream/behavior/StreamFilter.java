package com.lentouqing.stream.behavior;

import com.fasterxml.jackson.databind.JsonNode;
import com.lentouqing.stream.metadata.Operation;
import com.lentouqing.stream.metadata.StreamTask;
import org.apache.kafka.streams.kstream.KStream;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

/**
 * 数据流的过滤操作
 */
public class StreamFilter implements StreamOperation {
    private final static String TYPE_DOUBLE = "double";
    private final static String TYPE_INT = "int";
    private final static String TYPE_LONG = "long";

    /**
     * 当前支持的数字类型
     */
    private static final HashSet<String> NUMBER_SET = new HashSet<String>() {
        {
            add(TYPE_DOUBLE);
            add(TYPE_INT);
            add(TYPE_LONG);
        }
    };

    /**
     * 对数据流进行过滤操作
     *
     * @param sourceStream 待处理的数据流
     * @param streamTask 待处理的数据流
     */
    @Override
    public KStream<String, JsonNode> processDataFlow(KStream<String, JsonNode> sourceStream, StreamTask streamTask) {
        KStream<String, JsonNode> resultStream = null;
        List<Operation> operation = streamTask.getOperation();
        for (int i = 0; i < operation.size(); i++) {
            if (i == 0) {
                resultStream = filterStream(sourceStream, operation.get(i));
            } else {
                resultStream = filterStream(resultStream, operation.get(i));
            }
        }

        return resultStream;
    }

    /**
     * 对数据流进行过滤操作，返回处理后的数据流。
     *
     * @param sourceStream 待处理的数据流
     * @param operation 待处理的数据流
     */
    private KStream<String, JsonNode> filterStream(KStream<String, JsonNode> sourceStream, Operation operation) {
        // 拿到这次过滤操作具体字段，类型，任务类型等信息
        String type = operation.getType();
        String targetField = operation.getField();
        String operator = operation.getOperator();
        String comparisonValue = operation.getValue();

        // 进行具体的过滤操作，返回过滤后的数据流以支持可持续进行过滤操作
        return sourceStream.filter(((key, value) -> {
            JsonNode after = value.get("after");
            try {
                if (NUMBER_SET.contains(type)) {
                    // 根据json文件中的字段做数据过滤
                    Number numberFieldValue = parseNumberField(after.get(targetField), type);
                    Number tempValue = NumberFormat.getInstance(Locale.CHINA).parse(comparisonValue);
                    // 选择对应的比较类型
                    switch (operator) {
                        case "<":
                            return lessThanResult(numberFieldValue, tempValue);
                        case ">":
                            return greaterThanResult(numberFieldValue, tempValue);
                        case "=":
                            return equalsResult(numberFieldValue, tempValue);
                        default:
                            throw new RuntimeException("Unsupported operation");
                    }
                }
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            return false;
        }));
    }

    /**
     * 小于操作的比较结果
     *
     * @param numberFieldValue 比较的字段值
     * @param comparisonValue 目标值
     */
    private boolean lessThanResult(Number numberFieldValue, Number comparisonValue) {
        boolean result = false;
        if (numberFieldValue instanceof Double) {
            Double numberFieldRealValue = (Double) numberFieldValue;
            double comparisonRealValue = comparisonValue.doubleValue();
            return numberFieldRealValue < comparisonRealValue;
        } else if (numberFieldValue instanceof Long) {
            Long numberFieldRealValue = (Long) numberFieldValue;
            long comparisonRealValue = comparisonValue.longValue();
            return numberFieldRealValue < comparisonRealValue;
        } else if (numberFieldValue instanceof Integer) {
            Integer numberFieldRealValue = (Integer) numberFieldValue;
            int comparisonRealValue = comparisonValue.intValue();
            return numberFieldRealValue < comparisonRealValue;
        } else {
            return result;
        }
    }

    /**
     * 大于操作的比较结果
     *
     * @param numberFieldValue 比较的字段值
     * @param comparisonValue 目标值
     */
    private boolean greaterThanResult(Number numberFieldValue, Number comparisonValue) {
        boolean result = false;
        if (numberFieldValue instanceof Double) {
            Double numberFieldRealValue = (Double) numberFieldValue;
            double comparisonRealValue = comparisonValue.doubleValue();
            return numberFieldRealValue > comparisonRealValue;
        } else if (numberFieldValue instanceof Long) {
            Long numberFieldRealValue = (Long) numberFieldValue;
            long comparisonRealValue = comparisonValue.longValue();
            return numberFieldRealValue > comparisonRealValue;
        } else if (numberFieldValue instanceof Integer) {
            Integer numberFieldRealValue = (Integer) numberFieldValue;
            int comparisonRealValue = comparisonValue.intValue();
            return numberFieldRealValue > comparisonRealValue;
        } else {
            return result;
        }
    }

    /**
     * 等于操作的比较结果
     *
     * @param numberFieldValue 比较的字段值
     * @param comparisonValue  目标值
     */
    private boolean equalsResult(Number numberFieldValue, Number comparisonValue) {
        boolean result = false;
        if (numberFieldValue instanceof Double) {
            Double numberFieldRealValue = (Double) numberFieldValue;
            double comparisonRealValue = comparisonValue.doubleValue();
            return numberFieldRealValue.equals(comparisonRealValue);
        } else if (numberFieldValue instanceof Long) {
            Long numberFieldRealValue = (Long) numberFieldValue;
            long comparisonRealValue = comparisonValue.longValue();
            return numberFieldRealValue.equals(comparisonRealValue);
        } else if (numberFieldValue instanceof Integer) {
            Integer numberFieldRealValue = (Integer) numberFieldValue;
            int comparisonRealValue = comparisonValue.intValue();
            return numberFieldRealValue.equals(comparisonRealValue);
        } else {
            return result;
        }
    }

    /**
     * 解析处理的Number类型字段
     *
     * @param value 从流中解析出来的字段值
     * @param type  从流中解析出来的字段类型
     */
    private Number parseNumberField(JsonNode value, String type) {
        switch (type) {
            case TYPE_DOUBLE:
                return value.asDouble();
            case TYPE_INT:
                return value.asInt();
            case TYPE_LONG:
                return value.asLong();
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }
}
