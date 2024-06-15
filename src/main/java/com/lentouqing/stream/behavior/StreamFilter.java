package com.lentouqing.stream.behavior;

import com.fasterxml.jackson.databind.JsonNode;
import com.lentouqing.stream.common.StreamTaskConstants;
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

    /**
     * 当前支持的数字类型
     */
    private static final HashSet<String> NUMBER_SET = new HashSet<String>() {
        {
            add(StreamTaskConstants.TYPE_DOUBLE);
            add(StreamTaskConstants.TYPE_INT);
            add(StreamTaskConstants.TYPE_LONG);
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
            JsonNode after = value.get(StreamTaskConstants.DEBEZIUM_JSON_AFTER);
            try {
                // 对Number类型字段做处理
                if (NUMBER_SET.contains(type)) {
                    // 根据json文件中的字段做数据过滤
                    return numberFieldFilter(after, targetField, type, comparisonValue, operator);
                }
                // 对String类型字段做处理
                if (StreamTaskConstants.STING_TYPE.equals(type)) {
                    return streamFieldFilter(after, targetField, operator, comparisonValue);
                }
                // 对Boolean类型字段做处理,无需判断operator默认为equals
                if (type.equals(StreamTaskConstants.BOOLEAN_TYPE)) {
                    return booleanFieldFilter(after, targetField, comparisonValue);
                }

                // todo 对时间类型字段做处理
                if (type.equals("Date")) {

                }
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            return false;
        }));
    }

    /**
     * 对布尔类型字段做过滤操作，无需添加operator
     * @param after
     * @param targetField
     * @param comparisonValue
     */
    private static boolean booleanFieldFilter(JsonNode after, String targetField, String comparisonValue) {
        boolean targetValue = after.get(targetField).asBoolean();
        if (Boolean.valueOf(comparisonValue)) {
            return targetValue;
        } else {
            return !targetValue;
        }
    }

    /**
     * 对字符串类型字段做过滤操作
     *
     * @param after           当前采用DebeziumJson格式中含有after字段，该字段为DebeziumJson格式，包含字段值
     * @param targetField     字段名称
     * @param operator        具体操作
     * @param comparisonValue 比较值
     */
    private static boolean streamFieldFilter(JsonNode after, String targetField, String operator, String comparisonValue) {
        String targetValue = after.get(targetField).asText();
        switch (operator) {
            case StreamTaskConstants.STRING_EQUALS:
                return targetValue.equals(comparisonValue);
            case StreamTaskConstants.STARTS_WITH:
                return targetValue.startsWith(comparisonValue);
            case StreamTaskConstants.ENDS_WITH:
                return targetValue.endsWith(comparisonValue);
            case StreamTaskConstants.MATCHES:
                return targetValue.matches(comparisonValue);
            default:
                throw new RuntimeException("Unsupported operation");
        }
    }

    /**
     * 对数字类型字段做过滤操作
     *
     * @param after           当前采用DebeziumJson格式中含有after字段，该字段为DebeziumJson格式，包含字段值
     * @param targetField     字段名称
     * @param type            字段类型
     * @param comparisonValue 比较值
     * @param operator        具体操作
     */
    private boolean numberFieldFilter(JsonNode after, String targetField, String type, String comparisonValue, String operator) throws ParseException {
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
            case StreamTaskConstants.TYPE_DOUBLE:
                return value.asDouble();
            case StreamTaskConstants.TYPE_INT:
                return value.asInt();
            case StreamTaskConstants.TYPE_LONG:
                return value.asLong();
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }
}
