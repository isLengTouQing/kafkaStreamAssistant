package com.lentouqing.stream.util;

import com.lentouqing.stream.KafkaStreamAssistantApplication;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 自用工具类
 */
public class FileUtils {
    /**
     * 读取资源文件
     *
     * @param fileName
     * @return
     * @throws IOException
     */
    public static String readResourceToString(String fileName) throws IOException {
        InputStream is = KafkaStreamAssistantApplication.class.getResourceAsStream("/" + fileName);
        if (is == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            return sb.toString();
        }
    }

    /**
     * 读取文件
     *
     * @param filePath
     * @return
     */
    public static String readFileByPath(String filePath) {
        try {
            return new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
