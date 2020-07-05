package com.redis.stream.Util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Producer {
    private final static Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void addMessage(String content,String stream_name){
        RedisClient redisClient = RedisClient.create("redis://localhost:6379"); // change to reflect your environment
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> syncCommands = connection.sync();
        logger.info("生😋{}",content);
        //传递的消息必须是Map类型
        Map<String, String> messageBody = JSON.parseObject(content, new TypeReference<HashMap<String, String>>(){
        });

        String messageId = syncCommands.xadd(
                stream_name,
                messageBody);

        logger.info("条目ID为{}", messageId);

        connection.close();
        redisClient.shutdown();
    }
}
