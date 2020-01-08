package com.redis.stream.Util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
@Async
@Component
public class ConsumptionUtil {

    protected  final static Logger logger = LoggerFactory.getLogger(Consumer.class);

    public void consumption(String consumer){
        RedisClient redisClient = RedisClient.create("redis://localhost:6379"); // change to reflect your environment
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> syncCommands = connection.sync();
        try {
            syncCommands.xgroupCreate(XReadArgs.StreamOffset.from("redis-stream", "0-0"), "application_1", XGroupCreateArgs.Builder.mkstream(true));
        } catch (RedisBusyException redisBusyException) {
            logger.info(String.format("\t Group '%s already' exists", "application_1"));
        }
        logger.info("Waiting for new messages....");
        while (true) {
            List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                    io.lettuce.core.Consumer.from("application_1", consumer),
                    XReadArgs.StreamOffset.lastConsumed("redis-stream")
            );
            if (!messages.isEmpty()) {
                messages.stream()
                        .forEach(a -> {
                            boolean result =  printMessage(a.getBody().toString());
                            if (result){
                                syncCommands.xack("redis-stream","application_1",a.getId());
                            }
                        });
            }
        }
    }

    public boolean printMessage(String message){

        logger.info("现在时间是{}， 我获得消息为{}",System.currentTimeMillis(),message);
        message=message.replace("=",":");
        HashMap<String,String> map = new HashMap();
        map= JSON.parseObject(message,new TypeReference<HashMap<String,String>>(){});
        map.entrySet()
                .stream()
                .forEach(a -> {
                    logger.info("这次key是{}，这次的value是{}。",a.getKey(),a.getValue());
                });
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("结束 "+message);
        return true;
    }
}
