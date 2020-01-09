package com.redis.stream.Util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;

@Async
@Component
public class ConsumptionUtil {
    private final String GROUP = "application_1";
    private final String REDIS_STREAM = "redis-stream";

    protected  final static Logger logger = LoggerFactory.getLogger(Consumer.class);

    @SuppressWarnings("InfiniteLoopStatement")
    public void consumption(String consumer){
        RedisClient redisClient = RedisClient.create("redis://localhost:6379"); // change to reflect your environment
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> syncCommands = connection.sync();
        try {
            syncCommands.xgroupCreate(XReadArgs.StreamOffset.from(REDIS_STREAM, "0-0"), GROUP, XGroupCreateArgs.Builder.mkstream(true));
        } catch (RedisBusyException redisBusyException) {
            logger.info(String.format("\t Group '%s already' exists", GROUP));
        }
        logger.info("Waiting for new messages....");

        while (true) {
            @SuppressWarnings("unchecked") List<StreamMessage<String, String>> messages = syncCommands.xreadgroup(
                    io.lettuce.core.Consumer.from(GROUP, consumer),
                    XReadArgs.StreamOffset.lastConsumed(REDIS_STREAM)
            );
            if (!messages.isEmpty()) {
                messages.forEach(a -> {
                            Future<String> result =  printMessage(a.getBody().toString());

                            if ("true".equals(result.toString())){
                                syncCommands.xack(REDIS_STREAM,GROUP,a.getId());
                            }
                        });
            }
        }
    }

    public Future<String> printMessage(String message){

        logger.info("现在时间是{}， 我获得消息为{}",System.currentTimeMillis(),message);
        message=message.replace("=",":");
        HashMap<String,String> map;
        map= JSON.parseObject(message,new TypeReference<HashMap<String,String>>(){});
        map.forEach((key, value) -> logger.info("这次key是{}，这次的value是{}。", key, value));
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("结束 "+message);
        return new AsyncResult<>("true");
    }

}
