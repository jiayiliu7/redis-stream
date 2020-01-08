package com.redis.stream.Util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(2)
public class Consumer implements ApplicationRunner {
    protected  final static Logger logger = LoggerFactory.getLogger(Consumer.class);
    @Autowired
    private ConsumptionUtil consumption;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        logger.info("消费者队列启动");
        consumption.consumption("consumer_1");
        consumption.consumption("consumer_2");
    }








}
