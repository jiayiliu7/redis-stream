package com.redis.stream.Util;

import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

@Component
@Order(2)
public class Observer implements ApplicationRunner {
    @Override
    public void run(ApplicationArguments args) throws Exception {

    }
}
