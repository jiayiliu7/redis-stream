package com.redis.stream.Controller;

import com.alibaba.fastjson.JSONObject;
import com.redis.stream.Util.Producer;
import com.redis.stream.Util.ResponseData;
import com.redis.stream.Util.ResponseDataUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class StreamController {
    private final static Logger logger = LoggerFactory.getLogger(StreamController.class);
    @PostMapping("/xs")
    public ResponseData xs(@RequestBody String json){
        JSONObject jsonObject = JSONObject.parseObject(json);
        String content = jsonObject.getString("content");
        String stream_name = jsonObject.getString("stream_name");
        Producer.addMessage(content,stream_name);
        return ResponseDataUtil.buildSuccess();
    }

}
