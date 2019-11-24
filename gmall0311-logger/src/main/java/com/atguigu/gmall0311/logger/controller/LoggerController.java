package com.atguigu.gmall0311.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


import com.atguigu.gmall0311.common.constant.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import oracle.jrockit.jfr.VMJFR;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;



@Slf4j
@RestController //Controller+responsebody
public class LoggerController {
    @Autowired
    KafkaTemplate<String,String> kafkaTemplate;

    private static final Logger logger= LoggerFactory.getLogger(LoggerController.class);

    @PostMapping("log")
    public String doLog(@RequestParam("logString") String logString ){

        // 0 补充时间戳
        JSONObject jsonObject = JSON.parseObject(logString);
        jsonObject.put("ts",System.currentTimeMillis());
        // 1 落盘 file
       String jsonString = jsonObject.toJSONString();
        logger.info(jsonObject.toJSONString());

        //3 发送kafka
        // 2 推送到kafka
        if( "startup".equals( jsonObject.getString("type"))){
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP,jsonString);
        }else{
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT,jsonString);
        }



        return "success";
    }

}
