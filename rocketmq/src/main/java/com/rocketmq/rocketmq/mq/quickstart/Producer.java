package com.rocketmq.rocketmq.mq.quickstart;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * author wyt 2019-12-06
 */
public class Producer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建消息生产者
        DefaultMQProducer defaultMQProducer=new DefaultMQProducer("demo_producer_group");//指定消息发送组
        //2.设置Nameser 的地址
        defaultMQProducer.setNamesrvAddr("localhost:9876");
        //3.开启defaultMQProducer
        defaultMQProducer.start();//此处有异常需要抛出
        //4.创建新消息
        //注意选择 导入这个包的：org.apache.rocketmq.common.message.Message;
        //public Message(String topic, String tags, String keys, byte[] body)


        //body:就是你需要发送的消息
        //RemotingHelper.DEFAULT_CHARSET 设置UTF_8的编码格式
        Message message =new Message(
                "Topic_Demo",//topic：主题
                "Tags_Demo",//tags：标签(主要用于消息过滤作用)
                "Keys_1",//消息的唯一值
                "hello!".getBytes(RemotingHelper.DEFAULT_CHARSET)); //body:就是你需要发送的消息

        //5.发送消息
        SendResult result = defaultMQProducer.send(message);

        System.out.println("消息发送结果    ："+result);
        //6.关闭消息发送对像

        defaultMQProducer.shutdown();


    }
}
