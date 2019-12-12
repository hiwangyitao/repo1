package com.rocketmq.rocketmq.mq.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class Consumer {


    public static void main(String[] args) throws MQClientException {
//        1.创建一个DefaultMQPushConsumer
        DefaultMQPushConsumer defaultMQPushConsumer=new DefaultMQPushConsumer("Demo_Consumer_Group");


        //        2.设置NameSerADD地址
        defaultMQPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        //        3.设置subscribe  ，这里要读取主题信息
        defaultMQPushConsumer.subscribe(
                "Topic_Demo",//指定要消费的消息主题
                "Tags_Demo"   //过滤规则
        );
        //        4.创建消息监听MessageListener
        // 设置消息拉去上限
        defaultMQPushConsumer.setConsumeMessageBatchMaxSize(2);
        defaultMQPushConsumer.setMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                //        5.获取消息信息
                //迭代消息信息
                for (MessageExt mes:list
                     ) {

                    try {
                        //获取主题
                        String Topic=mes.getTopic();
                        //获取标签
                        String tags=mes.getTags();
                        //获取消息
                        byte[] body=mes.getBody();
                        String message=new String(body, RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("Consumer消费信息---topic : "+Topic+"tags  :  "+tags+"   message:  "+message);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        //发生消费消息异常 进行从事机制
                        return   ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                }
                //        6.返回消息读取状态
                //说明完成消息消费
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
//      开启消息
        defaultMQPushConsumer.start();

    }
}
