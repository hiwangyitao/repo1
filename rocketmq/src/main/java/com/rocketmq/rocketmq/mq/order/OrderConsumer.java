package com.rocketmq.rocketmq.mq.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;
/**
 * author wyt 2019-12-10
 * 按一定顺序接受消息
 */
public class OrderConsumer {


    public static void main(String[] args) throws MQClientException {
//        1.创建一个DefaultMQPushConsumer
        DefaultMQPushConsumer defaultMQPushConsumer=new DefaultMQPushConsumer("demo_producer_group");


        //        2.设置NameSerADD地址
        defaultMQPushConsumer.setNamesrvAddr("localhost:9876");
        //        3.设置subscribe  ，这里要读取主题信息
        defaultMQPushConsumer.subscribe(
                "Topic_Order_Demo",//指定要消费的消息主题
                "Tags_Order_Demo"   //过滤规则
        );
        //        4.创建消息监听MessageListener
        // 设置消息拉去上限
        defaultMQPushConsumer.setConsumeMessageBatchMaxSize(10);
        defaultMQPushConsumer.setMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                {
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
                            System.out.println("Order_Consumer 有序消费信息---topic : "+Topic+"tags  :  "+tags+"   message:  "+message);
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                            //发生消费消息异常 进行从事机制
                            return   ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                        }
                    }
                    //        6.返回消息读取状态
                    //说明完成消息消费
                    return ConsumeOrderlyStatus.SUCCESS;
                }
            }
        });
//      开启消息
        defaultMQPushConsumer.start();

    }
}
