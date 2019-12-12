package com.rocketmq.rocketmq.mq.order;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * author wyt 2019-12-10
 * 按一定顺序发消息
 */
public class OrderProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建消息生产者
        DefaultMQProducer defaultMQProducer=new DefaultMQProducer("demo_producer_group");//指定消息发送组
        //2.设置Nameser 的地址
        defaultMQProducer.setNamesrvAddr("localhost:9876");
        //3.开启defaultMQProducer
        defaultMQProducer.start();//此处有异常需要抛出


        //5.发送消息（顺序发消息 需要制定消息队列 不认无法保持顺序）
        //第一个参数是发送的消息信息
        //第二个参数是选中指定的消息队列对象（会传入所有的消息队列）
        //第三个参数是指定对应的消息队列的下标
        /**
         * 循环多次发送验证
         */
        for (int i = 0; i <6 ; i++) {
            //4.创建新消息
            //注意选择 导入这个包的：org.apache.rocketmq.common.message.Message;
            //public Message(String topic, String tags, String keys, byte[] body)


            //body:就是你需要发送的消息
            //RemotingHelper.DEFAULT_CHARSET 设置UTF_8的编码格式
            Message message =new Message(
                    "Topic_Order_Demo",//topic：主题
                    "Tags_Order_Demo",//tags：标签(主要用于消息过滤作用)
                    "Keys_Order_1",//消息的唯一值
                    ("hello_Order!"+i).getBytes(RemotingHelper.DEFAULT_CHARSET)); //body:就是你需要发送的消息
            SendResult result = defaultMQProducer.send(
                    message,
                    new MessageQueueSelector() {
                        @Override
                        public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                            //你想要的指定的队列的下标 就是下面的数值1
                            Integer index=(Integer) o;
                            //返回队列
                            return list.get(index);
                        }
                    },
                    1
            );

            System.out.println("Order有序消息发送结果    ："+result);
        }

        //6.关闭消息发送对像

        defaultMQProducer.shutdown();


    }
}
