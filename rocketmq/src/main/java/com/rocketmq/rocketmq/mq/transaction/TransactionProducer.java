package com.rocketmq.rocketmq.mq.transaction;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * 消息发送的事务管理
 * author wyt 2019-12-10
 */
public class TransactionProducer {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //1.创建消息生产者
//        DefaultMQProducer defaultMQProducer=new DefaultMQProducer("demo_producer_group");//指定消息发送组
        TransactionMQProducer producer=new TransactionMQProducer("demo_producer_transaction_group");//指定消息发送组
        //2.设置Nameser 的地址
        producer.setNamesrvAddr("localhost:9876");
        //指定消息监听对象，用于执行本地事务和消息回查
        TransactionListener transactionListener=new TransactionListenerImpl();
        //监听对象给生产者
        producer.setTransactionListener(transactionListener);

        //线程池
        ExecutorService  executorService=new ThreadPoolExecutor(
                2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(
                        2000),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread=new Thread(r);
                        thread.setName("线程池事务处理");
                        return null;
                    }
                }


        );
        //把线程池给生产者
        producer.setExecutorService(executorService);
        //3.开启defaultMQProducer
        producer.start();//此处有异常需要抛出
        //4.创建新消息
        //注意选择 导入这个包的：org.apache.rocketmq.common.message.Message;
        //public Message(String topic, String tags, String keys, byte[] body)


        //body:就是你需要发送的消息
        //RemotingHelper.DEFAULT_CHARSET 设置UTF_8的编码格式
        Message message =new Message(
                "Topic_Demo_Transaction",//topic：主题
                "Tags_Demo_Transaction",//tags：标签(主要用于消息过滤作用)
                "Keys_1",//消息的唯一值
                "hello!-Transaction".getBytes(RemotingHelper.DEFAULT_CHARSET)); //body:就是你需要发送的消息

        //5.发送事务消息
        TransactionSendResult transactionSendResult = producer.sendMessageInTransaction(message,"hello_transaction");


        System.out.println("消息发送结果    ："+transactionSendResult);
        //6.关闭消息发送对像

        producer.shutdown();


    }
}
