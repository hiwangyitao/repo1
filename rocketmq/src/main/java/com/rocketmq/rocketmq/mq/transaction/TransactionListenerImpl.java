package com.rocketmq.rocketmq.mq.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;

public class TransactionListenerImpl implements TransactionListener {

    private ConcurrentHashMap<String,Integer> localTransac=new ConcurrentHashMap<String,Integer>();

    /**
     * 执行本地事务
     * @param message
     * @param o
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        //回调
        String transactionId = message.getTransactionId();//获取 事务ID

        //设置事务状态 0:表示执行中 状态未知 1:表示本地事务执行成功  2:本地事务执行失败
        localTransac.put(transactionId,0);
        //业务执行,处理本地事务 service
        System.out.println("hello--执行本地事务");

        try {
            System.out.println("正在执行本地事务----------------");
            Thread.sleep(75000);
            System.out.println("本地事务执行成功----------------");
            localTransac.put(transactionId,1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            localTransac.put(transactionId,2);//本地事务执行失败
            return LocalTransactionState.ROLLBACK_MESSAGE;//进行回滚
        }
        return LocalTransactionState.COMMIT_MESSAGE;//本地事务执行完成 进行提交
    }

    /**
     * 消息回查  及回查消息的执行状态
     * @param messageExt
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {

        String transactionId = messageExt.getTransactionId();//获取 事务ID
        //获取对应事务ID的执行状态
        Integer statu=localTransac.get(transactionId);

        System.out.println("消息回查   ----- "+transactionId+"  ----  消息状态 -----  "+statu);
        //0:表示执行中 状态未知 1:表示本地事务执行成功  2:本地事务执行失败
        switch(statu){
            case 0:
                return LocalTransactionState.UNKNOW;
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 2:
                return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.UNKNOW;
    }
}
