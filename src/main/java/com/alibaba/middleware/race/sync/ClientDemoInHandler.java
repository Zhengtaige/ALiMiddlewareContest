package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wanshao on 2017/5/25.
 */
public class ClientDemoInHandler extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(ClientDemoInHandler.class);
    private boolean recievedParams = false;
    private String schema;
    private String table;
    private int start;
    private int end;

//    private int limit = 0;

    // 接收server端的消息，并打印出来
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//        logger.info("com.alibaba.middleware.race.sync.ClientDemoInHandler.channelRead");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        result.readBytes(result1);
        String resultString = new String(result1);
//        System.out.println();
//        if (!recievedParams) { //第一次发送的参数
//            String[] params = resultString.split(",");
//            schema = params[0];
//            table = params[1];
//            start = Integer.valueOf(params[2]);
//            end = Integer.valueOf(params[3]);
//            recievedParams = true;
//            logger.info("get params: {} {} {} {}", schema, table, start, end);
////            System.out.println(schema + table + start + end);
//        } else { //之后发送的操作
            //进行重放操作
            logger.info("Server said:" + resultString);

//        }

//        if (limit++ > 50) ctx.close(); //用于测试，收到消息大于xx次退出

        result.release();

        //发个消息回去
//        ctx.writeAndFlush("I have received your messages and wait for next messages");
    }

    // 连接成功后，向server发送消息
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("com.alibaba.middleware.race.sync.ClientDemoInHandler.channelActive");
        String msg = "I am prepared to receive messages";
        ByteBuf encoded = ctx.alloc().buffer(4 * msg.length());
        encoded.writeBytes(msg.getBytes());
        ctx.write(encoded);
        ctx.flush();
    }
}
