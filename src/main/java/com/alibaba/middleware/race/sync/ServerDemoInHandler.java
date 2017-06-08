package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 处理client端的请求 Created by wanshao on 2017/5/25.
 */
public class ServerDemoInHandler extends ChannelInboundHandlerAdapter {

    private static Logger logger = LoggerFactory.getLogger(ServerDemoInHandler.class);
    int i = 0;
    private Channel channel;

    /**
     * 根据channel
     *
     * @param ctx
     * @return
     */
    public static String getIPString(ChannelHandlerContext ctx) {
        String ipString = "";
        String socketString = ctx.channel().remoteAddress().toString();
        int colonAt = socketString.indexOf(":");
        ipString = socketString.substring(1, colonAt);
        return ipString;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {

        // 保存channel
//        Server.getMap().put(getIPString(ctx), ctx.channel());
        channel = ctx.channel();

        logger.info("com.alibaba.middleware.race.sync.ServerDemoInHandler.channelRead");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
        result.readBytes(result1);
        String resultStr = new String(result1);
        // 接收并打印客户端的信息
        System.out.println("com.alibaba.middleware.race.sync.Client said:" + resultStr);

        channel.writeAndFlush(Unpooled.wrappedBuffer(Server.params.getBytes())).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                logger.info("send params success: " + Server.params);
            }
        });

        while (true) {
            // 向客户端发送消息
            final String message = (String) getMessage();
            if (message != null) {
//                Channel channel = Server.getMap().get("127.0.0.1"); //客户端在本地运行所以只取本地
                ByteBuf byteBuf = Unpooled.wrappedBuffer(message.getBytes());
                channel.writeAndFlush(byteBuf).addListener(new ChannelFutureListener() {

                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        logger.info("Server发送消息成功！(%s)", message);
                    }
                });
            }
        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private Object getMessage() throws InterruptedException {
        // 模拟下数据生成，每隔5秒产生一条消息
        Thread.sleep(5000);

        //比赛时在这里产生消息内容

        return "message generated in ServerDemoInHandler. i:" + i++;

    }
}
