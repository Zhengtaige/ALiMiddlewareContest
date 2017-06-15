package com.alibaba.middleware.race.sync;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.stream.ChunkedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;

/**
 * 处理client端的请求 Created by wanshao on 2017/5/25.
 */
public class ServerDemoInHandler extends ChannelInboundHandlerAdapter {

    private static final int mapLength = 1024 * 1024 * 1024; //1G
    private static Logger logger = LoggerFactory.getLogger(ServerDemoInHandler.class);
    int i = 0;
    File[] fileList;
    //    private Channel channel;
    private boolean inited = false;
    private MappedByteBuffer out;
    private int offset = 0; //读到第几个byte

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
//        channel = ctx.channel();


        logger.info("channelRead  - Server recieve message");
        ByteBuf result = (ByteBuf) msg;
        byte[] result1 = new byte[result.readableBytes()];
        // msg中存储的是ByteBuf类型的数据，把数据读取到byte[]中
        result.readBytes(result1);
        String resultStr = new String(result1);
        // 接收并打印客户端的信息
        logger.info("Client said:" + resultStr);

        //发送执行参数
//        channel.writeAndFlush(Unpooled.wrappedBuffer(Server.params.getBytes())).addListener(new ChannelFutureListener() {
//            @Override
//            public void operationComplete(ChannelFuture future) throws Exception {
//                logger.info("send params success: " + Server.params);
//            }
//        });
        GodVReader reader = GodVReader.getINSTANCE();
        while (!reader.done) {
            Thread.sleep(100);
        }

        if (!inited) {
            logger.info("start send file");
            RandomAccessFile randomAccessFile;
            try {
                randomAccessFile = new RandomAccessFile(Constants.MIDDLE_HOME + Constants.RESULT_FILE_NAME, "rw");
                ChunkedFile chunkedFile = new ChunkedFile(randomAccessFile);
                ctx.writeAndFlush(chunkedFile).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        future.channel().close();
                    }
                });
            } catch (Exception e) {
                logger.error(e.getMessage());
            }

            // 发送结束标志
//            channel.writeAndFlush(Unpooled.wrappedBuffer(new byte[]{-1})).addListener(new ChannelFutureListener() {
//                @Override
//                public void operationComplete(ChannelFuture future) throws Exception {
//                    logger.info("sent close signal");
//                }
//            });
            inited = true;
        }



//        int i = 0;
//        while (true) {
////            System.out.println(i);
//            // 向客户端发送消息
//            final byte[] message = getMessage();
//            if (message != null) {
////                Channel channel = Server.getMap().get("127.0.0.1"); //客户端在本地运行所以只取本地
//                ByteBuf byteBuf = Unpooled.wrappedBuffer(message);
//                channel.writeAndFlush(byteBuf).addListener(new ChannelFutureListener() {
//
//                    @Override
//                    public void operationComplete(ChannelFuture future) throws Exception {
//                        logger.info(String.format("sent %s bytes: " + new String(message), message.length));
//                    }
//                });
//            }
//
//            if (i++ > 500) {
//                ctx.close();
//                break;
//            }
//        }

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    private byte[] getMessage() throws InterruptedException {
        // 模拟下数据生成，每隔5秒产生一条消息
//        Thread.sleep(1000);
//
//        //比赛时在这里产生消息内容
//
//        return "message generated in ServerDemoInHandler. i:" + i++;


        return "".getBytes();

    }

    /*class MyRunnable implements Runnable {

        private int i;

        public MyRunnable(int i) {
            this.i = i;
        }

        @Override
        public void run() {
            FileReader.readOneFile(new File(Constants.DATA_HOME + "/" + i + ".txt"), Server.schemaName, Server.tableName, Server.startPkId, Server.endPkId);
//                Thread.sleep(100);
            channel.writeAndFlush(Unpooled.wrappedBuffer(String.format("file %s.txt read done!", i).getBytes())).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    logger.info("sent read success!");
                }
            });
        }
    }*/

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        logger.info("Server({}) channelActive.", ctx.channel().localAddress().toString());
    }
}
