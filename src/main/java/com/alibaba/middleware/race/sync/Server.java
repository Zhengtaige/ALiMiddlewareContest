package com.alibaba.middleware.race.sync;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.bytes.ByteArrayEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 服务器类，负责push消息到client Created by wanshao on 2017/5/25.
 */
public class Server {

    // 接收评测程序的三个参数
    public static String params;
    public static String schemaName;
    public static String tableName;
    public static int startPkId;
    public static int endPkId;

    // 保存channel,保存key和value为   客户端ip，chanel
    private static Map<String, Channel> map = new ConcurrentHashMap<String, Channel>();
    private static Map tableNamePkMap;

    public static Map<String, Channel> getMap() {
        return map;
    }

    public static void setMap(Map<String, Channel> map) {
        Server.map = map;
    }

    public static void main(String[] args) throws InterruptedException {
        initProperties();
        printInput(args);
        final Logger logger = LoggerFactory.getLogger(Server.class);
        Server server = new Server();
//        for (int i = 0; i < 100; i++) { //防止后面的log被截断
        logger.info("Start Server.... {}", params);
//        }

        File file = new File(Constants.DATA_HOME);
        File[] fileList = file.listFiles();
        for (File f :
                fileList) {
            //输出目录下所有文件名和文件大小
            logger.info("name: {}, size: {} MB", f.getName(), f.length() / 1024. / 1024.);
        }

        //直接开始读文件
        new Thread(new Runnable() {
            @Override
            public void run() {
                logger.info("start FileReader");
                for (int j = 10; j > 0; j--) {
                    long start = System.currentTimeMillis();
                    FileReader.readOneFile(new File(Constants.DATA_HOME + "/" + j + ".txt"), schemaName, tableName, startPkId, endPkId);
                    logger.info("FileReader read file{}.txt cost", j, System.currentTimeMillis() - start);
                }

                logger.info("start GodVReader");
                GodVReader reader = GodVReader.getINSTANCE(
                        Server.startPkId,
                        Server.endPkId);
                for (int j = 10; j > 0; j--) {
                    reader.doRead(j + ".txt");
                }
                reader.getResult();
                reader.done = true;
            }
        }).start();

        server.startServer(5527);
    }

    /**
     * 打印赛题输入 赛题输入格式： schemaName tableName startPkId endPkId，例如输入： middleware student 100 200
     * 上面表示，查询的schema为middleware，查询的表为student,主键的查询范围是(100,200)，注意是开区间 对应DB的SQL为： select * from middleware.student where
     * id>100 and id<200
     */
    private static void printInput(String[] args) {
//        // 第一个参数是Schema Name
//        System.out.println("Schema:" + args[0]);
//        // 第二个参数是Schema Name
//        System.out.println("table:" + args[1]);
//        // 第三个参数是start pk Id
//        System.out.println("start:" + args[2]);
//        // 第四个参数是end pk Id
//        System.out.println("end:" + args[3]);
        schemaName = args[0];
        tableName = args[1];
        startPkId = Integer.parseInt(args[2]);
        endPkId = Integer.parseInt(args[3]);

        params = String.format("%s,%s,%s,%s", args);

    }

    /**
     * 初始化系统属性
     */
    private static void initProperties() {
        System.setProperty("middleware.test.home", Constants.TESTER_HOME);
        System.setProperty("middleware.teamcode", Constants.TEAMCODE);
        System.setProperty("app.logging.level", Constants.LOG_LEVEL);
    }


    private void startServer(int port) throws InterruptedException {

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {

                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        // 注册handler
                        ch.pipeline().addLast(new ByteArrayEncoder());
                        ch.pipeline().addLast(new ChunkedWriteHandler());
                        ch.pipeline().addLast(new ServerDemoInHandler());
                        // ch.pipeline().addLast(new ServerDemoOutHandler());
                    }
                })
//                    .childHandler(new ServerDemoInHandler())
                    .option(ChannelOption.SO_BACKLOG, 1024)
                .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }
}
