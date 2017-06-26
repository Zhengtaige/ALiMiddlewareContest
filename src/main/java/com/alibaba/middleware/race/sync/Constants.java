package com.alibaba.middleware.race.sync;

/**
 * 外部赛示例代码需要的常量 Created by wanshao on 2017/5/25.
 */
interface Constants {

    // ------------ 本地测试可以使用自己的路径--------------//

    // 工作主目录
//    String TESTER_HOME = "/home/autulin/middlewareRace";
//    // 赛题数据
    String LOCAL_DATA_HOME = "E:\\__下载\\Data_Ali";
//    // 结果文件目录
//    String RESULT_HOME = "/home/autulin/middlewareRace/user_result";
////     中间结果目录
//    String MIDDLE_HOME = "/home/autulin/middlewareRace/middle";
    // teamCode
    String TEAMCODE = "7721890v3f";
    // 日志级别
    String LOG_LEVEL = "INFO";
    // server端口
    Integer SERVER_PORT = 5527;

    // ------------ 正式比赛指定的路径--------------//
    //// 工作主目录
    String TESTER_HOME = "/home/admin";
    // 赛题数据
//    String DATA_HOME = "/home/admin/canal_data";
    String DATA_HOME = "/Users/Nick_Zhengtaige/Desktop/1/Data";
    // 结果文件目录(client端会用到)
    String RESULT_HOME = "/home/admin/sync_results/7721890v3f/";
    // 中间结果目录（client和server都会用到）
//    String MIDDLE_HOME = "/home/admin/middle/7721890v3f/";
    String MIDDLE_HOME = "/Users/Nick_Zhengtaige/Desktop/1/";
    //结果文件的命名
    String RESULT_FILE_NAME = "ztgResult.rs";

}
