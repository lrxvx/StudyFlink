package com.luruixiao.kafka;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * @author luruixiao
 */
public class FileDemo {

    public static void main(String[] args) throws IOException {
        File log = new File("E:\\下载\\SogouQ.mini\\SogouQ.sample");
        List<String> logDatas = FileUtils.readLines(log, "gbk");
        for (String logData : logDatas) {
            System.out.println(logData);
        }
    }
}
