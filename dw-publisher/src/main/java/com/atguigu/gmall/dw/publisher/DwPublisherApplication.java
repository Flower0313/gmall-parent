package com.atguigu.gmall.dw.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall.dw.publisher.mapper")
public class DwPublisherApplication {

    public static void main(String[] args) {
        //sprint boot的启动类
        SpringApplication.run(DwPublisherApplication.class, args);
    }

}
