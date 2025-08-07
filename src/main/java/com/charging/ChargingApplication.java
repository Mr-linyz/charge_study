package com.charging;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
public class ChargingApplication {
    public static void main(String[] args) {
        SpringApplication.run(ChargingApplication.class, args);
        Main.demo();
        System.out.println("====== 充电订单与积分系统启动成功 ======");
    }
}
    