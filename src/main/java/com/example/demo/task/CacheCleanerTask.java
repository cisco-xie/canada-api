package com.example.demo.task;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Slf4j
@Component
public class CacheCleanerTask {

    // 每天凌晨6点10分执行清理缓存任务
    @Scheduled(cron = "0 10 6 * * ?")
    public void clearCache() {
        try {
            // 执行 shell 命令来清理缓存
            Process process = new ProcessBuilder("sudo", "bash", "-c", "echo 3 > /proc/sys/vm/drop_caches").start();
            process.waitFor(); // 等待命令执行完成
            log.info("系统缓存已清理");
        } catch (IOException | InterruptedException e) {
            log.error("系统缓存清理失败", e);
        }
    }

}
