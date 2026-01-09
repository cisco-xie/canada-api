package com.example.demo.task;

import com.example.demo.api.HandicapApi;
import com.example.demo.api.PlanDoubleService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;


/**
 * @Description:
 * @Author: 谢诗宏
 * @Date: 2024年11月27日
 * @Version 1.0
 */
@Slf4j
@Component
public class AutoBetTask {

    private boolean isRunning = false;
    @Resource
    private HandicapApi handicapApi;
    @Resource
    private PlanDoubleService planDoubleService;

    // 上一次任务完成后再延迟 10 秒执行
    // @Scheduled(fixedDelay = 10000)
    @Scheduled(fixedRate = 10000)
    public void bet() {
        long startTime = System.currentTimeMillis();

        /*if (isRunning) {
            log.info("上一轮任务还在执行，跳过...");
            return;
        }*/
        isRunning = true;
        try {
            log.info("开始执行 自动下注...");
            handicapApi.autoBetCompletableFuture();
        } catch (Exception e) {
            log.error("自动下注 执行异常", e);
        } finally {
            long endTime = System.currentTimeMillis();
            long costTime = (endTime - startTime) / 1000;
            log.info("此轮下注任务执行花费{}s", costTime);
            isRunning = false;  // 任务结束后重置标志位
        }
    }

    /**
     * 翻倍计划自动投注
     */
    // @Scheduled(fixedDelay = 10000)
    @Scheduled(fixedRate = 10000)
    public void betDouble() {
        long startTime = System.currentTimeMillis();
        try {
            log.info("开始执行 计划自动下注...");
            planDoubleService.autoBetCompletableFuturePlanDouble();
        } catch (Exception e) {
            log.error("计划自动下注 执行异常", e);
        } finally {
            long endTime = System.currentTimeMillis();
            long costTime = (endTime - startTime) / 1000;
            log.info("此轮计划下注任务执行花费{}s", costTime);
        }
    }

    @Scheduled(cron = "0 30 * * * ?")
    public void autoRefreshSummaryToday() {
        long startTime = System.currentTimeMillis();
        try {
            log.info("开始执行 自动刷新账号额度...");
            // falaliApi.autoRefreshBalance();
        } catch (Exception e) {
            log.error("自动刷新账号额度 执行异常", e);
        } finally {
            long endTime = System.currentTimeMillis();
            long costTime = (endTime - startTime) / 1000;
            log.info("此轮自动刷新账号额度任务执行花费{}s", costTime);
            if (costTime > 20) {
                log.warn("自动刷新账号额度 执行时间过长");
            }
        }
    }
}
