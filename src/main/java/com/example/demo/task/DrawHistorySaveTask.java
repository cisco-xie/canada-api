package com.example.demo.task;

import cn.hutool.json.JSONObject;
import com.example.demo.api.DrawHistoryApi;
import com.example.demo.common.constants.RedisConstants;
import com.example.demo.common.utils.KeyUtil;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;

/**
 * 定时从第三方开奖 API 拉取开奖结果并写入 Redis，与盘口登录解耦。
 * 写入的 key 与 winCheck 一致：draw:result:{lottery}:{drawNumber}，便于中奖校验与历史统计。
 * 不影响原有盘口自动投注逻辑：winCheck 仍优先读 Redis，缺失时再走 handicapApi.odds。
 * <p>
 * 加拿大28 开奖频率：每 3.5 分钟一期、每天 402 期（夏令时 20:00～次日19:30，冬令时 20:33～次日19:57:30，均为北京时间）。
 */
@Slf4j
@Component
public class DrawHistorySaveTask {

    @Resource
    private DrawHistoryApi drawHistoryApi;

    @Resource
    @Qualifier("businessUserRedissonClient")
    private RedissonClient businessUserRedissonClient;

    /** 当前支持的彩票（加拿大28），可扩展 */
    private static final String[] SUPPORTED_LOTTERIES = {"JND28"};

    /**
     * 定时拉取即时开奖（newopen），将上一期开奖结果写入 Redis，永久保存。
     * 每 2 分钟执行一次（开奖间隔 3.5 分钟，2 分钟内可覆盖每期新开奖）。
     */
    @Scheduled(fixedRate = 120_000)
    public void saveNewopenDrawResult() {
        for (String lottery : SUPPORTED_LOTTERIES) {
            Integer lotCode = DrawHistoryApi.getLotCodeByLottery(lottery);
            if (lotCode == null) continue;
            try {
                JSONObject data = drawHistoryApi.fetchNewopen(lotCode);
                if (data == null) continue;
                String preDrawIssue = data.getStr("preDrawIssue");
                String preDrawCode = data.getStr("preDrawCode");
                if (preDrawIssue == null || preDrawIssue.isEmpty() || preDrawCode == null || preDrawCode.isEmpty()) {
                    continue;
                }
                JSONObject drawResult = DrawHistoryApi.toDrawResult(data);
                if (drawResult == null) continue;
                // 永久保存 + 更新统计
                saveDrawResultAndStat(lottery, drawResult);
                log.debug("开奖历史-已保存 newopen 期数 lottery={}, drawNumber={}", lottery, preDrawIssue);
            } catch (Exception e) {
                log.error("开奖历史-保存 newopen 异常 lottery={}", lottery, e);
            }
        }
    }

    /**
     * 每日拉取当天历史开奖，补全可能遗漏的期数（如服务重启期间的开奖）。
     * 每天 23:59:50 执行，拉取当天数据。
     */
    @Scheduled(cron = "50 59 23 * * ?")
    public void saveTodayHistory() {
        LocalDate today = LocalDate.now();
        for (String lottery : SUPPORTED_LOTTERIES) {
            Integer lotCode = DrawHistoryApi.getLotCodeByLottery(lottery);
            if (lotCode == null) continue;
            try {
                List<JSONObject> list = drawHistoryApi.fetchHistory(lotCode, today);
                int saved = 0;
                for (JSONObject item : list) {
                    JSONObject drawResult = DrawHistoryApi.toDrawResult(item);
                    if (drawResult == null) continue;
                    String preDrawIssue = item.getStr("preDrawIssue");
                    if (preDrawIssue == null || preDrawIssue.isEmpty()) continue;
                    // 永久保存 + 更新统计
                    saveDrawResultAndStat(lottery, drawResult);
                    saved++;
                }
                if (saved > 0) {
                    log.info("开奖历史-已补全当日历史 lottery={}, date={}, saved={}", lottery, today, saved);
                }
            } catch (Exception e) {
                log.error("开奖历史-保存当日历史异常 lottery={}, date={}", lottery, today, e);
            }
        }
    }

    /**
     * ✅ 核心方法：永久保存开奖 + 更新统计
     * drawResult 中必须包含：
     * drawNumber、balls、preDrawTime
     */
    public void saveDrawResultAndStat(String lottery, JSONObject drawResult) {
        String drawNumber = drawResult.getStr("drawNumber");
        List<String> balls = drawResult.getJSONArray("balls").toList(String.class);
        String preDrawTime = drawResult.getStr("preDrawTime");

        if (balls == null || balls.size() != 3) {
            log.warn("开奖 balls 数据异常, drawNumber={}", drawNumber);
            return;
        }

        // 1️⃣ 检查这期是否已经保存
        String resultKey = KeyUtil.genKey(RedisConstants.DRAW_RESULT_PREFIX, lottery, drawNumber);
        RBucket<JSONObject> bucket = businessUserRedissonClient.getBucket(resultKey);
        if (bucket.isExists()) {
            log.debug("开奖已存在，不重复累加统计, drawNumber={}", drawNumber);
            return;
        }

        int b1 = Integer.parseInt(balls.get(0));
        int b2 = Integer.parseInt(balls.get(1));
        int b3 = Integer.parseInt(balls.get(2));
        int sum = b1 + b2 + b3;

        // 2️⃣ 永久保存
        bucket.set(drawResult);

        // 3️⃣ 更新统计
        RAtomicLong total = businessUserRedissonClient.getAtomicLong(KeyUtil.genKey(RedisConstants.DRAW_RESULT_TOTAL_KEY_PREFIX, lottery));
        total.incrementAndGet();

        RMap<String, Long> ball1Map = businessUserRedissonClient.getMap(KeyUtil.genKey(RedisConstants.DRAW_RESULT_BALL1_KEY_PREFIX, lottery));
        ball1Map.addAndGet(String.valueOf(b1), 1L);

        RMap<String, Long> ball2Map = businessUserRedissonClient.getMap(KeyUtil.genKey(RedisConstants.DRAW_RESULT_BALL2_KEY_PREFIX, lottery));
        ball2Map.addAndGet(String.valueOf(b2), 1L);

        RMap<String, Long> ball3Map = businessUserRedissonClient.getMap(KeyUtil.genKey(RedisConstants.DRAW_RESULT_BALL3_KEY_PREFIX, lottery));
        ball3Map.addAndGet(String.valueOf(b3), 1L);

        RMap<String, Long> sumMap = businessUserRedissonClient.getMap(KeyUtil.genKey(RedisConstants.DRAW_RESULT_SUM_KEY_PREFIX, lottery));
        sumMap.addAndGet(String.valueOf(sum), 1L);

        log.debug("开奖统计更新成功, drawNumber={}, balls={}", drawNumber, balls);
    }
}
