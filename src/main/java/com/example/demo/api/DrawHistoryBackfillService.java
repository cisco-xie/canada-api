package com.example.demo.api;

import cn.hutool.json.JSONObject;
import com.example.demo.common.constants.RedisConstants;
import com.example.demo.common.utils.KeyUtil;
import com.example.demo.task.DrawHistorySaveTask;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

/**
 * 历史开奖补全：按日期范围从第三方 API 拉取并写入 Redis。
 */
@Slf4j
@Service
public class DrawHistoryBackfillService {

    private static final String[] SUPPORTED_LOTTERIES = {"JND28"};

    @Resource
    private DrawHistoryApi drawHistoryApi;

    @Resource
    private DrawHistorySaveTask drawHistorySaveTask;

    @Resource
    @Qualifier("businessUserRedissonClient")
    private RedissonClient businessUserRedissonClient;

    /**
     * 从 from 到 to（含）逐日拉取历史开奖并写入 Redis。
     *
     * @param from 起始日期
     * @param to   结束日期
     * @return 写入的期数总数
     */
    public int backfill(LocalDate from, LocalDate to) {
        if (from == null || to == null || from.isAfter(to)) {
            log.warn("开奖历史补全-参数无效 from={}, to={}", from, to);
            return 0;
        }
        log.info("开奖历史补全-开始 from={}, to={}", from, to);
        int totalSaved = 0;
        for (LocalDate date = from; !date.isAfter(to); date = date.plusDays(1)) {
            for (String lottery : SUPPORTED_LOTTERIES) {
                Integer lotCode = DrawHistoryApi.getLotCodeByLottery(lottery);
                if (lotCode == null) continue;
                try {
                    List<JSONObject> list = drawHistoryApi.fetchHistory(lotCode, date);
                    int saved = 0;
                    for (JSONObject item : list) {
                        JSONObject drawResult = DrawHistoryApi.toDrawResult(item);
                        if (drawResult == null) continue;
                        String preDrawIssue = item.getStr("preDrawIssue");
                        if (preDrawIssue == null || preDrawIssue.isEmpty()) continue;
                        // 永久保存 + 更新统计
                        drawHistorySaveTask.saveDrawResultAndStat(lottery, drawResult);
                        saved++;
                    }
                    totalSaved += saved;
                    if (saved > 0) {
                        log.info("开奖历史补全-日期 date={}, lottery={}, saved={}", date, lottery, saved);
                    }
                } catch (Exception e) {
                    log.error("开奖历史补全-异常 date={}, lottery={}", date, lottery, e);
                }
            }
        }
        log.info("开奖历史补全-结束 totalSaved={}", totalSaved);
        return totalSaved;
    }
}
