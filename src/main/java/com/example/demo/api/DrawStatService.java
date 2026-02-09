package com.example.demo.api;

import com.example.demo.common.constants.RedisConstants;
import com.example.demo.common.utils.KeyUtil;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class DrawStatService {

    @Resource(name = "businessUserRedissonClient")
    private RedissonClient businessUserRedissonClient;

    /**
     * 获取指定彩票的全部统计信息，包括：
     * 1. 第一球/第二球/第三球出现次数及概率
     * 2. 三球和值出现次数及概率
     *
     * @param lottery 彩票名称
     * @return Map 结构：
     * {
     *   "ball1": {"0": {"count": 123, "prob": 0.12}, ...},
     *   "ball2": {...},
     *   "ball3": {...},
     *   "sum": {"0": {"count": 0, "prob":0}, ..., "27": {...}}
     * }
     */
    public Map<String, Map<String, Map<String, Object>>> getStatistics(String lottery) {
        Map<String, Map<String, Map<String, Object>>> result = new HashMap<>();
        try {
            // 总开奖次数
            RAtomicLong totalAtomic = businessUserRedissonClient.getAtomicLong(
                    KeyUtil.genKey(RedisConstants.DRAW_RESULT_TOTAL_KEY_PREFIX, lottery)
            );
            long total = totalAtomic.get();
            if (total == 0) total = 1;

            // 球号统计
            result.put("ball1", buildBallStatMap(lottery, RedisConstants.DRAW_RESULT_BALL1_KEY_PREFIX, total, 0, 9));
            result.put("ball2", buildBallStatMap(lottery, RedisConstants.DRAW_RESULT_BALL2_KEY_PREFIX, total, 0, 9));
            result.put("ball3", buildBallStatMap(lottery, RedisConstants.DRAW_RESULT_BALL3_KEY_PREFIX, total, 0, 9));

            // 和值统计
            result.put("sum", buildBallStatMap(lottery, RedisConstants.DRAW_RESULT_SUM_KEY_PREFIX, total, 0, 27));

        } catch (Exception e) {
            log.error("获取开奖统计异常 lottery={}", lottery, e);
            // 返回空数据，避免抛出 BusinessException
            result.put("ball1", Collections.emptyMap());
            result.put("ball2", Collections.emptyMap());
            result.put("ball3", Collections.emptyMap());
            result.put("sum", Collections.emptyMap());
        }

        return result;
    }

    /** 构建球号/和值统计 Map */
    private Map<String, Map<String, Object>> buildBallStatMap(
            String lottery, String prefixKey, long total, int min, int max
    ) {
        Map<String, Map<String, Object>> map = new HashMap<>();
        try {
            RMap<String, Long> redisMap = businessUserRedissonClient.getMap(
                    KeyUtil.genKey(prefixKey, lottery)
            );

            for (int i = min; i <= max; i++) {
                String key = String.valueOf(i);
                Long count = 0L;
                try {
                    Object value = redisMap.get(key);
                    if (value != null) {
                        count = Long.parseLong(value.toString());
                    }
                } catch (Exception e) {
                    log.warn("解析 Redis 值失败 key={}, value={}", key, redisMap.get(key), e);
                }

                double probPercent = ((double) count / total) * 100;
                probPercent = Math.round(probPercent * 1000.0) / 1000.0; // 保留3位小数
                Map<String, Object> item = new HashMap<>();
                item.put("count", count);
                item.put("prob", probPercent);
                map.put(key, item);
            }

        } catch (Exception e) {
            log.error("Redis 统计读取失败 prefixKey={}, lottery={}", prefixKey, lottery, e);
        }
        return map;
    }

}
