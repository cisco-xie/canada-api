package com.example.demo.api;

import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.example.demo.config.OkHttpProxyDispatcher;
import com.example.demo.core.model.UserConfig;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 第三方开奖历史 API（无需登录即可获取开奖数据）。
 * 用于实时保存开奖信息，与盘口 handicapApi.odds 解耦，避免因盘口掉线导致无法获取历史开奖。
 * <p>
 * 历史接口: GET /sscInfo/index/history/{lotCode}/{date}
 * 即时开奖: GET /sscInfo/index/newopen/{lotCode}
 * <p>
 * <b>加拿大28 开奖频率（北京时间）：</b>
 * <ul>
 *   <li>夏令时：每天 20:00 至次日 19:30</li>
 *   <li>冬令时：每天 20:33:00 至次日 19:57:30</li>
 *   <li>每 3.5 分钟一期，每天 402 期</li>
 * </ul>
 */
@Slf4j
@Component
public class DrawHistoryApi {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** 每期间隔（分钟） */
    public static final double DRAW_INTERVAL_MINUTES = 3.5;
    /** 每天期数 */
    public static final int DRAWS_PER_DAY = 402;

    @Value("${draw.history.api.base-url:https://ctlop8966.bw6571.com}")
    private String baseUrl;

    @Resource
    private OkHttpProxyDispatcher dispatcher;

    /** 第三方开奖 API 无需登录与代理，使用无代理配置走统一 dispatcher */
    private final UserConfig noProxyConfig = new UserConfig();

    /** 加拿大28 在第三方 API 中的 lotCode */
    public static final int LOTCODE_JND28 = 61;

    /** 本系统彩票代码与第三方 lotCode 映射（仅支持加拿大28 时可扩展） */
    private static final Map<String, Integer> LOTTERY_TO_LOTCODE = new HashMap<>();

    static {
        LOTTERY_TO_LOTCODE.put("JND28", LOTCODE_JND28);
    }

    /**
     * 获取指定日期的历史开奖列表
     *
     * @param lotCode 第三方彩种编码，如 61 表示加拿大28
     * @param date    日期 yyyy-MM-dd
     * @return 每条包含 preDrawIssue, preDrawCode, preDrawTime 等，与内部格式无关，由调用方转换
     */
    public List<JSONObject> fetchHistory(int lotCode, LocalDate date) {
        String url = baseUrl + "/sscInfo/index/history/" + lotCode + "/" + date.format(DATE_FMT);
        try {
            OkHttpProxyDispatcher.HttpResult result = dispatcher.execute("GET", url, null, buildHeaders(), noProxyConfig, false);
            int status = result.getStatus();
            String body = result.getBody();
            if (status < 200 || status >= 300) {
                log.warn("开奖历史API请求失败 lotCode={}, date={}, status={}", lotCode, date, status);
                return new ArrayList<>();
            }
            if (StringUtils.isBlank(body)) {
                return new ArrayList<>();
            }
            JSONObject root = JSONUtil.parseObj(body);
            if (root.getInt("errorCode", -1) != 0) {
                log.warn("开奖历史API返回错误 lotCode={}, date={}, body={}", lotCode, date, body);
                return new ArrayList<>();
            }
            JSONObject resultJson = root.getJSONObject("result");
            if (resultJson == null || resultJson.getInt("businessCode", -1) != 0) {
                return new ArrayList<>();
            }
            JSONArray data = resultJson.getJSONArray("data");
            if (data == null || data.isEmpty()) {
                return new ArrayList<>();
            }
            List<JSONObject> list = new ArrayList<>();
            for (int i = 0; i < data.size(); i++) {
                list.add(data.getJSONObject(i));
            }
            return list;
        } catch (IOException e) {
            log.error("开奖历史API异常 lotCode={}, date={}", lotCode, date, e);
            return new ArrayList<>();
        }
    }

    /**
     * 获取即时开奖信息（当前期 + 上一期已开奖结果）
     *
     * @param lotCode 第三方彩种编码
     * @return 含 preDrawIssue, preDrawCode, preDrawTime, drawIssue, drawTime 等；无数据时返回 null
     */
    public JSONObject fetchNewopen(int lotCode) {
        String url = baseUrl + "/sscInfo/index/newopen/" + lotCode;
        try {
            OkHttpProxyDispatcher.HttpResult result = dispatcher.execute("GET", url, null, buildHeaders(), noProxyConfig, false);
            int status = result.getStatus();
            String body = result.getBody();
            if (status < 200 || status >= 300) {
                log.warn("即时开奖API请求失败 lotCode={}, status={}", lotCode, status);
                return null;
            }
            if (StringUtils.isBlank(body)) {
                return null;
            }
            JSONObject root = JSONUtil.parseObj(body);
            if (root.getInt("errorCode", -1) != 0) {
                log.warn("即时开奖API返回错误 lotCode={}, body={}", lotCode, body);
                return null;
            }
            JSONObject resultJson = root.getJSONObject("result");
            if (resultJson == null || resultJson.getInt("businessCode", -1) != 0) {
                return null;
            }
            return resultJson.getJSONObject("data");
        } catch (IOException e) {
            log.error("即时开奖API异常 lotCode={}", lotCode, e);
            return null;
        }
    }

    /**
     * 将第三方单条开奖数据转换为与 winCheck 一致的 Redis 存储格式。
     * 格式: gid, bnum, drawNumber, balls（与 handicapApi.odds 写入的 drawResult 一致）
     */
    public static JSONObject toDrawResult(JSONObject item) {
        if (item == null) return null;
        String preDrawIssue = item.getStr("preDrawIssue");
        String preDrawCode = item.getStr("preDrawCode");
        if (StringUtils.isBlank(preDrawIssue) || StringUtils.isBlank(preDrawCode)) {
            return null;
        }
        JSONObject drawResult = new JSONObject();
        drawResult.set("gid", preDrawIssue);
        drawResult.set("bnum", preDrawCode);
        drawResult.set("drawNumber", preDrawIssue);
        String[] parts = preDrawCode.split(",");
        JSONArray balls = new JSONArray();
        for (String p : parts) {
            balls.add(p.trim());
        }
        drawResult.set("balls", balls);
        if (item.containsKey("preDrawTime")) {
            drawResult.set("preDrawTime", item.getStr("preDrawTime"));
        }
        return drawResult;
    }

    public static Integer getLotCodeByLottery(String lottery) {
        return LOTTERY_TO_LOTCODE.get(lottery);
    }

    public static boolean isSupportedLottery(String lottery) {
        return LOTTERY_TO_LOTCODE.containsKey(lottery);
    }

    private Map<String, String> buildHeaders() {
        Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "*/*");
        headers.put("Accept-Language", "zh-CN,zh;q=0.9");
        headers.put("Cache-Control", "no-cache");
        headers.put("Connection", "keep-alive");
        headers.put("Origin", "https://968kj.com");
        headers.put("Pragma", "no-cache");
        headers.put("Referer", "https://968kj.com/");
        headers.put("Sec-Fetch-Dest", "empty");
        headers.put("Sec-Fetch-Mode", "cors");
        headers.put("Sec-Fetch-Site", "cross-site");
        headers.put("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36");
        return headers;
    }
}
