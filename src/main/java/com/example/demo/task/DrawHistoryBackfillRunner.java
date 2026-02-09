package com.example.demo.task;

import com.example.demo.api.DrawHistoryBackfillService;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * 历史开奖补全脚本：通过启动参数触发时，调用 {@link DrawHistoryBackfillService} 并按参数或默认日期执行后退出。
 * <p>
 * 使用方式（默认 2025-12-01 ～ 2026-02-02）：
 * <pre>
 *   java -jar canada-api.jar --draw-history-backfill
 * </pre>
 * 或指定日期范围：
 * <pre>
 *   java -jar canada-api.jar --draw-history-backfill --draw-history-backfill-from=2025-12-01 --draw-history-backfill-to=2026-02-02
 * </pre>
 * 也可通过 TestController 接口调用：GET /api/test/draw-history-backfill?from=2025-12-01&amp;to=2026-02-02
 */
@Slf4j
@Component
@Order(1)
public class DrawHistoryBackfillRunner implements ApplicationRunner {

    private static final String ARG_BACKFILL = "draw-history-backfill";
    private static final String ARG_FROM = "draw-history-backfill-from";
    private static final String ARG_TO = "draw-history-backfill-to";
    private static final LocalDate DEFAULT_FROM = LocalDate.of(2025, 12, 1);
    private static final LocalDate DEFAULT_TO = LocalDate.of(2026, 2, 9);

    @Resource
    private DrawHistoryBackfillService drawHistoryBackfillService;

    @Override
    public void run(ApplicationArguments args) {
        if (!args.containsOption(ARG_BACKFILL)) {
            return;
        }
        LocalDate from = parseDate(args, ARG_FROM, DEFAULT_FROM);
        LocalDate to = parseDate(args, ARG_TO, DEFAULT_TO);
        if (from == null || to == null || from.isAfter(to)) {
            log.error("开奖历史补全-参数无效 from={}, to={}", from, to);
            System.exit(1);
        }
        drawHistoryBackfillService.backfill(from, to);
        System.exit(0);
    }

    private static LocalDate parseDate(ApplicationArguments args, String optionName, LocalDate defaultVal) {
        List<String> values = args.getOptionValues(optionName);
        if (values == null || values.isEmpty()) {
            return defaultVal;
        }
        try {
            return LocalDate.parse(values.get(0));
        } catch (DateTimeParseException e) {
            log.warn("开奖历史补全-无法解析日期 {}={}", optionName, values.get(0));
            return defaultVal;
        }
    }
}
