package com.example.demo.api;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.date.TimeInterval;
import cn.hutool.core.lang.Validator;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.core.util.*;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.example.demo.common.constants.RedisConstants;
import com.example.demo.common.utils.KeyUtil;
import com.example.demo.common.utils.ToDayRangeUtil;
import com.example.demo.config.OkHttpProxyDispatcher;
import com.example.demo.config.PriorityTaskExecutor;
import com.example.demo.core.model.UserConfig;
import com.example.demo.model.dto.AdminLoginDTO;
import com.example.demo.model.vo.*;
import com.example.demo.model.vo.plandouble.PlanDoubleVO;
import com.example.demo.model.vo.plandouble.PositionVO;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.*;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@Component
public class PlanDoubleService {

    @Resource(name = "defaultRedissonClient")
    private RedissonClient redisson;

    @Resource(name = "businessUserRedissonClient")
    private RedissonClient businessUserRedissonClient;

    @Resource
    private ConfigService configService;

    @Resource
    private OkHttpProxyDispatcher dispatcher;

    @Resource
    private HandicapApi handicapApi;

    /**
     * 计算延迟时间
     * @param closeTime
     * @param userConfigCloseTime
     * @return
     */
    private long calculateRemainingTime(long closeTime, Integer userConfigCloseTime) {
        if (userConfigCloseTime == null || userConfigCloseTime == 0) {
            return 0;
        }
        long currentTime = System.currentTimeMillis() / 1000;
        return closeTime - userConfigCloseTime - currentTime;
    }

    /**
     * 获取所有开启下注的平台用户
     * @return
     */
    private List<AdminLoginDTO> getAutoBetAdminUsers() {
        // 匹配所有平台用户的 Redis Key
        String pattern = KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, "*");

        // 使用 Redisson 执行扫描所有平台用户操作
        RKeys keys = businessUserRedissonClient.getKeys();
        Iterator<String> iterableKeys = keys.getKeysByPattern(pattern).iterator();

        // 提取出已开启方案的平台用户和方案 使用线程安全的列表
        List<AdminLoginDTO> adminUsers = new ArrayList<>();

        // 创建批量操作实例
        RBatch batch = businessUserRedissonClient.createBatch();

        // 存储 RBucket 实例
        List<RBucket<String>> buckets = new ArrayList<>();

        // 将每个操作添加到批处理队列
        while (iterableKeys.hasNext()) {
            String key = iterableKeys.next();
            RBucket<String> bucket = businessUserRedissonClient.getBucket(key); // 获取 RBucket 对象
            bucket.getAsync(); // 为每个键添加获取操作
            buckets.add(bucket);
        }
        // 执行所有批量操作
        batch.execute();

        // 处理结果
        buckets.forEach(bucket -> {
            String json = bucket.get();  // 获取每个键的值
            if (json != null) {
                AdminLoginDTO admin = JSONUtil.toBean(json, AdminLoginDTO.class);
                if (null != admin.getAutoBet() && admin.getAutoBet() != 0) {
                    adminUsers.add(admin);
                }
            }
        });
        return adminUsers;
    }

    private void logThreadPoolStatus(ThreadPoolExecutor executor, String poolName) {
        log.info("{} 状态 - 核心线程数: {}, 最大线程数: {}, 当前线程数: {}, 活跃线程数: {}, 队列大小: {}, 已完成任务数: {}",
                poolName,
                executor.getCorePoolSize(),
                executor.getMaximumPoolSize(),
                executor.getPoolSize(),
                executor.getActiveCount(),
                executor.getQueue().size(),
                executor.getCompletedTaskCount());
    }

    /**
     * 自动下注 - 翻倍计划
     * 使用@Async注解，将方法标记为异步执行,不会阻塞定时任务
     */
    @Async
    public void autoBetCompletableFuturePlanDouble() {
        TimeInterval timerTotal = DateUtil.timer();
        TimeInterval timerPre = DateUtil.timer();
        log.info("翻倍计划-前置操作，获取真正有效的平台用户和方案...");
        TimeInterval timerRedis = DateUtil.timer();
        // 提取出已开启下注的平台用户和方案 使用线程安全的列表
        // 第一阶段：过滤出有效平台用户
        List<AdminLoginDTO> adminUsers = getAutoBetAdminUsers();
        log.info("翻倍计划-过滤出开启下注的平台用户花费:{}毫秒", timerRedis.interval());
        // 如果没有有效用户，直接退出
        if (CollUtil.isEmpty(adminUsers)) {
            log.info("翻倍计划-所有平台用户都未开启自动下注，直接退出当前任务");
            return;
        }

        // 计数器，用于统计成功/失败数量
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();
        AtomicInteger reverseCount = new AtomicInteger();
        // 计数器，用于统计总的有效方案数量
        AtomicInteger totalTasks = new AtomicInteger(0);
        // 计数器，用于统计总的有效账号数量
        AtomicInteger totalAccounts = new AtomicInteger(0);
        // 计数器，用于统计总的下注数量
        AtomicInteger totalBets = new AtomicInteger(0);
        // 使用一个列表保存需要删除的用户
        List<AdminLoginDTO> toRemoveUsers = new ArrayList<>();
        Map<String, List<PlanDoubleVO>> enabledPlanDoublesMap = new HashMap<>();
        Map<String, List<UserConfig>> tokenValidConfigsMap = new HashMap<>();

        // 第二阶段：并发处理每个有效用户的方案和登录账号
        TimeInterval timerThear = DateUtil.timer();
        int cpuCoreCount = Runtime.getRuntime().availableProcessors();
        ExecutorService executorAdminUserService = Executors.newFixedThreadPool(Math.min(adminUsers.size(), cpuCoreCount * 2));
//        ExecutorService executorAdminUserService = Executors.newCachedThreadPool();

        // 使用 ForkJoinPool 进行更高效的任务执行
//        ForkJoinPool forkJoinPool = new ForkJoinPool(Math.min(adminUsers.size(), cpuCoreCount * 2));
        List<CompletableFuture<Void>> adminFutures = new ArrayList<>();

        // 先请求所有用户的数据
        adminUsers.forEach(admin -> {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                // 批量请求启用方案和有效配置
                CompletableFuture<List<PlanDoubleVO>> enabledPlanDoubleFuture = getEnabledConfigPlansAsync(admin.getUsername());
                CompletableFuture<List<UserConfig>> tokenValidConfigsFuture = getTokenValidConfigsAsync(admin.getUsername());

                CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(
                        enabledPlanDoubleFuture,
                        tokenValidConfigsFuture
                ).thenRun(() -> {
                    try {
                        // 校验当期是否需要跳过
                        JSONObject skipPlan = configService.getMaxDatetimeNotice(admin.getUsername());
                        if (skipPlan != null) {
                            LocalDateTime now = LocalDateTime.now();
                            LocalDateTime localDateTime = skipPlan.getLocalDateTime("datetime", now.plusSeconds(-60));
                            // -60是为了防止datetime时间不存在
                            Duration between = LocalDateTimeUtil.between(localDateTime, now);
                            if (between.toSeconds() < 60) {
                                // 如果距离上次跳过通知时间小于60秒，则跳过当期投注
                                log.info("翻倍计划-平台用户:{}发生投注跳过事件,剩余跳过时间:{}秒", admin.getUsername(), 60 - between.toSeconds());
                                toRemoveUsers.add(admin);
                                return;
                            }
                        }

                        // 获取启用的配置方案和有效配置
                        List<PlanDoubleVO> enabledPlanDouble = enabledPlanDoubleFuture.join();
                        List<UserConfig> tokenValidConfigs = tokenValidConfigsFuture.join();

                        // 校验结果并更新
                        if (CollUtil.isEmpty(enabledPlanDouble) || CollUtil.isEmpty(tokenValidConfigs)) {
                            toRemoveUsers.add(admin);
                            return;
                        }
                        // 校验正反投账号
                        List<UserConfig> validUserConfigs = validUserBetTypeAsync(admin, tokenValidConfigs).join();
                        if (CollUtil.isEmpty(validUserConfigs)) {
                            toRemoveUsers.add(admin);
                            return;
                        }

                        // 将有效方案添加到 map
                        enabledPlanDoublesMap.put(admin.getUsername(), enabledPlanDouble);
                        tokenValidConfigsMap.put(admin.getUsername(), tokenValidConfigs);

                        // 更新总有效方案数量
                        totalTasks.addAndGet(enabledPlanDouble.size());
                        totalAccounts.addAndGet(tokenValidConfigs.size());
                    } catch (Exception e) {
                        log.error("翻倍计划-处理用户 {} 时出错", admin.getUsername(), e);
                    }
                });
                // 使用 ForkJoinPool 提高并发性能
                combinedFuture.join();
            }, executorAdminUserService);

            adminFutures.add(future);
        });

        // 等待所有任务完成
        CompletableFuture<Void> allOf = CompletableFuture.allOf(adminFutures.toArray(new CompletableFuture[0]));
        allOf.join(); // 等待所有任务完成
        PriorityTaskExecutor.shutdownExecutor(executorAdminUserService);
        log.info("翻倍计划-过滤出所有开启方案和已登录账号花费:{}毫秒", timerThear.interval());
        // 过滤掉需要删除的用户
        adminUsers.removeAll(toRemoveUsers);
        // 如果没有有效用户，直接退出
        if (CollUtil.isEmpty(adminUsers)) {
            log.info("翻倍计划-所有平台用户都未开启方案或者不存在有效登录账号，直接退出当前任务");
            return;
        }
        log.info("翻倍计划-前置操作结束,当前任务有:{}位平台用户开启方案并且存在有效登录账号,总有效方案数量:{},花费:{}毫秒", adminUsers.size(), totalTasks.get(), timerPre.interval());

        int threadPoolUserSize = Math.min(adminUsers.size(), cpuCoreCount * 2);
        int threadPoolPlanSize = Math.min(totalTasks.get(), Math.max(cpuCoreCount * 2, 100));
        // 初始化线程池（动态线程池，避免任务阻塞）
        // ExecutorService executorUserService = Executors.newFixedThreadPool(threadPoolUserSize);
        // ExecutorService executorPlanService = Executors.newFixedThreadPool(threadPoolPlanSize);
        // 平台用户线程池
        ExecutorService executorUserService = new ThreadPoolExecutor(
                threadPoolUserSize, // 核心线程数
                Math.max(adminUsers.size(), threadPoolUserSize), // 最大线程数
                0L, TimeUnit.MILLISECONDS, // 无空闲线程超时
                new ArrayBlockingQueue<>(300), // 有界队列，避免任务积压
                new ThreadFactoryBuilder().setNameFormat("double-user-pool-%d").build(), // 自定义线程工厂，便于调试
                new ThreadPoolExecutor.CallerRunsPolicy() // 拒绝策略：由调用者线程执行任务
        );

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        // 对每个平台用户的操作并行化
        for (AdminLoginDTO admin : adminUsers) {
            // 用户方案线程池
            ExecutorService executorPlanService = new ThreadPoolExecutor(
                    threadPoolPlanSize, // 核心线程数
                    Math.max(totalTasks.get(), threadPoolPlanSize), // 最大线程数
                    0L, TimeUnit.SECONDS, // 空闲线程超时时间
                    new LinkedBlockingQueue<>(1000), // 有界队列，防止任务积压
                    new ThreadFactoryBuilder().setNameFormat("double-plan-pool-%d").build(),
                    new ThreadPoolExecutor.AbortPolicy() // 拒绝策略：抛出异常，便于发现问题
            );

            String username = admin.getUsername();
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
                TimeInterval timerFutures = DateUtil.timer();
                AtomicInteger successUserCount = new AtomicInteger();
                AtomicInteger failureUserCount = new AtomicInteger();
                AtomicInteger reverseUserCount = new AtomicInteger();
                // 获取该用户的启用方案
                List<PlanDoubleVO> enabledPlanDoubles = enabledPlanDoublesMap.get(admin.getUsername());
                if (CollUtil.isEmpty(enabledPlanDoubles)) {
                    log.info("翻倍计划-当前平台用户:{}不存在启用方案，直接跳过", username);
                    return null;
                }
                // 过滤掉未登录的盘口账号
                List<UserConfig> tokenValidConfigs = tokenValidConfigsMap.get(admin.getUsername());
                if (CollUtil.isEmpty(tokenValidConfigs)) {
                    log.info("翻倍计划-当前平台用户:{}不存在有效盘口账号，直接跳过", username);
                    return null;
                }

                // 初始化记录盘口账户的下单校验缓存
                Map<String, Boolean> validOrderCache = new ConcurrentHashMap<>();
                // 记录成功账号
                List<UserConfig> successAccounts = new CopyOnWriteArrayList<>();
                // 记录失败账号
                List<UserConfig> failedAccounts = new CopyOnWriteArrayList<>();
                // 记录每个方案使用的监控json
                Map<Integer, List<JSONObject>> useMonitorInfos = new ConcurrentHashMap<>();
                // 记录每个方案使用的正投账号
                Map<Integer, List<UserConfig>> usePositiveAccounts = new ConcurrentHashMap<>();
                // 记录账号使用的监控期数id
                Map<String, Map<Integer, String>> useMonitorIds = new ConcurrentHashMap<>();
                // 记录失败账号的监控期数id
                Map<String, Map<Integer, String>> failedMonitorIds = new ConcurrentHashMap<>();
                // ✅ 所有订单最终合并请求
                Map<String, OrderVO> finalOrder = new ConcurrentHashMap<>();
                // 获取方案列表中第一个所属游戏（前提是所有方案都是同一种游戏）
                String lottery = enabledPlanDoubles.get(0).getLottery();
                AtomicReference<String> drawNumber = new AtomicReference<>();
                AtomicReference<Long> drawTime = new AtomicReference<>();
                AtomicReference<Long> closeTime = new AtomicReference<>();
                // 方案当前监控期数
                Map<Integer, Integer> roundsMap = new ConcurrentHashMap<>();
                // 对每个配置计划进行异步处理
                List<CompletableFuture<Void>> planFutures = new ArrayList<>();
                for (PlanDoubleVO plan : enabledPlanDoubles) {
                    planFutures.add(CompletableFuture.runAsync(() -> {
                        TimeInterval timer = DateUtil.timer();
                        Map<String, String> accountOdds = new ConcurrentHashMap<>();
                        // 校验当期是否需要跳过
                        JSONObject skip = configService.getMaxDatetimeNotice(username);
                        if (skip != null) {
                            LocalDateTime now = LocalDateTime.now();
                            LocalDateTime localDateTime = skip.getLocalDateTime("datetime", now.plusSeconds(-60));
                            // -60是为了防止datetime时间不存在
                            Duration between = LocalDateTimeUtil.between(localDateTime, now);
                            if (between.toSeconds() < 60) {
                                // 如果距离上次跳过通知时间小于60秒，则跳过当期投注
                                log.info("翻倍计划-平台用户:{}发生投注跳过事件,剩余跳过时间:{}秒", username, 60 - between.toSeconds());
                                return;
                            }
                        }
                        Map<String, String> periodMap = handicapApi.validatePeriod(username, plan.getName(), plan.getLottery(), tokenValidConfigs);
                        // 下注前检验所有账号token是否有效，或者盘口网络是否正常，所有账号都获取期数成功说明正常，只要有一个账号获取失败则跳出下注
                        if (StringUtils.isBlank(periodMap.get("period"))) {
                            // 如果开启了跳过参数，则跳过,这里如果设置了跳过通知，如果token或网络无效，就会出现不会投注的情况
                             if (1 == admin.getSkip()) {
                                // 如果开启了跳过参数，则跳过,这里如果设置了跳过通知，如果token或网络无效不会投注的情况,然后又因为没有通知导致前端一下不知道什么情况为什么不投注
                                handicapApi.handlePeriodError(username, periodMap.get("account"), plan.getName(), plan.getLottery());
                             }
                            return;
                        }

                        /*JSONObject validateOrder = falaliApi.validateOrder(username, plan.getName(), plan.getLottery(), tokenValidConfigs, validOrderCache);
                        // 下注前检验所有账号token是否有效，或者盘口网络是否正常，所有账号都获取期数成功说明正常，只要有一个账号获取失败则跳出下注
                        if (!validateOrder.getBool("valid")) {
                            // 如果开启了跳过参数，则跳过,这里如果设置了跳过通知，如果token或网络无效不会投注的情况,然后又因为没有通知导致前端一下不知道什么情况为什么不投注
                            if (1 == admin.getSkip()) {
                                falaliApi.handlePeriodError(username, validateOrder.getStr("account"), plan.getName(), plan.getLottery());
                            }
                            return;
                        }*/

                        JSONObject periodJson = JSONUtil.parseObj(periodMap.get("period"));
                        if (StringUtils.isNotBlank(periodMap.get("period")) && drawNumber.get() == null) {
                            drawNumber.compareAndSet(null, periodJson.getStr("drawNumber"));
                            drawTime.compareAndSet(null, periodJson.getLong("drawTime"));
                            closeTime.compareAndSet(null, periodJson.getLong("closeTime"));
                        }

                        // 校验当前期数是否为可下注状态
                        if (1 != periodJson.getInt("status")) {
                            log.info("翻倍计划-平台用户:{},游戏:{},期数:{},当前期数不可下注", username, plan.getLottery(), drawNumber.get());
                            return;
                        }

                        // 校验封盘时间
                        if (!validateCloseTime(periodJson, username, plan.getLottery(), drawNumber.get())) {
                            return;
                        }

                        // 获取期数锁
                        RLock lock = acquireBetLock(periodJson, username, plan, drawNumber.get());
                        if (lock == null) {
                            log.info("翻倍计划-平台用户:{},游戏:{},期数:{},方案:{},当前期数正在执行", username, plan.getLottery(), drawNumber.get(), plan.getName());
                            return;
                        }

                        // 获取正投账号数
                        List<UserConfig> positiveAccounts = getRandomAccount(tokenValidConfigs, plan.getPositiveAccountNum(), 1);
                        // 获取反投账号数
                        List<UserConfig> reverseAccounts = getRandomAccount(tokenValidConfigs, plan.getReverseAccountNum(), 2);
                        // 把正反投账号集合
                        List<UserConfig> allAccounts = new ArrayList<>();
                        allAccounts.addAll(positiveAccounts);
                        allAccounts.addAll(reverseAccounts);
                        // 记录当前方案的正投账号
                        usePositiveAccounts.put(plan.getId(), positiveAccounts);
                        totalBets.addAndGet(allAccounts.size());
                        // 定义线程池
                        TimeInterval timerOdds = DateUtil.timer();
                        ExecutorService oddsThreadPool = Executors.newFixedThreadPool(Math.min(allAccounts.size(), 10));
                        List<CompletableFuture<Void>> futuresOdds = new ArrayList<>();
                        for (UserConfig userConfig : allAccounts) {
                            CompletableFuture<Void> futureOdds = CompletableFuture.runAsync(() -> {
                                try {
                                    // 获取赔率
                                    String odds = handicapApi.getOddsFromCache(username, drawNumber.get(), userConfig.getId(), plan.getLottery(), plan.getName());
                                    accountOdds.put(userConfig.getAccount(), odds);
                                } catch (Exception e) {
                                    log.warn("翻倍计划-获取赔率异常 平台用户:{},游戏:{},期数:{},方案:{},账户:{}", username, plan.getLottery(), drawNumber.get(), plan.getName(), userConfig.getAccount(), e);
                                }
                            }, oddsThreadPool);
                            futuresOdds.add(futureOdds);
                        }
                        // 等待所有异步任务完成
                        CompletableFuture.allOf(futuresOdds.toArray(new CompletableFuture[0])).join();
                        // 打印线程池状态
                        logThreadPoolStatus((ThreadPoolExecutor) oddsThreadPool, "翻倍计划-批量获取账号赔率线程池");
                        // 关闭线程池
                        PriorityTaskExecutor.shutdownExecutor(oddsThreadPool);
                        log.info("翻倍计划-获取所有账号赔率 平台用户:{},有效盘口账号数:{},游戏:{},期数:{},封盘时间:{},方案:{},花费:{}ms", username, tokenValidConfigs.size(), plan.getLottery(), drawNumber.get(), LocalDateTimeUtil.of(closeTime.get()), plan.getName(), timerOdds.interval());

                        if (accountOdds.isEmpty()) {
                            log.info("翻倍计划-获取整个方案的用户赔率为空 平台用户:{},游戏:{},期数:{},方案:{},释放当前期数锁", username, plan.getLottery(), drawNumber.get(), plan.getName());
                            // 确保锁释放
                            if (lock.isHeldByCurrentThread()) {
                                lock.unlock();
                            }
                            if (admin.getSkip() == 1) {
                                handicapApi.handlePeriodError(username, "所有账号", plan.getName(), plan.getLottery());
                            }
                            return;
                        }
                        // 校验赔率账号数量是否一致
                        if (accountOdds.size() != allAccounts.size()) {
                            List<String> missingAccounts = allAccounts.stream()
                                    .map(UserConfig::getAccount)
                                    .filter(acc -> !accountOdds.containsKey(acc))
                                    .collect(Collectors.toList());

                            log.warn("翻倍计划-部分账号赔率缺失 平台用户:{},缺失账号:{},游戏:{},期数:{},方案:{}",
                                    username, missingAccounts, plan.getLottery(), drawNumber.get(), plan.getName());

                            if (lock.isHeldByCurrentThread()) {
                                lock.unlock();
                            }
                            if (admin.getSkip() == 1) {
                                handicapApi.handlePeriodError(username, String.join(",", missingAccounts), plan.getName(), plan.getLottery());
                            }
                            return;
                        }
                        // 获取一个赔率
                        String odds = accountOdds.get(allAccounts.get(0).getAccount());
                        if (StringUtils.isBlank(odds)) {
                            log.error("翻倍计划-获取赔率失败 游戏:{},期数:{},平台用户:{},方案:{},释放当前期数锁", plan.getLottery(), drawNumber.get(), username, plan.getName());
                            // 确保锁释放
                            if (lock.isHeldByCurrentThread()) {
                                lock.unlock();
                            }
                            if (admin.getSkip() == 1) {
                                handicapApi.handlePeriodError(username, "所有账号", plan.getName(), plan.getLottery());
                            }
                            return;
                        }
                        JSONObject oddsJson = JSONUtil.parseObj(odds);

                        // 获取正反投的位置 key
                        Map<Integer, Map<String, List<String>>> oddsMap = new HashMap<>();
                        // 获取号码
                        List<PositionVO> positions = new ArrayList<>();
                        // 获取大小
                        Map<String, List<Integer>> twoSidedDxPositions = new HashMap<>();
                        // 获取单双
                        Map<String, List<Integer>> twoSidedDsPositions = new HashMap<>();
                        // 获取龙虎
                        Map<String, List<Integer>> twoSidedLhPositions = new HashMap<>();

                        List<Integer> amountKeys = new ArrayList<>();
                        if (null != plan.getPositions()) {
                            String lastResult = null;
                            // 如果是类型2则获取最新开奖信息
                            if (1 == plan.getPlanType()) {
                                log.info("翻倍计划-号码投注-平台用户:{},游戏:{},期数:{}", username, plan.getLottery(), drawNumber.get());
                                for (PositionVO pos : plan.getPositions()) {
                                    if (pos.getEnable() == 0) continue;
                                    boolean hasChinese = Validator.hasChinese(pos.getPositionNum());
                                    if (!hasChinese) {
                                        // 没有中文的一概以号码处理
                                        List<Integer> ids = formatIds(pos.getPositionNum());
//                                        List<Integer> idsNew = new ArrayList<>();
//                                        if (plan.getReverseType() == 1) { // 竖式
//                                            idsNew = ids;
//                                        } else if (plan.getReverseType() == 2) {    // 横式
//                                            // 获取除了ids的其他位置
//                                            idsNew = IntStream.rangeClosed(1, 10) // 生成 1 到 10 的整数
//                                                    .boxed() // 转换为 List<Integer>
//                                                    .filter(id -> !ids.contains(id)) // 过滤掉 `ids` 中已有的值
//                                                    .toList();
//                                        }
                                        for (Integer id : ids) {
                                            String jsonKey = "B" + id;
                                            List<String> matchedKeys = oddsJson.keySet().stream()
                                                    .filter(matchedKey -> matchedKey.contains(jsonKey + "_"))
                                                    .collect(Collectors.toList());
                                            Map<String, List<String>> oddKeys = getOdds(plan.getReverseType(), matchedKeys, pos.getLocationNum());
                                            oddsMap.put(id, oddKeys);
                                            amountKeys.add(id);
                                        }
                                        positions.add(pos);
                                    } else {
                                        List<Integer> ids = formatIds(pos.getLocationNum());
                                        // 有中文的以双面处理
                                        if (pos.getPositionNum().contains("大") || pos.getPositionNum().contains("小")) {
                                            // 获取大小
                                            twoSidedDxPositions.put(pos.getPositionNum(), ids);
                                        } else if (pos.getPositionNum().contains("单") || pos.getPositionNum().contains("双")) {
                                            // 获取单双
                                            twoSidedDsPositions.put(pos.getPositionNum(), ids);
                                        } else if (pos.getPositionNum().contains("龙") || pos.getPositionNum().contains("虎")) {
                                            // 获取龙虎
                                            twoSidedLhPositions.put(pos.getPositionNum(), ids);
                                        }
                                    }
                                }
                            } else if (2 == plan.getPlanType()) {
                                log.info("翻倍计划-位置开号投注-平台用户:{},游戏:{},期数:{}", username, plan.getLottery(), drawNumber.get());
                                // 获取最新开奖信息
                                try {
                                    lastResult = handicapApi.lastResult(username, tokenValidConfigs.get(0).getId(), plan.getLottery());
                                    JSONObject lastResultJson = JSONUtil.parseObj(lastResult);
                                    log.info("翻倍计划-位置开号投注-平台用户:{},游戏:{},期数:{},最新开奖信息:{}", username, plan.getLottery(), drawNumber.get(), lastResultJson);
                                    amountKeys = formatIds(lastResultJson.getStr("result"));
                                    for (PositionVO pos : plan.getPositions()) {
                                        if (pos.getEnable() == 0) continue;
                                        // 位置开号投注类型全都是号码
                                        List<Integer> ids = formatIds(pos.getPositionNum());
//                                        List<Integer> idsNew = new ArrayList<>();
//                                        if (plan.getReverseType() == 1) { // 竖式
//                                            idsNew = ids;
//                                        } else if (plan.getReverseType() == 2) {    // 横式
//                                            idsNew = IntStream.rangeClosed(1, 10) // 生成 1 到 10 的整数
//                                                    .boxed() // 转换为 List<Integer>
//                                                    .filter(id -> !ids.contains(id)) // 过滤掉 `ids` 中已有的值
//                                                    .toList();
//                                        }
                                        for (Integer id : ids) {
                                            String jsonKey = "B" + id;
                                            List<Integer> locations = new ArrayList<>();
                                            List<Integer> locationIds = formatIds(pos.getLocationNum());
                                            for (Integer locationId : locationIds) {
                                                locations.add(amountKeys.get(locationId - 1));
                                            }
                                            List<String> matchedKeys = oddsJson.keySet().stream()
                                                    .filter(matchedKey -> matchedKey.contains(jsonKey + "_"))
                                                    .collect(Collectors.toList());
                                            Map<String, List<String>> oddKeys = getOdds(plan.getReverseType(), matchedKeys, Joiner.on(",").join(locations));
                                            oddsMap.put(id, oddKeys);
                                        }
                                        positions.add(pos);
                                    }
                                } catch (Exception e) {
                                    log.error("翻倍计划-位置开号投注-平台用户:{},游戏:{},期数:{},获取最新开奖信息失败", username, plan.getLottery(), drawNumber.get());
                                    return;
                                }
                            } else if (3 == plan.getPlanType()) {
                                log.info("翻倍计划-灵活开某投某-平台用户:{},游戏:{},期数:{}", username, plan.getLottery(), drawNumber.get());
                                // 获取最新开奖信息
                                try {
                                    lastResult = handicapApi.lastResult(username, tokenValidConfigs.get(0).getId(), plan.getLottery());
                                    JSONObject lastResultJson = JSONUtil.parseObj(lastResult);
                                    log.info("翻倍计划-灵活开某投某-平台用户:{},游戏:{},期数:{},最新开奖信息:{}", username, plan.getLottery(), drawNumber.get(), lastResultJson);
                                    amountKeys = formatIds(lastResultJson.getStr("result"));
                                    for (PositionVO pos : plan.getPositions()) {
                                        if (pos.getEnable() == 0) continue;
                                        int id = Integer.parseInt(pos.getWinPositionNum());
                                        // 最新开奖号码需要和设置的一致才进行投注
                                        if (Objects.equals(amountKeys.get(Integer.parseInt(pos.getWinLocationNum()) - 1), id)) {
                                            List<Integer> posNum = formatIds(pos.getPositionNum());
//                                            List<Integer> idsNew = new ArrayList<>();
//                                            if (plan.getReverseType() == 1) { // 竖式
//                                                idsNew = posNum;
//                                            } else if (plan.getReverseType() == 2) {    // 横式
//                                                idsNew = IntStream.rangeClosed(1, 10) // 生成 1 到 10 的整数
//                                                        .boxed() // 转换为 List<Integer>
//                                                        .filter(newId -> !posNum.contains(newId)) // 过滤掉 `ids` 中已有的值
//                                                        .toList();
//                                            }
                                            for (Integer posNumId : posNum) {
                                                String jsonKey = "B" + posNumId;
                                                List<String> matchedKeys = oddsJson.keySet().stream()
                                                        .filter(matchedKey -> matchedKey.contains(jsonKey + "_"))
                                                        .collect(Collectors.toList());
                                                // 这个计划的位置数组需要使用Locations参数，由list转换成string
                                                Map<String, List<String>> oddKeys = getOdds(plan.getReverseType(), matchedKeys, pos.getLocationNum());
                                                oddsMap.put(posNumId, oddKeys);
                                                positions.add(pos);
                                            }
                                        }
                                    }
                                } catch (Exception e) {
                                    log.error("翻倍计划-灵活开某投某-平台用户:{},游戏:{},期数:{},获取最新开奖信息失败", username, plan.getLottery(), drawNumber.get());
                                    return;
                                }
                            } else if (5 == plan.getPlanType()) {
                                log.info("翻倍计划-005-号码追踪-平台用户:{},游戏:{},期数:{}", username, plan.getLottery(), drawNumber.get());
                                // 获取最新开奖信息
                                try {
                                    lastResult = handicapApi.lastResult(username, tokenValidConfigs.get(0).getId(), plan.getLottery());
                                    JSONObject lastResultJson = JSONUtil.parseObj(lastResult);
                                    log.info("翻倍计划-号码追踪-平台用户:{},游戏:{},期数:{},最新开奖信息:{}", username, plan.getLottery(), drawNumber.get(), lastResultJson);
                                    List<Integer> resultKeys = formatIds(lastResultJson.getStr("result"));
                                    for (PositionVO pos : plan.getPositions()) {
                                        if (pos.getEnable() == 0) continue;
                                        if (!resultKeys.contains(Integer.valueOf(pos.getTrackPositionNum()))) {
                                            continue;
                                        }
                                        for (int i = 0; i < resultKeys.size(); i++) {
                                            if (!Objects.equals(resultKeys.get(i), Integer.valueOf(pos.getTrackPositionNum()))) {
                                                continue;
                                            }
                                            // 匹配到号码
                                            List<String> ids = formatIdsStr(pos.getShiftPositionNum());
                                            List<Integer> posNum = new ArrayList<>();
                                            for (String id : ids) {
                                                int key;
                                                if (id.contains("+")) {
                                                    // +号表示号码往后移动一位,+1是因为list数组下标是从0开始，开奖信息是从1开始
                                                    key = i + 1 + Integer.parseInt(id.replaceAll("[^0-9]", ""));    // 去掉非数字字符
                                                    if (key > 10) {
                                                        // 如果计算后的位置下标大于则10跳出循环，因为位置最大为10
                                                        continue;
                                                    }
                                                } else if (id.contains("-")) {
                                                    // -号表示号码往前移动一位,-1是因为list数组下标是从0开始，开奖信息是从1开始
                                                    key = i + 1 - Integer.parseInt(id.replaceAll("[^0-9]", ""));    // 去掉非数字字符
                                                    if (key < 1) {
                                                        // 如果计算后的位置下标小于则1跳出循环，因为位置最小为1
                                                        continue;
                                                    }
                                                } else {
                                                    // 默认*号,+1是因为list数组下标是从0开始，开奖信息是从1开始
                                                    key = i + 1;
                                                }
                                                posNum.add(key);
                                            }
//                                            List<Integer> idsNew = new ArrayList<>();
//                                            if (plan.getReverseType() == 1) { // 竖式
//                                                idsNew = posNum;
//                                            } else if (plan.getReverseType() == 2) {    // 横式
//                                                idsNew = IntStream.rangeClosed(1, 10) // 生成 1 到 10 的整数
//                                                        .boxed() // 转换为 List<Integer>
//                                                        .filter(newId -> !posNum.contains(newId)) // 过滤掉 `ids` 中已有的值
//                                                        .toList();
//                                            }

                                            for (Integer posNumId : posNum) {
                                                String jsonKey = "B" + posNumId;
                                                List<String> matchedKeys = oddsJson.keySet().stream()
                                                        .filter(matchedKey -> matchedKey.contains(jsonKey + "_"))
                                                        .collect(Collectors.toList());
                                                Map<String, List<String>> oddKeys = getOdds(plan.getReverseType(), matchedKeys, pos.getLocationNum());
                                                oddsMap.put(posNumId, oddKeys);
                                                amountKeys.add(posNumId);
                                            }
                                            positions.add(pos);
                                        }
                                    }
                                } catch (Exception e) {
                                    log.error("翻倍计划-号码追踪-平台用户:{},游戏:{},期数:{},获取最新开奖信息失败", username, plan.getLottery(), drawNumber.get());
                                    return;
                                }
                            } else if (17 == plan.getPlanType()) {
                                log.info("翻倍计划-017-冷热号投注-平台用户:{},游戏:{},期数:{}", username, plan.getLottery(), drawNumber.get());
                                // 获取最新开奖信息
                                try {
                                    String drawKey = KeyUtil.genKey(
                                            RedisConstants.DRAW_RESULT_PREFIX,
                                            plan.getLottery(),
                                            "latest"
                                    );
                                    JSONArray dresults = JSONUtil.parseArray(businessUserRedissonClient.getBucket(drawKey).get());
                                    if (dresults == null || dresults.isEmpty()) {
                                        dresults = handicapApi.dresults(username, tokenValidConfigs.get(0).getId(), plan.getLottery());
                                    }
                                    for (Object dresult : dresults) {
                                        JSONObject dresultJson = JSONUtil.parseObj(dresult);
                                        String drawTimeNew = dresultJson.getStr("drawTime");
                                        long between = LocalDateTimeUtil.between(LocalDateTime.now(), LocalDateTimeUtil.parse(drawTimeNew, DatePattern.NORM_DATETIME_PATTERN), ChronoUnit.SECONDS);
                                        // 如果最新的开奖时间小于当前时间，between结果为负
                                        if (between < 0) {
                                            // 获取最新开奖结果并插入缓存中
                                            dresults = handicapApi.dresults(username, tokenValidConfigs.get(0).getId(), plan.getLottery());
                                        }
                                        // 只获取最新一条开奖信息，所以执行完一遍后直接退出
                                        break;
                                    }
                                    if (dresults == null || dresults.isEmpty()) {
                                        log.error("翻倍计划-017-冷热号投注-平台用户:{},游戏:{},期数:{},获取最新开奖信息失败", username, plan.getLottery(), drawNumber.get());
                                        return;
                                    }
                                    // 更新开奖列表,保存24小时
                                    businessUserRedissonClient.getBucket(drawKey).set(dresults, Duration.ofHours(24));
                                    log.info("翻倍计划-017-冷热号投注-平台用户:{},游戏:{},期数:{}", username, plan.getLottery(), drawNumber.get());
                                    for (PositionVO pos : plan.getPositions()) {
                                        if (pos.getEnable() == 0) continue;
                                        List<Integer> ids = formatIds(pos.getPositionNum());
                                        int index = Integer.parseInt(pos.getCoolHotPositionNum());
                                        // 获取冷热号位置
                                        List<String> coolHotNumbers = getCoolHotNumbersAtIndex(dresults, index);
                                        List<Integer> rankIds = formatIds(pos.getCoolHotRank());
                                        StringBuilder locationNumBuilder = new StringBuilder();
                                        for (Integer rankId : rankIds) {
                                            if (rankId > 0 && rankId <= coolHotNumbers.size()) {  // 避免越界
                                                if (!locationNumBuilder.isEmpty()) {
                                                    locationNumBuilder.append(",");
                                                }
                                                locationNumBuilder.append(coolHotNumbers.get(rankId - 1));
                                            }
                                        }
//                                        List<Integer> idsNew = new ArrayList<>();
//                                        if (plan.getReverseType() == 1) { // 竖式
//                                            idsNew = ids;
//                                        } else if (plan.getReverseType() == 2) {  // 横式
//                                            idsNew = IntStream.rangeClosed(1, 10) // 生成 1 到 10 的整数
//                                                    .boxed() // 转换为 List<Integer>
//                                                    .filter(newId -> !ids.contains(newId)) // 过滤掉 `ids` 中已有的值
//                                                    .toList();
//                                        }
                                        for (Integer id : ids) {
                                            String jsonKey = "B" + id;
                                            List<String> matchedKeys = oddsJson.keySet().stream()
                                                    .filter(matchedKey -> matchedKey.contains(jsonKey + "_"))
                                                    .collect(Collectors.toList());
                                            // 这个计划的位置数组需要使用Locations参数，由list转换成string
                                            Map<String, List<String>> oddKeys = getOdds(plan.getReverseType(), matchedKeys, locationNumBuilder.toString());
                                            oddsMap.put(id, oddKeys);
                                            positions.add(pos);
                                            amountKeys.add(id);
                                        }
                                    }
                                } catch (Exception e) {
                                    log.error("翻倍计划-017-冷热号投注-平台用户:{},游戏:{},期数:{},获取最新开奖信息失败", username, plan.getLottery(), drawNumber.get());
                                    return;
                                }
                            } else if (25 == plan.getPlanType()) {
                                log.info("翻倍计划-开某投某-平台用户:{},游戏:{},期数:{}", username, plan.getLottery(), drawNumber.get());
                                // 获取最新开奖信息
                                try {
                                    lastResult = handicapApi.lastResult(username, tokenValidConfigs.get(0).getId(), plan.getLottery());
                                    JSONObject lastResultJson = JSONUtil.parseObj(lastResult);
                                    log.info("翻倍计划-开某投某-平台用户:{},游戏:{},期数:{},最新开奖信息:{}", username, plan.getLottery(), drawNumber.get(), lastResultJson);
                                    amountKeys = formatIds(lastResultJson.getStr("result"));
                                    for (PositionVO pos : plan.getPositions()) {
                                        if (pos.getEnable() == 0 || StringUtils.isBlank(pos.getWinPositionNum())) continue;
                                        // 开某投某投注类型全都是号码
                                        int id = Integer.parseInt(pos.getWinPositionNum());

//                                        List<Integer> idsNew = new ArrayList<>();
//                                        if (plan.getReverseType() == 1) { // 竖式
//                                            idsNew = plan.getLocations();
//                                        } else if (plan.getReverseType() == 2) {    // 横式
//                                            idsNew = IntStream.rangeClosed(1, 10) // 生成 1 到 10 的整数
//                                                    .boxed() // 转换为 List<Integer>
//                                                    .filter(newId -> !plan.getLocations().contains(newId)) // 过滤掉 `ids` 中已有的值
//                                                    .toList();
//                                        }
                                        for (Integer location : plan.getLocations()) {
                                            // 最新开奖号码需要和设置的一致才进行投注
                                            if (Objects.equals(amountKeys.get(location - 1), id)) {
                                                String jsonKey = "B" + location;
                                                List<String> matchedKeys = oddsJson.keySet().stream()
                                                        .filter(matchedKey -> matchedKey.contains(jsonKey + "_"))
                                                        .collect(Collectors.toList());
                                                // 这个计划的位置数组需要使用Locations参数，由list转换成string
                                                Map<String, List<String>> oddKeys = getOdds(plan.getReverseType(), matchedKeys, pos.getPositionNum());
                                                oddsMap.put(location, oddKeys);
                                                positions.add(pos);
                                            }
                                        }
                                    }
                                } catch (Exception e) {
                                    log.error("翻倍计划-开某投某-平台用户:{},游戏:{},期数:{},获取最新开奖信息失败", username, plan.getLottery(), drawNumber.get());
                                    return;
                                }
                            }
                        }

                        // 生成监控期数key
                        String monitorKey = KeyUtil.genKey(
                                RedisConstants.USER_MONITOR_PREFIX,
                                username,
                                String.valueOf(plan.getId())
                        );
                        List<JSONObject> monitorInfos = businessUserRedissonClient.getList(monitorKey);
                        // 本期轮数值，根据轮数获取需要投注的金额
                        Integer rounds = 0;
                        // 下注线程池
                        ExecutorService executorBetService = new ThreadPoolExecutor(
                                allAccounts.size(), // 核心线程数
                                allAccounts.size(), // 最大线程数
                                0L, TimeUnit.SECONDS, // 空闲线程超时时间
                                new LinkedBlockingQueue<>(10), // 队列大小
                                new ThreadFactoryBuilder().setNameFormat("bet-pool-%d").build(),
                                new ThreadPoolExecutor.AbortPolicy() // 拒绝策略：抛出异常，便于发现问题
                        );

                        if (1 == plan.getDoubleType()) {
                            int monitorInfoIndex;
                            JSONObject monitorInfo = new JSONObject();
                            // 翻倍老号，投注老号，直到一轮结束或者中号
                            if (monitorInfos != null && !monitorInfos.isEmpty()) {
                                // 获取当前最新的监控期数，用于使用第几轮的投注金额
                                rounds = JSONUtil.parseObj(monitorInfos.get(monitorInfos.size() - 1)).getInt("rounds");
                                monitorInfoIndex = monitorInfos.size() - 1;
                                monitorInfo = JSONUtil.parseObj(monitorInfos.get(monitorInfos.size() - 1));
                            } else {
                                monitorInfoIndex = 0;
                                monitorInfo.putOpt("id", IdUtil.getSnowflakeNextIdStr());     // 期数ID
                                monitorInfo.putOpt("planId", plan.getId());     // 计划ID
                                monitorInfo.putOpt("drawNumber", drawNumber.get());   // 期数
                                monitorInfo.putOpt("rounds", 0);                // 监控轮次从第一期开始
                                monitorInfo.putOpt("createTime", LocalDateTimeUtil.formatNormal(LocalDateTime.now()));
                                monitorInfos = new ArrayList<>();
                                monitorInfos.add(monitorInfo);
                            }
                            // 配置监控轮次
                            roundsMap.put(plan.getId(), rounds);
                            // 记录当前计划对应的monitorInfos
                            useMonitorInfos.put(plan.getId(), monitorInfos);
                            // 使用 CountDownLatch 等待所有延迟任务完成
                            JSONObject twoSidedAmountJson = twoSidedAmountJson(oddsJson, twoSidedDxPositions, twoSidedDsPositions, twoSidedLhPositions, positiveAccounts, reverseAccounts, plan, rounds);
                            JSONObject amountJson = distributeAmount(oddsMap, positiveAccounts, reverseAccounts, plan, rounds);
                            if ((twoSidedAmountJson == null || twoSidedAmountJson.isEmpty()) && amountJson.isEmpty()) {
                                log.info("翻倍计划-获取分配金额全部为空,跳出任务 平台用户:{}, 游戏:{},期数:{},方案:{}", username, plan.getLottery(), drawNumber.get(), plan.getName());
                                return;
                            }
                            log.info("翻倍计划-翻倍老号-开始下注前,平台用户:{},游戏:{},期数:{},方案:{}, 正反投账号总数:{}, 正反投账号列表:{}, 投注金额拆分结果amountJson:{}", username, plan.getLottery(), drawNumber.get(), plan.getName(), allAccounts.size(), allAccounts, amountJson);
                            // 遍历下注操作
                            retryBet(username, plan, finalOrder, monitorInfo.getStr("id"), monitorInfo, drawNumber.get(), closeTime.get(), drawTime.get(),
                                    oddsJson, allAccounts, positiveAccounts, reverseAccounts, successAccounts, failedAccounts, useMonitorIds, failedMonitorIds, positions, amountJson, twoSidedAmountJson,
                                    successCount, failureCount, successUserCount, failureUserCount, amountKeys, executorBetService);

                            // 执行完下注任务后关闭线程池
                            PriorityTaskExecutor.shutdownExecutor(executorBetService);
                        } else if (2 == plan.getDoubleType()) {
                            int monitorInfoIndex;
                            JSONObject monitorInfo = new JSONObject();
                            // 翻倍新号，每次投注新号
                            if (monitorInfos != null && !monitorInfos.isEmpty()) {
                                // 获取当前最新的监控期数，用于使用第几轮的投注金额
                                rounds = JSONUtil.parseObj(monitorInfos.get(monitorInfos.size() - 1)).getInt("rounds");
                                monitorInfoIndex = monitorInfos.size() - 1;
                                monitorInfo = JSONUtil.parseObj(monitorInfos.get(monitorInfos.size() - 1));
                            } else {
                                monitorInfoIndex = 0;
                                monitorInfo.putOpt("id", IdUtil.getSnowflakeNextIdStr());     // 期数ID
                                monitorInfo.putOpt("planId", plan.getId());     // 计划ID
                                monitorInfo.putOpt("drawNumber", drawNumber.get());   // 期数
                                monitorInfo.putOpt("rounds", 0);                // 监控轮次从第一期开始
                                monitorInfo.putOpt("createTime", LocalDateTimeUtil.formatNormal(LocalDateTime.now()));
                                monitorInfos = new ArrayList<>();
                                monitorInfos.add(monitorInfo);
                            }
                            // 配置监控轮次
                            roundsMap.put(plan.getId(), rounds);
                            // 记录当前计划对应的monitorInfos
                            useMonitorInfos.put(plan.getId(), monitorInfos);
                            // 使用 CountDownLatch 等待所有延迟任务完成
                            // CountDownLatch latch = new CountDownLatch(allAccounts.size()); // 初始化为需要执行的任务数
                            JSONObject twoSidedAmountJson = twoSidedAmountJson(oddsJson, twoSidedDxPositions, twoSidedDsPositions, twoSidedLhPositions, positiveAccounts, reverseAccounts, plan, rounds);
                            JSONObject amountJson = distributeAmount(oddsMap, positiveAccounts, reverseAccounts, plan, rounds);
                            if ((twoSidedAmountJson == null || twoSidedAmountJson.isEmpty()) && (amountJson == null || amountJson.isEmpty())) {
                                log.info("翻倍计划-获取分配金额全部为空,跳出任务 平台用户:{}, 游戏:{},期数:{},方案:{}", username, plan.getLottery(), drawNumber.get(), plan.getName());
                                return;
                            }
                            log.info("翻倍计划-翻倍新号-开始下注前,平台用户:{},游戏:{},期数:{},方案:{}, 正反投账号总数:{}, 正反投账号列表:{}, 投注金额拆分结果amountJson:{}", username, plan.getLottery(), drawNumber.get(), plan.getName(), allAccounts.size(), allAccounts, amountJson);
                            // 遍历下注操作
                            retryBet(username, plan, finalOrder, monitorInfo.getStr("id"), monitorInfo, drawNumber.get(), closeTime.get(), drawTime.get(),
                                    oddsJson, allAccounts, positiveAccounts, reverseAccounts, successAccounts, failedAccounts, useMonitorIds, failedMonitorIds, positions, amountJson, twoSidedAmountJson,
                                    successCount, failureCount, successUserCount, failureUserCount, amountKeys, executorBetService);

                            // 执行完下注任务后关闭线程池
                            PriorityTaskExecutor.shutdownExecutor(executorBetService);
                        } else if (3 == plan.getDoubleType()) {
                            // 翻老投新,不用管上一期中不中,每期都是第一轮期数的金额
                            if (monitorInfos == null || monitorInfos.isEmpty()) {
                                monitorInfos = new ArrayList<>();
                            }

                            // 配置监控轮次
                            roundsMap.put(plan.getId(), rounds);
                            // 记录当前计划对应的monitorInfos
                            useMonitorInfos.put(plan.getId(), monitorInfos);
                            JSONObject monitorInfo = new JSONObject();
                            monitorInfo.putOpt("id", IdUtil.getSnowflakeNextIdStr());     // 期数ID
                            monitorInfo.putOpt("planId", plan.getId());     // 计划ID
                            monitorInfo.putOpt("drawNumber", drawNumber.get());   // 期数
                            monitorInfo.putOpt("rounds", 0);                // 监控轮次从第一期开始
                            monitorInfo.putOpt("createTime", LocalDateTimeUtil.formatNormal(LocalDateTime.now()));
                            monitorInfos.add(monitorInfo);
                            // 获取当前最新的监控期数，用于使用第几轮的投注金额
                            for (Object info : monitorInfos) {

                                JSONObject monitorInfoJson = JSONUtil.parseObj(info);

                                // 使用 CountDownLatch 等待所有延迟任务完成
                                // CountDownLatch latch = new CountDownLatch(allAccounts.size()); // 初始化为需要执行的任务数
                                JSONObject twoSidedAmountJson = twoSidedAmountJson(oddsJson, twoSidedDxPositions, twoSidedDsPositions, twoSidedLhPositions, positiveAccounts, reverseAccounts, plan, rounds);
                                JSONObject amountJson = distributeAmount(oddsMap, positiveAccounts, reverseAccounts, plan, monitorInfoJson.getInt("rounds"));
                                if ((twoSidedAmountJson == null || twoSidedAmountJson.isEmpty()) && (amountJson == null || amountJson.isEmpty())) {
                                    log.info("翻倍计划-获取分配金额全部为空,跳出任务 平台用户:{}, 游戏:{},期数:{},方案:{}", username, plan.getLottery(), drawNumber.get(), plan.getName());
                                    return;
                                }
                                log.info("翻倍计划-翻老投新-开始下注前,平台用户:{},游戏:{},期数:{},方案:{}, 正反投账号总数:{}, 正反投账号列表:{}, 投注金额拆分结果amountJson:{}", username, plan.getLottery(), drawNumber.get(), plan.getName(), allAccounts.size(), allAccounts, amountJson);
                                // 遍历下注操作
                                retryBet(username, plan, finalOrder, monitorInfoJson.getStr("id"), monitorInfoJson, drawNumber.get(), closeTime.get(), drawTime.get(),
                                        oddsJson, allAccounts, positiveAccounts, reverseAccounts, successAccounts, failedAccounts, useMonitorIds, failedMonitorIds, positions, amountJson, twoSidedAmountJson,
                                        successCount, failureCount, successUserCount, failureUserCount, amountKeys, executorBetService);

                            }
                            // 执行完下注任务后关闭线程池
                            PriorityTaskExecutor.shutdownExecutor(executorBetService);
                        }
                    }, executorPlanService));
                }
                // 等待所有配置计划任务完成
                CompletableFuture<Void> allPlans = CompletableFuture.allOf(planFutures.toArray(new CompletableFuture[0]));
                try {
                    allPlans.get();
                } catch (Exception e) {
                    log.error("翻倍计划-执行配置计划任务时发生异常 平台用户:{}", username, e);
                }
                logThreadPoolStatus((ThreadPoolExecutor) executorPlanService, "翻倍计划-方案线程池");
                PriorityTaskExecutor.shutdownExecutor(executorPlanService);

                // 存在监控id才进行合并投注
                if (!useMonitorIds.isEmpty()) {
                    // 下注线程池
                    ExecutorService executorMergeBetService = new ThreadPoolExecutor(
                            5, // 核心线程数
                            10, // 最大线程数
                            0L, TimeUnit.SECONDS, // 空闲线程超时时间
                            new LinkedBlockingQueue<>(10), // 队列大小
                            new ThreadFactoryBuilder().setNameFormat("bet-pool-%d").build(),
                            new ThreadPoolExecutor.AbortPolicy() // 拒绝策略：抛出异常，便于发现问题
                    );

                    // 在遍历下注操作时，使用线程池异步执行每个下注操作
                    List<CompletableFuture<Void>> betFutures = new ArrayList<>();
                    for (UserConfig userConfig : tokenValidConfigs) {
                        betFutures.add(CompletableFuture.runAsync(() -> {
                            if (null == finalOrder.get(userConfig.getAccount())) {
                                log.info("翻倍计划-发起下单,当前合并投注没有该账号的投注请求,无需投注, 平台用户:{}, 账号:{}, 期数:{}, 合并方案:{}", username, userConfig.getAccount(), drawNumber, finalOrder);
                                return;
                            }
                            String isBetRedisKey = KeyUtil.genKey(
                                    RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                                    lottery,
                                    ToDayRangeUtil.getToDayRange(),
                                    drawNumber.get(),
                                    "success",
                                    username,
                                    userConfig.getAccount(),
                                    "merge"
                            );
                            // 判断是否已下注
                            JSONObject exists = JSONUtil.parseObj(redisson.getBucket(isBetRedisKey).get());
                            if (!exists.isEmpty() && exists.getInt("type") == 2) {
                                log.info("翻倍计划-平台用户:{},游戏:{},期数:{},账号:{},方案:{},已下过注,不再重复下注", username, lottery, drawNumber.get(), userConfig.getAccount()
                                        , "合并式方案");
                                return;
                            }
                            log.info("翻倍计划-发起下单, 平台用户:{}, 账号:{}, 期数:{}, 合并方案:{}, 账号token:{}", username, userConfig.getAccount(), drawNumber, finalOrder, userConfig.getToken());
                            // 提交下注
                            String result = mergeSubmitOrder(username, userConfig, enabledPlanDoubles, finalOrder.get(userConfig.getAccount()), lottery, drawNumber.get(), useMonitorIds, closeTime.get());
                            // 结果解析
                            handleOrderResult(username, result, userConfig, lottery, drawNumber.get(), enabledPlanDoubles, successAccounts, failedAccounts, successCount, failureCount, successUserCount, failureUserCount);
                        }, executorMergeBetService));
                    }
                    // 等待所有下注操作完成
                    CompletableFuture<Void> allBets = CompletableFuture.allOf(betFutures.toArray(new CompletableFuture[0]));
                    try {
                        allBets.get();  // 等待所有下注操作完成
                    } catch (Exception e) {
                        log.error("翻倍计划-执行批量下注操作时发生异常:", e);
                    }

                    logThreadPoolStatus((ThreadPoolExecutor) executorMergeBetService, "翻倍计划-批量下注线程池");
                    // 执行完下注任务后关闭线程池
                    PriorityTaskExecutor.shutdownExecutor(executorMergeBetService);

                    TimeInterval timerProcess = DateUtil.timer();
                    int retryCount = 3;  // 最大重试次数
                    int attempt = 0;  // 当前重试次数
                    boolean successProcess = false;

                    // 等待所有延迟任务完成
                    while (attempt < retryCount && !successProcess) {
                        try {
                            // latch.await();
                            if (CollUtil.isNotEmpty(failedAccounts)) {
                                // 补单处理
                                processReverseBet(admin, failedAccounts, failedMonitorIds, roundsMap, successAccounts, enabledPlanDoubles, drawNumber.get(), closeTime.get(), reverseCount, reverseUserCount);
                            }
                            successProcess = true;  // 如果执行成功，标记为成功
                        } catch (Exception e) {
                            attempt++;  // 重试计数器递增
                            log.error("翻倍计划-补单等待延迟任务完成时出现中断异常 平台用户:{}, 游戏:{},期数:{},方案:{}", username, lottery, drawNumber.get(), "合并型方案", e);
                            if (attempt >= retryCount) {
                                Thread.currentThread().interrupt();
                                log.error("翻倍计划-补单重试次数已达到上限，平台用户:{}, 游戏:{},期数:{},方案:{}", username, lottery, drawNumber.get(), "合并型方案");
                            }
                        } finally {
                            log.info("翻倍计划-平台用户:{}, 游戏:{},方案:{},期数:{},花费:{}毫秒", username, lottery, "合并型方案", drawNumber.get(), timerProcess.interval());
                            // 确保锁释放
                            // todo 防止释放锁后，同一期任务再次进来时因为没有锁导致同一个方案重复下单，暂时先只使用tryLock的超时时间自动释放看看效果如何
                            //if (lock.isHeldByCurrentThread()) {
                            //    lock.unlock();
                            //}
                        }
                    }

                    if (successCount.get() > 0 || reverseCount.get() > 0) {
                        // 如果发生过投注，那么异步刷新一下流水
                        ThreadUtil.execAsync(() -> {
                            handicapApi.summaryToday(username, null, null);
                        });
                    }

                    long drawTimeDelay = drawTime.get() - System.currentTimeMillis();
                    // 计算距离开奖还是多少时间 ms
                    // 阻塞线程进行延迟时间查询开奖信息,加10秒是确保这时已经开奖了
                    try {
                        log.info("翻倍计划-判断投注中号结果延迟, 平台用户:{}, 游戏:{},方案:{},期数:{}, 需等待:{}毫秒", username, lottery, "合并型方案", drawNumber.get(), drawTimeDelay + 10000);
                        if (drawTimeDelay > 0) {
                            Thread.sleep(drawTimeDelay + 10000);  // 异步延迟
                        }
                    } catch (InterruptedException e) {
                        log.error("翻倍计划-判断投注中号结果延迟异常, 平台用户:{}, 游戏:{},方案:{},期数:{}", username, lottery, "合并型方案", drawNumber.get());
                        throw new RuntimeException(e);
                    }

                    for (PlanDoubleVO plan : enabledPlanDoubles) {
                        List<JSONObject> finalMonitorInfos = useMonitorInfos.get(plan.getId());
                        // 投注和补单结束后进行判断投注中号结果，进而通知下一期投注的金额
                        for (int i = finalMonitorInfos.size() - 1; i >= 0; i--) {// 改为从后往前遍历以避免删除元素导致的索引问题,进而出现中号删除后金额不对的问题
                            JSONObject monitorInfo = JSONUtil.parseObj(finalMonitorInfos.get(i));
                            log.info("翻倍计划, 平台用户:{}, 游戏:{},方案:{},期数:{},当前监控期数信息:{}", username, lottery, "合并型方案", drawNumber.get(), monitorInfo);
                            String orderPositiveKey = KeyUtil.genKey(
                                    RedisConstants.USER_BET_ORDER_DOUBLE_PREFIX,
                                    lottery,
                                    username,
                                    String.valueOf(plan.getId()),
                                    "positive",
                                    monitorInfo.getStr("id")
                            );
                            String orderReverseKey = KeyUtil.genKey(
                                    RedisConstants.USER_BET_ORDER_DOUBLE_PREFIX,
                                    lottery,
                                    username,
                                    String.valueOf(plan.getId()),
                                    "reverse",
                                    monitorInfo.getStr("id")
                            );
                            winCheck(username, usePositiveAccounts.get(plan.getId()), plan, drawNumber.get(), monitorInfo.getStr("id"), i, orderPositiveKey, orderReverseKey);
                        }
                    }

                }
                log.info("翻倍计划-批量下注完成,平台用户:{}, 成功次数: {}, 失败次数: {}, 反补次数: {}, 花费:{}毫秒", username, successUserCount.get(), failureUserCount.get(), reverseUserCount.get(), timerFutures.interval());
                return null;
            }, executorUserService);
            futures.add(future);
        }
        // 等待所有平台用户的任务完成
        CompletableFuture<Void> allTasks = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            allTasks.get(180, TimeUnit.SECONDS); // 等待所有任务完成，设置 60 秒超时
        } catch (Exception e) {
            log.error("翻倍计划-执行任务时出现异常", e);
        }

        logThreadPoolStatus((ThreadPoolExecutor) executorUserService, "翻倍计划-用户线程池");
        // 执行完所有任务后关闭线程池
        PriorityTaskExecutor.shutdownExecutor(executorUserService);
        // 打印批量登录的成功和失败次数
        log.info("翻倍计划-当前任务批量下注完成, 开启下注用户数:{}, 总方案数:{}, 总登录账号数:{}, 应下注总数:{}, 成功次数: {}, 失败次数: {}, 反补次数: {}, 总花费:{}毫秒",
                adminUsers.size(),
                totalTasks.get(),
                totalAccounts.get(),
                totalBets.get(),
                successCount.get(),
                failureCount.get(),
                reverseCount.get(),
                timerTotal.interval());
    }

    /**
     * id字符串转换成idList int
     * @param ids ids
     * @return
     */
    private static List<Integer> formatIds(String ids) {
        if (ids == null) {
            return null;
        }
        List<Integer> id = new ArrayList<>();
        String[] strings = ids.split(",");
        for (String string : strings) {
            id.add(Integer.parseInt(string.trim()));
        }
        return id;
    }

    /**
     * id字符串转换成idList String
     * @param ids ids
     * @return
     */
    private static List<String> formatIdsStr(String ids) {
        if (ids == null) {
            return null;
        }
        List<String> id = new ArrayList<>();
        String[] strings = ids.split(",");
        for (String string : strings) {
            id.add(string.trim());
        }
        return id;
    }

    /**
     * 根据开奖信息获取冷热号码
     * 根据指定下标（1-based，下标为5则表示取 balls 数组中索引 4 的元素），
     * 从开奖记录中依次收集首次出现的号码，直到收集到 10 个唯一号码为止。
     *
     * @param records 已经解析为 List<JSONObject> 的开奖记录列表，列表顺序从最新到最旧
     * @param specifiedIndex 指定下标（例如 5 表示取第 5 个位置的号码）
     * @return 包含 10 个唯一号码的 List，如果记录不足则返回所有收集到的号码
     */
    public static List<String> getCoolHotNumbersAtIndex(JSONArray records, int specifiedIndex) {
        // 转换为 0-based 下标
        int columnIndex = specifiedIndex - 1;
        // 使用 LinkedHashSet 保持插入顺序，便于按照最新记录先收集
        Set<String> uniqueNumbers = new LinkedHashSet<>();

        // 遍历开奖记录（假设 records 中的第 0 条是最新）
        for (Object record : records) {
            JSONArray balls = JSONUtil.parseObj(record).getJSONArray("balls");
            if (balls != null && columnIndex < balls.size()) {
                String num = balls.getStr(columnIndex);
                if (!uniqueNumbers.contains(num)) {
                    uniqueNumbers.add(num);
                    // 如果已收集到 10 个唯一号码，则退出循环
                    if (uniqueNumbers.size() >= 10) {
                        break;
                    }
                }
            }
        }

        return new ArrayList<>(uniqueNumbers);
    }

    /**
     * 执行下单
     *
     * @param username
     * @param plan
     * @param drawNumber
     * @param oddsJson
     * @param successAccounts
     * @param failedAccounts
     * @param positions
     * @param amountJson
     * @param twoSidedAmountJson
     * @param successCount
     * @param failureCount
     * @param successUserCount
     * @param failureUserCount
     */
    private void retryBet(String username, PlanDoubleVO plan, Map<String, OrderVO> finalOrder, String monitorId, JSONObject monitorInfo, String drawNumber, long closeTime, Long drawTime,
                          JSONObject oddsJson, List<UserConfig> allAccounts, List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts, List<UserConfig> successAccounts, List<UserConfig> failedAccounts, Map<String, Map<Integer, String>> useMonitorIds, Map<String, Map<Integer, String>> failedMonitorIds, List<PositionVO> positions, JSONObject amountJson, JSONObject twoSidedAmountJson,
                          AtomicInteger successCount, AtomicInteger failureCount, AtomicInteger successUserCount, AtomicInteger failureUserCount, List<Integer> amountKeys, ExecutorService executorBetService) {

        log.info("翻倍计划-开始执行投注, 平台用户:{},游戏:{},期数:{},方案:{}, 监控ID: {}, 监控信息: {}, 执行用户: {}", username, plan.getLottery(), drawNumber, plan.getName(), monitorId, monitorInfo, allAccounts);
        // 计算新的投注金额
        List<Integer> positiveAmounts = formatIds(plan.getPositiveAmount());
        List<Integer> reverseAmounts = formatIds(plan.getReverseAmount());
        JSONObject newPositiveAmountJson = distributeAmountsForAccounts(positiveAccounts, positiveAmounts.get(monitorInfo.getInt("rounds")));
        JSONObject newReverseAmountJson = distributeAmountsForAccounts(reverseAccounts, reverseAmounts.get(monitorInfo.getInt("rounds")));

        // 在遍历下注操作时，使用线程池异步执行每个下注操作
        List<CompletableFuture<Void>> betFutures = new ArrayList<>();
        for (UserConfig userConfig : allAccounts) {
            betFutures.add(CompletableFuture.runAsync(() -> {
                String account = userConfig.getAccount();
                log.info("翻倍计划-开始执行投注, 执行用户: {}", account);
                int retryCount = 3;
                int attempt = 0;
                boolean successBet = false;

                // 投注前获取当前盘口账号的赔率信息, 这样做的目的是防止同一批下注的账号中会有不同盘口的，不同盘口赔率不一致就会导致投注失败
                String odds = null;
                int maxRetry = 3;
                int retry = 0;
                while (retry < maxRetry) {
                    try {
                        odds = handicapApi.getOddsFromCache(username, drawNumber, userConfig.getId(), plan.getLottery(), plan.getName());
                        if (StringUtils.isNotBlank(odds)) {
                            break;
                        }
                    } catch (Exception e) {
                        log.info("翻倍计划-获取赔率异常 第{}次尝试 平台用户:{},游戏:{},期数:{},方案:{}", retry + 1, username, plan.getLottery(), drawNumber, plan.getName());
                    }
                    retry++;
                    try {
                        Thread.sleep(200); // 可选，稍微等一下再重试
                    } catch (InterruptedException ignored) {}
                }

                JSONObject oddsJsonNew;
                if (StringUtils.isBlank(odds)) {
                    log.info("翻倍计划-重试{}次后仍无法获取最新赔率，使用传入的赔率 平台用户:{},游戏:{},期数:{},方案:{}", maxRetry, username, plan.getLottery(), drawNumber, plan.getName());
                    oddsJsonNew = oddsJson; // 使用传入的赔率
                } else {
                    oddsJsonNew = JSONUtil.parseObj(odds);
                }

                // 内层也换成线程安全集合
                useMonitorIds.computeIfAbsent(userConfig.getAccount(), k -> new ConcurrentHashMap<>())
                        .put(plan.getId(), monitorId);

                log.info("翻倍计划-合并下单,put,useMonitorIds:{}", useMonitorIds);

                if (oddsJsonNew == null || oddsJsonNew.isEmpty()) {
                    log.info("最终无法获取最新赔率 平台用户:{},游戏:{},期数:{},方案:{}", username, plan.getLottery(), drawNumber, plan.getName());
                    failedAccounts.add(userConfig);
                    failedMonitorIds.computeIfAbsent(userConfig.getAccount(), k -> new ConcurrentHashMap<>())
                            .put(plan.getId(), monitorId);
                    failureCount.incrementAndGet();
                    failureUserCount.incrementAndGet();
                    return;
                }

                // while (attempt < retryCount && !successBet) {
                    try {
                        String isBetRedisKey = KeyUtil.genKey(
                                RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                                plan.getLottery(),
                                ToDayRangeUtil.getToDayRange(),
                                drawNumber,
                                "success",
                                username,
                                userConfig.getAccount(),
                                String.valueOf(plan.getId()),
                                monitorId
                        );
                        if (1 == plan.getDoubleType() || 2 == plan.getDoubleType()) {
                            // 判断是否已下注
                            JSONObject exists = JSONUtil.parseObj(redisson.getBucket(isBetRedisKey).get());
                            if (!exists.isEmpty() && exists.getInt("type") == 2) {
                                log.info("翻倍计划-平台用户:{},游戏:{},期数:{},账号:{},方案:{},已下过注,不再重复下注", username, plan.getLottery(), drawNumber, userConfig.getAccount()
                                        , plan.getName());
                                return;
                            }
                        }

                        String betTypeStr = userConfig.getBetType() == 1 ? "positive" : "reverse";

                        // 计算延迟时间
                        // long delay = calculateRemainingTime(closeTime, userConfig.getCloseTime());
                        // delay = Math.max(delay, 0); // 确保延迟时间非负数

                        // log.info("平台用户:{},游戏:{},期数:{},封盘时间:{},账号:{},方案:{}, 及时延迟下注", username, plan.getLottery(), drawNumber, closeTime, userConfig.getAccount(), plan.getName());
                        // 延迟下注任务
                        // scheduledExecutorService.schedule(() -> {
                        String orderKey = KeyUtil.genKey(
                                RedisConstants.USER_BET_ORDER_DOUBLE_PREFIX,
                                plan.getLottery(),
                                username,
                                String.valueOf(plan.getId()),
                                betTypeStr,
                                userConfig.getAccount(), // 新增这一项，确保账号维度隔离
                                monitorInfo.getStr("id")
                        );
                        // 创建下注请求参数
                        OrderVO order = new OrderVO();
                        if (1 == plan.getDoubleType()) {
                            if (plan.getMonitorNum() <= 1) {
                                order = createOrder(username, monitorId, plan, oddsJsonNew, drawNumber, betTypeStr, amountJson, twoSidedAmountJson, userConfig, amountKeys);
                            } else {
                                Object objRedis = businessUserRedissonClient.getBucket(orderKey).get();
                                if (Objects.isNull(objRedis)) {
                                    order = createOrder(username, monitorId, plan, oddsJsonNew, drawNumber, betTypeStr, amountJson, twoSidedAmountJson, userConfig, amountKeys);
                                    businessUserRedissonClient.getBucket(orderKey).set(JSONUtil.parseObj(order));
                                } else {
                                    log.info("翻倍老号-开始获取老号的下单记录, 执行用户: {}, 老号下单记录: {}", userConfig.getAccount(), objRedis);
                                    // 获取老号的下单记录
                                    order = JSONUtil.toBean(String.valueOf(objRedis), OrderVO.class);
                                    // 更新期数和金额
                                    order.setDrawNumber(drawNumber);
                                    for (Bet bet : order.getBets()) {
                                        if (StringUtils.equals("positive", betTypeStr)) {
                                            bet.setAmount(newPositiveAmountJson.getInt(userConfig.getAccount()));
                                        } else {
                                            bet.setAmount(newReverseAmountJson.getInt(userConfig.getAccount()));
                                        }
                                    }
                                }
                            }
                        } else if (2 == plan.getDoubleType()) {
                            order = createOrder(username, monitorId, plan, oddsJsonNew, drawNumber, betTypeStr, amountJson, twoSidedAmountJson, userConfig, amountKeys);
                        } else if (3 == plan.getDoubleType()) {
                            if (plan.getMonitorNum() <= 1) {
                                order = createOrder(username, monitorId, plan, oddsJsonNew, drawNumber, betTypeStr, amountJson, twoSidedAmountJson, userConfig, amountKeys);
                            } else {
                                Object objRedis = businessUserRedissonClient.getBucket(orderKey).get();
                                if (Objects.isNull(objRedis)) {
                                    order = createOrder(username, monitorId, plan, oddsJsonNew, drawNumber, betTypeStr, amountJson, twoSidedAmountJson, userConfig, amountKeys);
                                    businessUserRedissonClient.getBucket(orderKey).set(JSONUtil.parseObj(order));
                                } else {
                                    log.info("翻老投新-开始获取老号的下单记录, 执行用户: {}, 老号下单记录: {}", userConfig.getAccount(), objRedis);
                                    // 获取翻老投新的下单记录
                                    order = JSONUtil.toBean(String.valueOf(objRedis), OrderVO.class);
                                    // 更新期数和金额
                                    order.setDrawNumber(drawNumber);
                                    for (Bet bet : order.getBets()) {
                                        if (StringUtils.equals("positive", betTypeStr)) {
                                            bet.setAmount(newPositiveAmountJson.getInt(userConfig.getAccount()));
                                        } else {
                                            bet.setAmount(newReverseAmountJson.getInt(userConfig.getAccount()));
                                        }
                                    }
                                }
                            }
                        }
                        if (order.getBets().isEmpty()) {
                            log.info("翻倍计划-平台用户:{},游戏:{},期数:{},账号:{},方案:{},没有可下注的号码", username, plan.getLottery(), drawNumber, userConfig.getAccount(), plan.getName());
                            return;
                        }
                        log.info("翻倍计划-平台用户:{},游戏:{},期数:{},账号:{},方案:{},投注请求:{},将投注请求插入到finalOrder中,插入前:{}", username, plan.getLottery(), drawNumber, userConfig.getAccount(), plan.getName(), JSONUtil.parseObj(order), finalOrder);
                        // 安全获取或创建OrderVO
                        OrderVO orderCopy = order;
                        finalOrder.compute(account, (k, existingOrder) -> {
                            if (existingOrder == null) {
                                return orderCopy;
                            } else {
                                existingOrder.getBets().addAll(orderCopy.getBets());
                                return existingOrder;
                            }
                        });
                        log.info("翻倍计划-平台用户:{},游戏:{},期数:{},账号:{},方案:{},投注请求:{},将投注请求插入到finalOrder中,插入后:{}", username, plan.getLottery(), drawNumber, userConfig.getAccount(), plan.getName(), JSONUtil.parseObj(order), finalOrder);

                        // 记录订单请求
                        redisson.getBucket(KeyUtil.genKey(
                                RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                                plan.getLottery(),
                                ToDayRangeUtil.getToDayRange(),
                                drawNumber,
                                username,
                                String.valueOf(plan.getId()),
                                monitorId,
                                userConfig.getAccount()
                        )).set(JSONUtil.toJsonStr(order), Duration.ofHours(24));

                        // 提交下注
                        // String result = submitOrder(username, order, userConfig, plan, drawNumber, monitorId, closeTime);
                        // 结果解析
                        // handleOrderResult(username, result, userConfig, drawNumber, plan, successAccounts, failedAccounts, failedMonitorIds, successCount, failureCount, successUserCount, failureUserCount, monitorId);

                        successBet = true; // 正常执行成功，设置为 true，退出循环
                        // latch.countDown();
                        // }, delay, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        attempt++;  // 重试计数器递增
                        log.error("翻倍计划-处理账户时发生异常,平台用户:{},账号:{}", username, userConfig.getAccount(), e);
                        failedAccounts.add(userConfig);
                        failedMonitorIds.computeIfAbsent(userConfig.getAccount(), k -> new ConcurrentHashMap<>())
                                .put(plan.getId(), monitorId);
                        failureCount.incrementAndGet();
                        failureUserCount.incrementAndGet();
                        /*if (attempt >= retryCount) {
                            Thread.currentThread().interrupt();
                            log.error("翻倍计划-下单重试次数已达到上限，平台用户:{}, 游戏:{},期数:{},方案:{}", username, plan.getLottery(), drawNumber, plan.getName());
                        }*/
                    }
                // }
            }, executorBetService));
        }

        // 等待所有下注操作完成
        CompletableFuture<Void> allBets = CompletableFuture.allOf(betFutures.toArray(new CompletableFuture[0]));
        try {
            allBets.get();  // 等待所有下注操作完成
        } catch (Exception e) {
            log.error("翻倍计划-执行下注操作时发生异常", e);
        }
        log.info("外层打印useMonitorIds: {}", useMonitorIds);

        logThreadPoolStatus((ThreadPoolExecutor) executorBetService, "翻倍计划-下注线程池");
    }

    /**
     * 校验投注中号结果
     * @param username
     * @param positiveAccounts
     * @param plan
     * @param drawNumber
     * @param monitorId
     * @param monitorInfoIndex
     * @param orderPositiveKey
     */
    private void winCheck(String username, List<UserConfig> positiveAccounts, PlanDoubleVO plan, String drawNumber, String monitorId, int monitorInfoIndex, String orderPositiveKey, String orderReverseKey) {

        Map<String, List<String>> gameContentsMap = new HashMap<>();
        List<String> balls = new ArrayList<>();
        for (UserConfig userConfig : positiveAccounts) {
            // 获取指定期数开奖信息
            String drawKey = KeyUtil.genKey(
                    RedisConstants.DRAW_RESULT_PREFIX,
                    plan.getLottery(),
                    drawNumber
            );
            JSONObject dresult = JSONUtil.parseObj(businessUserRedissonClient.getBucket(drawKey).get());
            if (dresult == null || dresult.isEmpty()) {
                dresult = handicapApi.dresult(username, userConfig.getId(), plan.getLottery(), drawNumber);
            } else {
                if (dresult.getJSONArray("balls").isEmpty() && dresult.getJSONArray("dragonTiger").isEmpty()) {
                    dresult = handicapApi.dresult(username, userConfig.getId(), plan.getLottery(), drawNumber);
                }
            }
            if (dresult == null || dresult.isEmpty() || dresult.getJSONArray("balls").isEmpty() || dresult.getJSONArray("dragonTiger").isEmpty()) {
                log.info("翻倍计划-获取开奖信息失败-平台用户:{},账号:{}, 期数:{}, 方案:{}, 开奖信息{}", username, userConfig.getAccount(), drawNumber, plan.getName(), dresult);
                return;
            }
            log.info("翻倍计划-获取开奖信息-平台用户:{},账号:{}, 期数:{}, 方案:{}, 开奖信息{}", username, userConfig.getAccount(), drawNumber, plan.getName(), dresult);
            // 每期开奖信息保存10分钟即可
            businessUserRedissonClient.getBucket(drawKey).set(dresult, Duration.ofMinutes(10));
            // 解析开奖单号
            balls = dresult.getJSONArray("balls").toList(String.class);
            String orderReqKey = KeyUtil.genKey(
                    RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                    plan.getLottery(),
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    username,
                    String.valueOf(plan.getId()),
                    monitorId,
                    userConfig.getAccount()
            );
            JSONObject orderReq = JSONUtil.parseObj(redisson.getBucket(orderReqKey).get());
            if (orderReq.isEmpty()) {
                log.info("翻倍计划-校验中号结果-获取订单请求为空-平台用户:{},账号:{}, 期数:{}, 方案:{}", username, userConfig.getAccount(), drawNumber, plan.getName());
                return;
            }
            JSONArray betsArray = orderReq.getJSONArray("bets");
            // 遍历 bets 数组
            for (int i = 0; i < betsArray.size(); i++) {
                JSONObject bet = betsArray.getJSONObject(i);
                String game = bet.getStr("game");
                String contents = bet.getStr("contents");
                // 存入 map
                gameContentsMap.computeIfAbsent(game, k -> new ArrayList<>()).add(contents);
            }
        }
        log.info("翻倍计划-校验中号结果-平台用户:{}, 期数:{}, 方案:{}, 开奖位置信息:{}, 投注位置:{}", username, drawNumber, plan.getName(), balls, gameContentsMap);

        // ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(10);
        // scheduler.schedule(() -> {
        AtomicInteger winNum = new AtomicInteger();

        // 获取平台账户当前是否开启自动投注
        int autoBet = configService.getAutoBet(username);
        // 获取当前方案是否开启投注
        String key = KeyUtil.genKey(RedisConstants.USER_PLAN_DOUBLE_PREFIX, username, plan.getLottery(), String.valueOf(plan.getId()));
        String configJson = (String) businessUserRedissonClient.getBucket(key).get();
        PlanDoubleVO configPlan = null;
        if (configJson != null) {
            // 将 JSON 转换为对象
            configPlan = JSONUtil.toBean(configJson, PlanDoubleVO.class);
        }
        // 如果当前已经关闭投注，那么直接清空监控期数信息
        if (autoBet == 0 || configJson == null || configPlan.getEnable() == 0) {
            configService.clearMonitor(username, plan.getLottery(), String.valueOf(plan.getId()));
            return;
        }
        if (CollUtil.isNotEmpty(balls) && CollUtil.isNotEmpty(gameContentsMap)) {
            List<String> finalBalls = balls;
            // 遍历下单的位置，然后与开奖信息位置对比是否投中
            gameContentsMap.forEach((game, contents) -> {
                String content = finalBalls.get(Integer.parseInt(game.replaceAll("B", "")) - 1);
                if (contents.contains(content)) {
                    // 当前投注中了
                    winNum.getAndIncrement();
                }
            });
            log.info("翻倍计划-校验中号结果-平台用户:{}, 期数:{}, 方案:{}, 实际中号数:{}", username, drawNumber, plan.getName(), winNum.get());

            boolean isAchieved;  // 判断是否达标
            if (1 == plan.getSettleType()) {
                // 不中翻倍
                isAchieved = winNum.get() >= plan.getWinNum();  // 判断是否达标
            } else {
                // 中翻倍
                isAchieved = winNum.get() < plan.getWinNum();  // 判断是否达标
            }

            String monitorKey = KeyUtil.genKey(
                    RedisConstants.USER_MONITOR_PREFIX,
                    username,
                    String.valueOf(plan.getId())
            );
            List<JSONObject> monitorInfos = businessUserRedissonClient.getList(monitorKey);
            JSONObject monitorInfo = new JSONObject();
            if (monitorInfos.isEmpty()) {
                log.info("翻倍计划-校验中号结果-平台用户:{}, 期数:{}, 方案:{}, 实际中号数:{}, 第1次投注isAchieved:{}, orderKey:{}, 监控期数id:{}", username, drawNumber, plan.getName(), winNum.get(), isAchieved, orderPositiveKey, monitorId);
                // 如果监控信息不存在并且不满足中号条件，则创建新的监控信息
                if (!isAchieved) {
                    monitorInfo.putOpt("id", monitorId);     // 期数ID
                    monitorInfo.putOpt("planId", plan.getId());     // 计划ID
                    monitorInfo.putOpt("drawNumber", drawNumber);   // 期数
                    monitorInfo.putOpt("achieve", isAchieved);      // 是否达到中号次数
                    monitorInfo.putOpt("createTime", LocalDateTimeUtil.formatNormal(LocalDateTime.now()));
                    if (1 == plan.getDoubleType()) {
                        // 翻倍老号
                        monitorInfo.putOpt("rounds", plan.getMonitorNum() <= 1 ? 0 : 1);                // 如果监控期数是1，那么直接回到第一期,如果监控期数大于1，则监控轮次从第二期开始
                    } else if (2 == plan.getDoubleType()) {
                        // 翻倍新号
                        monitorInfo.putOpt("rounds", 0);                // 监控轮次从第一期开始
                    } else if (3 == plan.getDoubleType()) {
                        // 翻老投新
                        monitorInfo.putOpt("rounds", plan.getMonitorNum() <= 1 ? 0 : 1);                // 如果监控期数是1，那么直接回到第一期,如果监控期数大于1，则监控轮次从第二期开始
                    }
                    monitorInfos.add(monitorInfo);
                }
            } else {
                monitorInfo = JSONUtil.parseObj(monitorInfos.get(monitorInfoIndex));
                monitorInfo.putOpt("planId", plan.getId());     // 计划ID
                monitorInfo.putOpt("drawNumber", drawNumber);   // 期数
                monitorInfo.putOpt("achieve", isAchieved);      // 是否达到中号次数
                monitorInfo.putOpt("createTime", LocalDateTimeUtil.formatNormal(LocalDateTime.now()));
                log.info("翻倍计划-校验中号结果-平台用户:{}, 期数:{}, 方案:{}, 实际中号数:{}, 第N次投注isAchieved:{}, orderKey:{}, 翻倍类型:{}, 当前监控期数数据:{}, 监控期数id:{}", username, drawNumber, plan.getName(), winNum.get(), isAchieved, orderPositiveKey, plan.getDoubleType(), monitorInfo, monitorId);
                if (1 == plan.getDoubleType()) {
                    if (isAchieved) {
                        // 当前投注中号次数已经达到计划配置的中号次数，进行重新期数记录
                        monitorInfo.putOpt("rounds", 0);                            // 监控轮次返回第一期
                        businessUserRedissonClient.getBucket(orderPositiveKey).delete();    // 删除老订单正反投信息
                        businessUserRedissonClient.getBucket(orderReverseKey).delete();    // 删除老订单正反投信息
                    } else {
                        // 当前投注中号次数未达到计划配置的中号次数，翻倍投注
                        if (Objects.equals(monitorInfo.getInt("rounds"), plan.getMonitorNum() - 1)) {
                            // 如果监控轮次达到计划配置的轮次，则重新返回第一期投注
                            monitorInfo.putOpt("rounds", 0);
                            businessUserRedissonClient.getBucket(orderPositiveKey).delete();    // 删除老订单正反投信息
                            businessUserRedissonClient.getBucket(orderReverseKey).delete();    // 删除老订单正反投信息
                        } else {
                            monitorInfo.putOpt("rounds", monitorInfo.getInt("rounds") + 1);     // 监控期数加1
                        }
                    }
                    monitorInfos.set(monitorInfoIndex, monitorInfo);
                } else if (2 == plan.getDoubleType()) {
                    if (isAchieved) {
                        // 当前投注中号次数已经达到计划配置的中号次数，进行重新期数记录
                        monitorInfo.putOpt("rounds", 0);            // 监控轮次返回第一期
                    } else {
                        // 当前投注中号次数未达到计划配置的中号次数，翻倍投注
                        if (Objects.equals(monitorInfo.getInt("rounds"), plan.getMonitorNum() - 1)) {
                            // 如果监控轮次达到计划配置的轮次，则重新返回第一期投注
                            monitorInfo.putOpt("rounds", 0);
                        } else {
                            monitorInfo.putOpt("rounds", monitorInfo.getInt("rounds") + 1);     // 监控期数加1
                        }
                    }
                    monitorInfos.set(monitorInfoIndex, monitorInfo);
                } else if (3 == plan.getDoubleType()) {
                    if (isAchieved) {
                        // 当前投注中号次数已经达到计划配置的中号次数，结束老号翻倍
                        monitorInfos.remove(monitorInfoIndex);                      // 移除当前监控信息
                        businessUserRedissonClient.getBucket(orderPositiveKey).delete();    // 删除老订单正反投信息
                        businessUserRedissonClient.getBucket(orderReverseKey).delete();    // 删除老订单正反投信息
                    } else {
                        // 当前投注中号次数未达到计划配置的中号次数，翻倍投注
                        if (Objects.equals(monitorInfo.getInt("rounds"), plan.getMonitorNum() - 1)) {
                            // 如果监控轮次达到计划配置的轮次，结束老号翻倍
                            monitorInfos.remove(monitorInfoIndex);                      // 移除当前监控信息
                            businessUserRedissonClient.getBucket(orderPositiveKey).delete();    // 删除老订单正反投信息
                            businessUserRedissonClient.getBucket(orderReverseKey).delete();    // 删除老订单正反投信息
                        } else {
                            monitorInfo.putOpt("rounds", monitorInfo.getInt("rounds") + 1);     // 监控期数加1
                            // 修改老号翻倍监控数据
                            monitorInfos.set(monitorInfoIndex, monitorInfo);
                        }
                    }
                }
            }
        } else {
            log.info("翻倍计划-平台用户:{}, 期数:{}, 方案:{}, 已结订单信息没有匹配到任何注单号", username, drawNumber, plan.getName());
        }
        // },drawTimeDelay + 5000, TimeUnit.MILLISECONDS);// 延迟时间查询订单,加5秒是确保这时订单已经生成了
    }

    /**
     * 存储最新期数数据
     * @param lottery
     * @param period
     */
    private void savePeriodLatest(String lottery, String period) {
        JSONObject periodJson = JSONUtil.parseObj(period);
        long closeTime = periodJson.getLong("closeTime");
        long currentTime = System.currentTimeMillis();
        long betTime = closeTime - currentTime;
        businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_BET_SKIP_PREFIX,lottery)).set(periodJson, Duration.ofMillis(betTime));
    }

    /**
     * 校验封盘时间
     * @param periodJson
     * @param username
     * @param lottery
     * @param drawNumber
     * @return
     */
    private boolean validateCloseTime(JSONObject periodJson, String username, String lottery, String drawNumber) {
        long closeTime = periodJson.getLong("closeTime") / 1000;
        long currentTime = System.currentTimeMillis() / 1000;
        long betTime = closeTime - currentTime;
        if (betTime < 15) {
            log.info("翻倍计划-平台用户:{}, 游戏:{}, 期数:{}, 剩余封盘时间:{}, 即将封盘, 不进行下注", username, lottery, drawNumber, betTime);
            return false;
        }
        return true;
    }

    /**
     * 尝试获取期数锁
     * @param plan
     * @param username
     * @param drawNumber
     * @return
     */
    private RLock acquireBetLock(JSONObject periodJson, String username, PlanDoubleVO plan, String drawNumber) {
        long closeTime = periodJson.getLong("closeTime") / 1000;
        long currentTime = System.currentTimeMillis() / 1000;
        long betTime = closeTime - currentTime;
        RLock lock = businessUserRedissonClient.getLock(KeyUtil.genKey(RedisConstants.USER_BET_DOUBLE_PERIOD_PREFIX, plan.getLottery(), drawNumber, username, String.valueOf(plan.getId())));
        try {
            // 5秒内尝试获取锁，持锁时间到封盘时间+10秒
            if (lock.tryLock(5, betTime + 10, TimeUnit.SECONDS)) {
                return lock;
            }
        } catch (InterruptedException e) {
            log.info("翻倍计划-平台用户:{}, 游戏:{}, 期数:{}, 方案:{}, 获取锁失败", username, plan.getLottery(), drawNumber, plan.getName());
        }
        return null;
    }

    /**
     * 获取已启用的方案
     * @param username
     * @return
     */
    private CompletableFuture<List<PlanDoubleVO>> getEnabledConfigPlansAsync(String username) {
        return CompletableFuture.supplyAsync(() -> {
            List<PlanDoubleVO> plans = configService.getAllPlanDoublesRBatch(username, null);
            if (CollUtil.isEmpty(plans)) {
                return Collections.emptyList();
            }
            return plans.stream()
                    .filter(config -> config.getEnable() != 0)
                    .collect(Collectors.toList());
        });
    }

    /**
     * 获取token有效的账号
     * @param username
     * @return
     */
    private CompletableFuture<List<UserConfig>> getTokenValidConfigsAsync(String username) {
        return CompletableFuture.supplyAsync(() -> {
            List<UserConfig> userConfigs = configService.accountsRBatch(username, null);
            if (CollUtil.isEmpty(userConfigs)) {
                return Collections.emptyList();
            }
            return userConfigs.stream()
                    .filter(userConfig -> 1 == userConfig.getIsTokenValid() && StringUtils.isNotBlank(userConfig.getToken()))
                    .collect(Collectors.toList());
        });
    }

    /**
     * 校验list中正反投账号是否都存在
     * 如果缺少一种投注账户则关闭自动下注功能
     * @param userConfigs
     * @return
     */
    private CompletableFuture<List<UserConfig>> validUserBetTypeAsync(AdminLoginDTO admin, List<UserConfig> userConfigs) {
        return CompletableFuture.supplyAsync(() -> {
            long positiveCount = userConfigs.stream().filter(userConfig -> userConfig.getBetType() == 1).count();
            long reverseCount = userConfigs.stream().filter(userConfig -> userConfig.getBetType() == 2).count();
            if (positiveCount < 1 || reverseCount < 1) {
                admin.setAutoBet(0);
                businessUserRedissonClient.getBucket(KeyUtil.genKey(
                        RedisConstants.USER_ADMIN_PREFIX,
                        admin.getUsername()
                )).set(JSONUtil.toJsonStr(admin));
                return Collections.emptyList();
            }
            return userConfigs;
        });
    }

    /**
     * 获取双面分配金额
     * @param oddsJson
     * @param twoSidedDxPositions
     * @param twoSidedDsPositions
     * @param twoSidedLhPositions
     * @param positiveAccounts
     * @param reverseAccounts
     * @param plan
     * @return
     */
    private JSONObject twoSidedAmountJson(JSONObject oddsJson, Map<String, List<Integer>> twoSidedDxPositions, Map<String, List<Integer>> twoSidedDsPositions, Map<String, List<Integer>> twoSidedLhPositions, List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts, PlanDoubleVO plan, Integer rounds) {
        JSONObject twoSidedAmountJson = null;
        if (CollUtil.isNotEmpty(twoSidedDxPositions) || CollUtil.isNotEmpty(twoSidedDsPositions) || CollUtil.isNotEmpty(twoSidedLhPositions)) {
            // 获取双面-大小正反投的位置 key
            Map<String, List<String>> twoSidedDxOdds = new HashMap<>();
            // 获取双面-单双正反投的位置 key
            Map<String, List<String>> twoSidedDsOdds = new HashMap<>();
            // 获取双面-龙虎正反投的位置 key
            Map<String, List<String>> twoSidedLhOdds = new HashMap<>();

            if (null != twoSidedDxPositions) {
                twoSidedDxPositions.forEach((key, value) -> {
                    for (int pos : value) {
                        String jsonkey = "DX" + pos;
                        List<String> matchedKeys = oddsJson.keySet().stream()
                                .filter(matchedKey -> matchedKey.startsWith(jsonkey + "_"))
                                .toList();
                        twoSidedDxOdds.put(key, matchedKeys);
                    }
                });
            }
            if (null != twoSidedDsPositions) {
                twoSidedDsPositions.forEach((key, value) -> {
                    for (int pos : value) {
                        String jsonkey = "DS" + pos;
                        List<String> matchedKeys = oddsJson.keySet().stream()
                                .filter(matchedKey -> matchedKey.startsWith(jsonkey + "_"))
                                .toList();
                        twoSidedDsOdds.put(key, matchedKeys);
                    }
                });
            }
            if (null != twoSidedLhPositions) {
                twoSidedLhPositions.forEach((key, value) -> {
                    for (int pos : value) {
                        String jsonkey = "LH" + pos;
                        List<String> matchedKeys = oddsJson.keySet().stream()
                                .filter(matchedKey -> matchedKey.startsWith(jsonkey + "_"))
                                .toList();
                        twoSidedLhOdds.put(key, matchedKeys);
                    }
                });
            }

            // 分配大小单双龙虎对应正反投
            Map<String, Map<String, List<String>>> twoSidedMap = assignPositions(positiveAccounts, reverseAccounts, twoSidedDxOdds, twoSidedDsOdds, twoSidedLhOdds);
            // 获取双面金额分配
            twoSidedAmountJson = twoSidedDistributeAmount(twoSidedMap, positiveAccounts, reverseAccounts, plan, rounds);
        }
        return twoSidedAmountJson;
    }

    public static void main(String[] args) {
        System.out.println(System.currentTimeMillis());
        System.out.println(System.currentTimeMillis() / 1000);
        String play = "第五名 7 @ 9.939";
        String gameplay = "第五名 7";
        System.out.println(play.contains(gameplay));
        String ajson = "{\"1\":{\"positive\":{\"B1_1\":{\"dftg22\":1,\"feggr4\":4}},\"reverse\":{\"B1_1\":{\"fgrt34\":3,\"vger2\":2}}},\"3\":{\"positive\":{\"B3_1\":{\"dftg22\":1,\"feggr4\":4}},\"reverse\":{\"B3_1\":{\"fgrt34\":1,\"vger2\":4}}},\"6\":{\"positive\":{\"B6_1\":{\"dftg22\":3,\"feggr4\":2}},\"reverse\":{\"B6_1\":{\"fgrt34\":2,\"vger2\":3}}}}";

        JSONObject inputJson = JSONUtil.parseObj(ajson);
        JSONObject positiveJson = new JSONObject();
        JSONObject reverseJson = new JSONObject();

        // 遍历最外层的 key
        inputJson.forEach((outerKey, outerValue) -> {
            JSONObject group = (JSONObject) outerValue;
            if (group.containsKey("positive")) {
                JSONObject positive = group.getJSONObject("positive");
                positive.forEach(positiveJson::putIfAbsent);
            }
            if (group.containsKey("reverse")) {
                JSONObject reverse = group.getJSONObject("reverse");
                reverse.forEach(reverseJson::putIfAbsent);
            }
        });
        List<String> poss = new ArrayList<>(positiveJson.keySet()); // 存储正投的 key

        // 获取正投的号码 key（如 ["B2_1", "B3_1"]）
        List<String> posNew = new ArrayList<>();
        // 提取 `_` 后面的编号（去重）
        Set<String> suffixes = poss.stream()
                .map(k -> k.split("_")[1]) // 取 `_` 之后的部分
                .collect(Collectors.toSet());

        // 生成可能的反投位置（B1~B10，但排除 `poss` 里的）
        for (int i = 1; i <= 10; i++) {
            String prefix = "B" + i + "_";
            for (String suffix : suffixes) {
                String newKey = prefix + suffix;
                if (!poss.contains(newKey)) { // 确保不在正投
                    posNew.add(newKey);
                }
            }
        }
        System.out.println(posNew);
    }

    /**
     * 封装订单
     * @param plan
     * @param oddsJson
     * @param drawNumber
     * @param oddsType
     * @return
     */
    private OrderVO createOrder(String username, String monitorId, PlanDoubleVO plan, JSONObject oddsJson, String drawNumber, String oddsType, JSONObject amountJson, JSONObject twoSidedAmountJson, UserConfig userConfig, List<Integer> amountKeys) {
        OrderVO order = new OrderVO();
        List<Bet> bets = new ArrayList<>();
        order.setLottery(plan.getLottery());
        order.setDrawNumber(drawNumber);
        order.setFastBets(false);
        order.setIgnore(false);
        log.info("翻倍计划-封装订单,解析横式反投模式, 账号:{}, 期数:{}, 方案:{}, amountKeys:{}, amountJson:{}, oddsJson:{}", userConfig.getAccount(), drawNumber, plan.getName(), amountKeys, amountJson, oddsJson);

        // 正投位置账号金额json
        JSONObject positiveJson = new JSONObject();
        // 反投投位置账号金额json
        JSONObject reverseJson = new JSONObject();
        // 遍历最外层的 key
        amountJson.forEach((outerKey, outerValue) -> {
            JSONObject group = (JSONObject) outerValue;
            if (group.containsKey("positive")) {
                JSONObject positive = group.getJSONObject("positive");
                positive.forEach(positiveJson::putIfAbsent);
            }
            if (group.containsKey("reverse")) {
                JSONObject reverse = group.getJSONObject("reverse");
                reverse.forEach(reverseJson::putIfAbsent);
            }
        });
        log.info("翻倍计划-封装订单,解析横式反投模式, 账号:{}, 期数:{}, 方案:{}, positiveJson:{}, reverseJson:{}", userConfig.getAccount(), drawNumber, plan.getName(), positiveJson, reverseJson);

        if (StringUtils.equals("positive", oddsType) && !positiveJson.isEmpty()) {
            positiveJson.forEach((k, v) -> {
                List<String> oddsKey = oddsKyeSplitter(k);
                JSONObject amountValue = JSONUtil.parseObj(v);
                Bet bet = new Bet();
                bet.setGame(oddsKey.get(0));
                bet.setAmount(amountValue.getLong(userConfig.getAccount()));
                bet.setContents(oddsKey.get(1));
                bet.setOdds(oddsJson.containsKey(k) ? oddsJson.getDouble(k) : 9.966);   // 设置默认赔率
                bets.add(bet);
            });
        }
        if (StringUtils.equals("reverse", oddsType) && !reverseJson.isEmpty()) {
            List<String> poss = new ArrayList<>(positiveJson.keySet()); // 存储正投的 key

            // 获取正投的号码 key（如 ["B2_1", "B3_1"]）
            List<String> posNew = new ArrayList<>();
            if ("1".equals(plan.getReverseType())) { // 竖式
                posNew = new ArrayList<>(reverseJson.keySet());
            } else if ("2".equals(plan.getReverseType())) { // 横式
                // 提取 `_` 后面的编号（去重）
                Set<String> suffixes = poss.stream()
                        .map(k -> k.split("_")[1]) // 取 `_` 之后的部分
                        .collect(Collectors.toSet());

                // 生成可能的反投位置（B1~B10，但排除 `poss` 里的）
                for (int i = 1; i <= 10; i++) {
                    String prefix = "B" + i + "_";
                    for (String suffix : suffixes) {
                        String newKey = prefix + suffix;
                        if (!poss.contains(newKey)) { // 确保不在正投
                            posNew.add(newKey);
                        }
                    }
                }
                log.info("翻倍计划-封装订单,解析横式反投模式, 账号:{}, 期数:{}, 方案:{}, posNew:{}", userConfig.getAccount(), drawNumber, plan.getName(), posNew);
            }

            // todo 反投金额或许需要重新计算
            // 获取 posJson 的第一个值,不同号码的位置
            Object firstValue = reverseJson.values().stream().findFirst().orElse(null);
            log.info("翻倍计划-封装订单,解析横式反投模式, 账号:{}, 期数:{}, 方案:{}, firstValue:{}", userConfig.getAccount(), drawNumber, plan.getName(), firstValue);
            // **重新构造 `posJson`，只保留 `posNew` 中的 key，value 不变**
            JSONObject newPosJson = new JSONObject();
            for (String pos : posNew) {
                newPosJson.putOpt(pos, firstValue);
            }

            newPosJson.forEach((k, v) -> {
                List<String> oddsKey = oddsKyeSplitter(k);
                JSONObject amountValue = JSONUtil.parseObj(v);
                log.info("翻倍计划-封装订单,反投金额, 账号:{}, 期数:{}, 方案:{}, amountValue:{}", userConfig.getAccount(), drawNumber, plan.getName(), amountValue);
                Bet bet = new Bet();
                bet.setGame(oddsKey.get(0));
                bet.setAmount(amountValue.getLong(userConfig.getAccount()));
                bet.setContents(oddsKey.get(1));
                bet.setOdds(oddsJson.containsKey(k) ? oddsJson.getDouble(k) : 9.966);   // 设置默认赔率
                bets.add(bet);
            });
        }

        if (null != twoSidedAmountJson && !twoSidedAmountJson.isEmpty()) {
            // 存在双面配置金额
            if (StringUtils.equals("positive", oddsType)) {
                JSONObject posJson = twoSidedAmountJson.getJSONObject("positive");
                posJson.forEach((k, v) -> {
                    List<String> oddsKey = oddsKyeSplitter(k);
                    JSONObject amountValue = JSONUtil.parseObj(v);
                    Bet bet = new Bet();
                    bet.setGame(oddsKey.get(0));
                    bet.setAmount(amountValue.getLong(userConfig.getAccount()));
                    bet.setContents(oddsKey.get(1));
                    bet.setOdds(oddsJson.containsKey(k) ? oddsJson.getDouble(k) : 9.966);   // 设置默认赔率
                    bets.add(bet);
                });
            } else {
                JSONObject posJson = twoSidedAmountJson.getJSONObject("reverse");
                posJson.forEach((k, v) -> {
                    List<String> oddsKey = oddsKyeSplitter(k);
                    JSONObject amountValue = JSONUtil.parseObj(v);
                    Bet bet = new Bet();
                    bet.setGame(oddsKey.get(0));
                    bet.setAmount(amountValue.getLong(userConfig.getAccount()));
                    bet.setContents(oddsKey.get(1));
                    bet.setOdds(oddsJson.containsKey(k) ? oddsJson.getDouble(k) : 9.966);   // 设置默认赔率
                    bets.add(bet);
                });
            }
        }
        order.setBets(bets);
        return order;
    }

    /**
     * 封装发起提交下注
     * @param order
     * @param userConfig
     * @param plan
     * @param drawNumber
     * @return
     */
    private String submitOrder(String username, OrderVO order, UserConfig userConfig, PlanDoubleVO plan, String drawNumber, String monitorId, long closeTime) {
        log.info("翻倍计划-发起下单, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 账号token:{}, 订单:{}", username, userConfig.getAccount(), drawNumber, plan.getName(), userConfig.getToken(), JSONUtil.parseObj(order));
        TimeInterval timeBet = DateUtil.timer();
        boolean success = true;
        String url = userConfig.getBaseUrl() + "member/bet";
        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("cookie", "defaultLT=" + plan.getLottery() + "; token=" + userConfig.getToken());
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36");
        headers.put("x-requested-with", "XMLHttpRequest");

        // 记录订单请求
        redisson.getBucket(KeyUtil.genKey(
                RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                plan.getLottery(),
                ToDayRangeUtil.getToDayRange(),
                drawNumber,
                username,
                String.valueOf(plan.getId()),
                monitorId,
                userConfig.getAccount()
        )).set(JSONUtil.toJsonStr(order), Duration.ofHours(24));
        String result = null;
        try {
            // ✅ 使用 OkHttpProxyDispatcher 调用，并封装异步 + 超时
            CompletableFuture<OkHttpProxyDispatcher.HttpResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.info("翻倍计划-进行下单-发起下单请求 平台用户:{},游戏:{},期数:{},封盘时间:{},方案:{}", username, plan.getLottery(), drawNumber, LocalDateTimeUtil.of(closeTime), plan.getName());
                    return dispatcher.execute("POST", url, JSONUtil.toJsonStr(order), headers, userConfig, false);
                } catch (IOException e) {
                    // 抛出包装异常，供外层 get() 捕获
                    throw new CompletionException(e);
                }
            });
            OkHttpProxyDispatcher.HttpResult resultHttp = future.get(10, TimeUnit.SECONDS); // 10 秒超时
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
            log.info("翻倍计划-下单结果, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息{}", username, userConfig.getAccount(), drawNumber, plan.getName(), result);

            if (StringUtils.isBlank(result)) {
                log.info("翻倍计划-下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息{}", username, userConfig.getAccount(), drawNumber, plan.getName(), result);
                JSONObject failed = new JSONObject();
                failed.putOpt("status", 1);
                failed.putOpt("lottery", plan.getLottery());
                failed.putOpt("drawNumber", drawNumber);
                failed.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                failed.putOpt("account", userConfig.getAccount());
                failed.putOpt("isRepair", 0);// 是否为补单 0:否 1:是
                failed.putOpt("message", "账号异常,请检查登录状态以及账号是否正常");
                return JSONUtil.toJsonStr(failed);
            } else if (!JSONUtil.isTypeJSONObject(result)) {
                log.info("翻倍计划-下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息{}", username, userConfig.getAccount(), drawNumber, plan.getName(), result);
                JSONObject failed = new JSONObject();
                failed.putOpt("status", 1);
                failed.putOpt("lottery", plan.getLottery());
                failed.putOpt("drawNumber", drawNumber);
                failed.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                failed.putOpt("account", userConfig.getAccount());
                failed.putOpt("isRepair", 0);// 是否为补单 0:否 1:是
                failed.putOpt("message", "代理异常");
                return JSONUtil.toJsonStr(failed);
            }
        } catch (Exception e) {
            // ---------------------- 进入注单校验逻辑 ----------------------
            log.info("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},封盘时间:{},进入确认是否需要补单逻辑：下注请求超时，启动最大 15 秒注单匹配流程",
                    username, userConfig.getAccount(), drawNumber, plan.getName(), LocalDateTimeUtil.of(closeTime));

            long betTime = closeTime - System.currentTimeMillis();
            boolean successBet = false;                     // 标记是否确认投注成功
            long fixStart = System.currentTimeMillis();     // 补单判断阶段起始时间戳
            int maxTotalMillis = (int) (betTime - 5 * 1000);       // 补单判断总时长限制在关闭投注的5秒前
            long maxDurationMs = betTime - 5 * 1000;        // 最大注单匹配时间

            List<Bet> bets = order.getBets();
            if (CollUtil.isNotEmpty(bets)) {
                boolean[] found = new boolean[bets.size()];  // 标记各注单是否匹配成功
                int foundCnt = 0;                            // 已匹配成功注单计数
                int maxRetries = 5;                          // 最大轮询次数
                int retryInterval = 2_000;                   // 每轮查询间隔时间 2秒
                int pageSize = 15;                           // 接口分页大小

                // 多轮查询已下注注单，确认是否已成功
                for (int attempt = 1; attempt <= maxRetries && foundCnt < bets.size(); attempt++) {
                    long elapsed = System.currentTimeMillis() - fixStart;

                    // 超过最大判断时间，提前终止补单判断
                    if (elapsed >= maxTotalMillis) {
                        log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},补单确认阶段超过20秒，提前结束查询，已匹配 {}/{} 注单",
                                username, userConfig.getAccount(), drawNumber, plan.getName(), foundCnt, bets.size());
                        break;
                    }

                    // 若剩余时间不足5秒，避免被 OkHttp 阻塞，跳过当前轮
                    if (maxDurationMs - elapsed < 5000) {
                        log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},剩余时间不足5秒，跳过第{}次注单确认查询",
                                username, userConfig.getAccount(), drawNumber, plan.getName(), attempt);
                        break;
                    }

                    int curPage = 1, totalPages = 1;

                    try {
                        // 分页查询注单列表
                        while (curPage <= totalPages && foundCnt < bets.size()) {
                            if (System.currentTimeMillis() - fixStart >= maxTotalMillis) break;

                            long remainingTime = maxDurationMs - (System.currentTimeMillis() - fixStart);
                            int finalCurPage = curPage;

                            // 异步调用 settled() 接口查询注单，控制单次查询超时
                            CompletableFuture<JSONObject> future = CompletableFuture.supplyAsync(() ->
                                    JSONUtil.parseObj(handicapApi.settled(username, userConfig.getId(), false, finalCurPage, false))
                            );

                            JSONObject settledPg;
                            try {
                                settledPg = future.get(remainingTime, TimeUnit.MILLISECONDS);
                            } catch (TimeoutException ex) {
                                log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},settled() 查询超时，终止当前页注单查询",
                                        username, userConfig.getAccount(), drawNumber, plan.getName());
                                break;
                            }

                            JSONArray pageList = settledPg.getJSONArray("list");

                            if (CollUtil.isNotEmpty(pageList)) {
                                if (curPage == 1) {
                                    int total = settledPg.getInt("total", 0);
                                    totalPages = Math.max(1, (total + pageSize - 1) / pageSize);
                                }

                                // 遍历当前页注单列表，逐个匹配订单中的下注注单
                                for (Object obj : pageList) {
                                    JSONObject jo = JSONUtil.parseObj(obj);
                                    String draw = jo.getStr("drawNumber", "");
                                    String play = jo.getStr("play", "");
                                    long amount = jo.getLong("amount", -1L);

                                    for (int i = 0; i < bets.size(); i++) {
                                        if (!found[i]) {
                                            Bet b = bets.get(i);
                                            String betPlay = handicapApi.getPlayOrderBets(b);
                                            if (draw.contains(drawNumber)
                                                    && play.contains(betPlay)
                                                    && amount == b.getAmount()) {
                                                found[i] = true;
                                                ++foundCnt;
                                                log.info("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},匹配注单成功({}/{}): {}",
                                                        username, userConfig.getAccount(), drawNumber, plan.getName(), foundCnt, bets.size(), b);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            curPage++;
                        }
                    } catch (Exception ex) {
                        log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},注单确认查询异常，第{}次尝试失败，错误：{}",
                                username, userConfig.getAccount(), drawNumber, plan.getName(), attempt, ex.getMessage());
                    }

                    log.info("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},注单确认进度: 第{}/{}次尝试，已匹配 {}/{} 注单",
                            username, userConfig.getAccount(), drawNumber, plan.getName(), attempt, maxRetries, foundCnt, bets.size());

                    if (foundCnt >= bets.size()) {
                        break;
                    }

                    if (attempt < maxRetries) {
                        if (System.currentTimeMillis() - fixStart + retryInterval >= maxTotalMillis) {
                            log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},剩余时间不足，放弃后续轮询",
                                    username, userConfig.getAccount(), drawNumber, plan.getName());
                            break;
                        }
                        try {
                            Thread.sleep(retryInterval);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},补单确认轮询被中断",
                                    username, userConfig.getAccount(), drawNumber, plan.getName());
                            break;
                        }
                    }
                }

                successBet = (foundCnt >= bets.size());
            } else {
                log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},订单注单列表为空，无法确认下注成功",
                        username, userConfig.getAccount(), drawNumber, plan.getName());
            }
            JSONObject resultObj = new JSONObject();
            if (successBet) {
                log.info("翻倍计划-下单代理请求响应超时,但盘口投注成功, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 异常信息:", username, userConfig.getAccount(), drawNumber, plan.getName(), e);
                resultObj.putOpt("status", 0);
                resultObj.putOpt("lottery", plan.getLottery());
                resultObj.putOpt("drawNumber", drawNumber);
                resultObj.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                resultObj.putOpt("account", userConfig.getAccount());
                resultObj.putOpt("isRepair", 0);// 是否为补单 0:否 1:是
                resultObj.putOpt("message", "盘口投注成功");
            } else {
                log.info("翻倍计划-下单代理请求响应超时,平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 异常信息:", username, userConfig.getAccount(), drawNumber, plan.getName(), e);
                resultObj.putOpt("status", 1);
                resultObj.putOpt("lottery", plan.getLottery());
                resultObj.putOpt("drawNumber", drawNumber);
                resultObj.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                resultObj.putOpt("account", userConfig.getAccount());
                resultObj.putOpt("isRepair", 0);// 是否为补单 0:否 1:是
                resultObj.putOpt("message", "代理链接超时");

                Throwable cause = e.getCause();
                if (cause instanceof UnknownHostException) {
                } else if (cause instanceof ConnectException) {
                } else if (cause instanceof SocketTimeoutException) {
                    resultObj.putOpt("message", "代理链接超时");
                } else if (cause instanceof IOException) {
                } else {
                    // 未知异常，记录日志并抛出通用错误码
                }
            }
            success = successBet;
            return JSONUtil.toJsonStr(resultObj);
        } finally {
            log.info("翻倍计划-下单结束, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息{}, 网络请求成功:{}, 下单花费:{}毫秒", username, userConfig.getAccount(), drawNumber, plan.getName(), result, success, timeBet.intervalMs());
        }
        return StringUtils.isEmpty(result) ? null : result;
    }

    private String mergeSubmitOrder(String username, UserConfig userConfig, List<PlanDoubleVO> plans, OrderVO finalOrder, String lottery, String drawNumber, Map<String, Map<Integer, String>> useMonitorIds, long closeTime) {
        log.info("翻倍计划-发起下单, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 账号token:{}", username, userConfig.getAccount(), drawNumber, "合并型方案", userConfig.getToken());
        TimeInterval timeBet = DateUtil.timer();
        boolean success = true;
        String url = userConfig.getBaseUrl() + "member/bet";
        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("cookie", "defaultLT=" + lottery + "; token=" + userConfig.getToken());
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36");
        headers.put("x-requested-with", "XMLHttpRequest");

        // ✅ 获取Redis指定账户期数的所有订单请求
        /*List<Bet> bets = new ArrayList<>();
        for (PlanDoubleVO plan : plans) {
            String monitorId = useMonitorIds.get(userConfig.getAccount()).get(plan.getId());
            // 记录订单请求
            Object orderObj = redisson.getBucket(KeyUtil.genKey(
                    RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    username,
                    String.valueOf(plan.getId()),
                    monitorId,
                    userConfig.getAccount()
            )).get();
            if (orderObj == null) {
                log.info("翻倍计划-合并下单, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 没有订单请求", username, userConfig.getAccount(), drawNumber, plan.getName());
                continue;
            }
            OrderVO orderTemp = JSONUtil.toBean(orderObj.toString(), OrderVO.class);
            bets.addAll(orderTemp.getBets());
        }*/
        if (ObjectUtil.isNull(finalOrder) || finalOrder.getBets().isEmpty()) {
            log.info("翻倍计划-合并下单, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}, 订单请求为空", username, userConfig.getAccount(), drawNumber, plans.size());
            return null;
        }
        log.info("翻倍计划-合并下单, 平台用户:{}, 账号:{}, 期数:{}, 封盘时间:{}, 方案数:{}, 订单bet数:{}", username, userConfig.getAccount(), drawNumber, LocalDateTimeUtil.of(closeTime), plans.size(), finalOrder.getBets().size());
        finalOrder.setDrawNumber(drawNumber);
        finalOrder.setIgnore(false);
        finalOrder.setLottery(lottery);
        finalOrder.setFastBets(false);
        log.info("翻倍计划-合并下单,平台用户:{},期数:{},finalOrder:{}", username, drawNumber, JSONUtil.parseObj(finalOrder));

        // 记录订单请求
        redisson.getBucket(KeyUtil.genKey(
                RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                lottery,
                ToDayRangeUtil.getToDayRange(),
                drawNumber,
                username,
                "merge",
                userConfig.getAccount()
        )).set(JSONUtil.toJsonStr(finalOrder), Duration.ofHours(24));
        String result = null;
        try {
            // ✅ 使用 OkHttpProxyDispatcher 调用，并封装异步 + 超时
            CompletableFuture<OkHttpProxyDispatcher.HttpResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.info("翻倍计划-进行下单-发起下单请求 平台用户:{},游戏:{},期数:{},封盘时间:{},方案:{}", username, lottery, drawNumber, LocalDateTimeUtil.of(closeTime), "合并型方案");
                    return dispatcher.execute("POST", url, JSONUtil.toJsonStr(finalOrder), headers, userConfig, false);
                } catch (IOException e) {
                    // 抛出包装异常，供外层 get() 捕获
                    throw new CompletionException(e);
                }
            });
            OkHttpProxyDispatcher.HttpResult resultHttp = future.get(10, TimeUnit.SECONDS); // 10 秒超时
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
            log.info("翻倍计划-下单结果, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息{}", username, userConfig.getAccount(), drawNumber, "合并型方案", result);

            if (StringUtils.isBlank(result)) {
                log.info("翻倍计划-下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息{}", username, userConfig.getAccount(), drawNumber, "合并型方案", result);
                JSONObject failed = new JSONObject();
                failed.putOpt("status", 1);
                failed.putOpt("lottery", lottery);
                failed.putOpt("drawNumber", drawNumber);
                failed.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                failed.putOpt("account", userConfig.getAccount());
                failed.putOpt("isRepair", 0);// 是否为补单 0:否 1:是
                failed.putOpt("message", "账号异常,请检查登录状态以及账号是否正常");
                return JSONUtil.toJsonStr(failed);
            } else if (!JSONUtil.isTypeJSONObject(result)) {
                log.info("翻倍计划-下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息{}", username, userConfig.getAccount(), drawNumber, "合并型方案", result);
                JSONObject failed = new JSONObject();
                failed.putOpt("status", 1);
                failed.putOpt("lottery", lottery);
                failed.putOpt("drawNumber", drawNumber);
                failed.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                failed.putOpt("account", userConfig.getAccount());
                failed.putOpt("isRepair", 0);// 是否为补单 0:否 1:是
                failed.putOpt("message", "代理异常");
                return JSONUtil.toJsonStr(failed);
            }
        } catch (Exception e) {
            // ---------------------- 进入注单校验逻辑 ----------------------
            log.info("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},封盘时间:{},进入确认是否需要补单逻辑：下注请求超时，启动最大 15 秒注单匹配流程",
                    username, userConfig.getAccount(), drawNumber, "合并型方案", LocalDateTimeUtil.of(closeTime));

            long betTime = closeTime - System.currentTimeMillis();
            boolean successBet = false;                     // 标记是否确认投注成功
            long fixStart = System.currentTimeMillis();     // 补单判断阶段起始时间戳
            int maxTotalMillis = (int) (betTime - 5 * 1000);       // 补单判断总时长限制在关闭投注的5秒前
            long maxDurationMs = betTime - 5 * 1000;        // 最大注单匹配时间

            List<Bet> betAll = finalOrder.getBets();
            if (CollUtil.isNotEmpty(betAll)) {
                boolean[] found = new boolean[betAll.size()];  // 标记各注单是否匹配成功
                int foundCnt = 0;                            // 已匹配成功注单计数
                int maxRetries = 5;                          // 最大轮询次数
                int retryInterval = 2_000;                   // 每轮查询间隔时间 2秒
                int pageSize = 15;                           // 接口分页大小

                // 多轮查询已下注注单，确认是否已成功
                for (int attempt = 1; attempt <= maxRetries && foundCnt < betAll.size(); attempt++) {
                    long elapsed = System.currentTimeMillis() - fixStart;

                    // 超过最大判断时间，提前终止补单判断
                    if (elapsed >= maxTotalMillis) {
                        log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},补单确认阶段超过20秒，提前结束查询，已匹配 {}/{} 注单",
                                username, userConfig.getAccount(), drawNumber, "合并型方案", foundCnt, betAll.size());
                        break;
                    }

                    // 若剩余时间不足5秒，避免被 OkHttp 阻塞，跳过当前轮
                    if (maxDurationMs - elapsed < 5000) {
                        log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},剩余时间不足5秒，跳过第{}次注单确认查询",
                                username, userConfig.getAccount(), drawNumber, "合并型方案", attempt);
                        break;
                    }

                    int curPage = 1, totalPages = 1;

                    try {
                        // 分页查询注单列表
                        while (curPage <= totalPages && foundCnt < finalOrder.getBets().size()) {
                            if (System.currentTimeMillis() - fixStart >= maxTotalMillis) break;

                            long remainingTime = maxDurationMs - (System.currentTimeMillis() - fixStart);
                            int finalCurPage = curPage;

                            // 异步调用 settled() 接口查询注单，控制单次查询超时
                            CompletableFuture<JSONObject> future = CompletableFuture.supplyAsync(() ->
                                    JSONUtil.parseObj(handicapApi.settled(username, userConfig.getId(), false, finalCurPage, false))
                            );

                            JSONObject settledPg;
                            try {
                                settledPg = future.get(remainingTime, TimeUnit.MILLISECONDS);
                            } catch (TimeoutException ex) {
                                log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},settled() 查询超时，终止当前页注单查询",
                                        username, userConfig.getAccount(), drawNumber, "合并型方案");
                                break;
                            }

                            JSONArray pageList = settledPg.getJSONArray("list");

                            if (CollUtil.isNotEmpty(pageList)) {
                                if (curPage == 1) {
                                    int total = settledPg.getInt("total", 0);
                                    totalPages = Math.max(1, (total + pageSize - 1) / pageSize);
                                }

                                // 遍历当前页注单列表，逐个匹配订单中的下注注单
                                for (Object obj : pageList) {
                                    JSONObject jo = JSONUtil.parseObj(obj);
                                    String draw = jo.getStr("drawNumber", "");
                                    String play = jo.getStr("play", "");
                                    long amount = jo.getLong("amount", -1L);

                                    for (int i = 0; i < finalOrder.getBets().size(); i++) {
                                        if (!found[i]) {
                                            Bet b = finalOrder.getBets().get(i);
                                            String betPlay = handicapApi.getPlayOrderBets(b);
                                            if (draw.contains(drawNumber)
                                                    && play.contains(betPlay)
                                                    && amount == b.getAmount()) {
                                                found[i] = true;
                                                ++foundCnt;
                                                log.info("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},匹配注单成功({}/{}): {}",
                                                        username, userConfig.getAccount(), drawNumber, "合并型方案", foundCnt, finalOrder.getBets().size(), b);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            curPage++;
                        }
                    } catch (Exception ex) {
                        log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},注单确认查询异常，第{}次尝试失败，错误：{}",
                                username, userConfig.getAccount(), drawNumber, "合并型方案", attempt, ex.getMessage());
                    }

                    log.info("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},注单确认进度: 第{}/{}次尝试，已匹配 {}/{} 注单",
                            username, userConfig.getAccount(), drawNumber, "合并型方案", attempt, maxRetries, foundCnt, finalOrder.getBets().size());

                    if (foundCnt >= finalOrder.getBets().size()) {
                        break;
                    }

                    if (attempt < maxRetries) {
                        if (System.currentTimeMillis() - fixStart + retryInterval >= maxTotalMillis) {
                            log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},剩余时间不足，放弃后续轮询",
                                    username, userConfig.getAccount(), drawNumber, "合并型方案");
                            break;
                        }
                        try {
                            Thread.sleep(retryInterval);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},补单确认轮询被中断",
                                    username, userConfig.getAccount(), drawNumber, "合并型方案");
                            break;
                        }
                    }
                }

                successBet = (foundCnt >= finalOrder.getBets().size());
            } else {
                log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},订单注单列表为空，无法确认下注成功",
                        username, userConfig.getAccount(), drawNumber, "合并型方案");
            }
            JSONObject resultObj = new JSONObject();
            if (successBet) {
                log.info("翻倍计划-下单代理请求响应超时,但盘口投注成功, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 异常信息:", username, userConfig.getAccount(), drawNumber, "合并型方案", e);
                resultObj.putOpt("status", 0);
                resultObj.putOpt("lottery", lottery);
                resultObj.putOpt("drawNumber", drawNumber);
                resultObj.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                resultObj.putOpt("account", userConfig.getAccount());
                resultObj.putOpt("isRepair", 0);// 是否为补单 0:否 1:是
                resultObj.putOpt("message", "盘口投注成功");
            } else {
                log.info("翻倍计划-下单代理请求响应超时,平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 异常信息:", username, userConfig.getAccount(), drawNumber, "合并型方案", e);
                resultObj.putOpt("status", 1);
                resultObj.putOpt("lottery", lottery);
                resultObj.putOpt("drawNumber", drawNumber);
                resultObj.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                resultObj.putOpt("account", userConfig.getAccount());
                resultObj.putOpt("isRepair", 0);// 是否为补单 0:否 1:是
                resultObj.putOpt("message", "代理链接超时");

                Throwable cause = e.getCause();
                if (cause instanceof UnknownHostException) {
                } else if (cause instanceof ConnectException) {
                } else if (cause instanceof SocketTimeoutException) {
                    resultObj.putOpt("message", "代理链接超时");
                } else if (cause instanceof IOException) {
                } else {
                    // 未知异常，记录日志并抛出通用错误码
                }
            }
            success = successBet;
            return JSONUtil.toJsonStr(resultObj);
        } finally {
            log.info("翻倍计划-下单结束, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息{}, 网络请求成功:{}, 下单花费:{}毫秒", username, userConfig.getAccount(), drawNumber, "合并型方案", result, success, timeBet.intervalMs());
        }
        return StringUtils.isEmpty(result) ? null : result;
    }

    /**
     * 封装处理下注结果
     *
     * @param result
     * @param userConfig
     * @param drawNumber
     * @param plans
     * @param successAccounts
     * @param failedAccounts
     */
    private void handleOrderResult(String username, String result, UserConfig userConfig, String lottery, String drawNumber, List<PlanDoubleVO> plans, List<UserConfig> successAccounts, List<UserConfig> failedAccounts, AtomicInteger successCount, AtomicInteger failureCount, AtomicInteger successUserCount, AtomicInteger failureUserCount) {
        boolean isSuccess = true;
        JSONObject resultJson = JSONUtil.parseObj(result);
        if (!resultJson.isEmpty()) {
            int status = resultJson.getInt("status");
            resultJson.putOpt("type", 2);// 类型 1:方案 2:计划
            if (status == 0) {
                // 成功逻辑
                redisson.getBucket(KeyUtil.genKey(
                        RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        "success",
                        username,
                        userConfig.getAccount(),
                        "merge"
                )).set(JSONUtil.toJsonStr(resultJson), Duration.ofHours(24));
                successAccounts.add(userConfig);
                successCount.incrementAndGet();
                successUserCount.incrementAndGet();
                log.info("翻倍计划-下单成功, 平台用户:{}, 账号:{}, 期数:{}", username, userConfig.getAccount(), drawNumber);
            } else if (status == 2) {
                // 失败逻辑
                isSuccess = false;
                resultJson.putOpt("message", "投注超时");
            } else {
                // 失败逻辑
                isSuccess = false;
            }
        } else {
            isSuccess = false;
        }

        if (!isSuccess) {
            log.error("翻倍计划-下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息{}", username, userConfig.getAccount(), drawNumber, "合并型方案", resultJson);
            resultJson.putOpt("confirm", 0); // 待确认
            resultJson.putOpt("lottery", lottery);
            resultJson.putOpt("drawNumber", drawNumber);
            resultJson.putOpt("planName", "合并型方案");
            resultJson.putOpt("message", "代理异常");
            resultJson.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            resultJson.putOpt("account", userConfig.getAccount());
            resultJson.putOpt("isRepair", 0);// 是否为补单 0:否 1:是
            resultJson.putOpt("type", 2);// 类型 1:方案 2:计划
            // 记录失败信息
            businessUserRedissonClient.getBucket(KeyUtil.genKey(
                    RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    "failed",
                    username,
                    userConfig.getAccount(),
                    "merge"
            )).set(resultJson, Duration.ofHours(24));
            failedAccounts.add(userConfig);
            failureCount.incrementAndGet();
            failureUserCount.incrementAndGet();
        }
    }

    /**
     * 处理下单失败的反补操作
     * @param failedAccounts
     * @param successAccounts
     * @param plans
     * @param drawNumber
     */
    private void processReverseBet(AdminLoginDTO admin, List<UserConfig> failedAccounts, Map<String, Map<Integer, String>> failedMonitorIds, Map<Integer, Integer> roundsMap, List<UserConfig> successAccounts, List<PlanDoubleVO> plans, String drawNumber, long closeTime, AtomicInteger reverseCount, AtomicInteger reverseUserCount) {
        if (CollectionUtil.isNotEmpty(failedAccounts)) {
            String lottery = plans.get(0).getLottery();
            // 当前方案存在账号下注失败
            for (UserConfig account : failedAccounts) {
                // 获取到下注失败的请求参数
                String key = KeyUtil.genKey(
                        RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        admin.getUsername(),
                        "merge",
                        account.getAccount()
                );
                log.info("翻倍计划-平台用户:{}账号:{}方案:{}存在账号下注失败,开始反补,下注失败的请求参数key:{}", admin.getUsername(), account.getAccount(), "合并型方案", key);
                String failedReq = (String) redisson.getBucket(key).get();
                if (StringUtils.isBlank(failedReq)) {
                    log.error("翻倍计划-平台用户:{}, 失败账号:{}, 方案:{}, 存在账号下注失败,反补失败,下注失败的请求参数key:{}不存在", admin.getUsername(), account.getAccount(), "合并型方案", key);
                    continue;
                }
                // 通过当前方案账号进行反补
                for (UserConfig sucAccount : successAccounts) {
                    // 正投账号失败由反投账号补单，反之反投失败由正投账号补单
                    if (Objects.equals(sucAccount.getBetType(), account.getBetType())) continue;
                    boolean reverseSuccess = handleReverseBet(admin.getUsername(), account, sucAccount, failedReq, account.getBetType(), plans, lottery, drawNumber, closeTime, reverseCount, reverseUserCount);
                    if (reverseSuccess) {
                        break; // 反补成功退出
                    }
                }
            }
            // 关闭自动下注
            admin.setAutoBet(0);
            businessUserRedissonClient.getBucket(KeyUtil.genKey(
                            RedisConstants.USER_ADMIN_PREFIX,
                    admin.getUsername()
                    )).set(JSONUtil.toJsonStr(admin));
            // 保险起见-再把当前平台用户下的账号全部下线
            // batchLoginOut(username);
            // 把当前平台用户下的所有方案停用
            // List<ConfigPlanVO> configPlans = configService.getAllPlans(admin.getUsername(), null);
            // configPlans.forEach(configPlan -> {
            //     configPlan.setEnable(0);
            //     configService.plan(admin.getUsername(), configPlan);
            // });
        }
    }

    private boolean handleReverseBet(String username, UserConfig failedAccount, UserConfig sucAccount, String failedReq, Integer failedBetType, List<PlanDoubleVO> plans, String lottery, String drawNumber, long closeTime, AtomicInteger reverseCount, AtomicInteger reverseUserCount) {
        String reverseLockKey = "reverse:lock:double:" + username + ":" + drawNumber + ":merge:" + failedAccount.getId();
        RLock lock = redisson.getLock(reverseLockKey);
        try {
            boolean locked = lock.tryLock(2, 10, TimeUnit.SECONDS);
            if (!locked) {
                log.warn("翻倍计划-补单正在进行中,跳过此次补单, 平台用户:{}, 失败账号:{}, 期数:{}, 方案:{}", username, failedAccount, drawNumber, "合并型方案");
                return false;
            }
        } catch (InterruptedException e) {
            log.info("翻倍计划-发起补单, 平台用户:{}, 游戏:{}, 期数:{}, 方案:{}, 补单时获取锁失败", username, lottery, drawNumber, "合并型方案");
        }
        log.info("翻倍计划-发起补单, 平台用户:user001{}, 账号:{}, 账号token:{}, 期数:{}, 封盘时间:{}, 方案:{}, 补单参数:{}", username, failedAccount.getAccount(), failedAccount.getToken(), drawNumber, LocalDateTimeUtil.of(closeTime), "合并型方案", failedReq);

        // 补单前获取当前盘口账号的赔率信息,这样做的目的是防止同一批下注的账号中会有不同盘口的，不同盘口赔率不一致就会导致投注失败
        TimeInterval timerOdds = DateUtil.timer();
        String odds = null;
        // 获取赔率
        try {
            odds = handicapApi.getOddsFromCache(username, drawNumber, sucAccount.getId(), lottery, "合并型方案");
        } catch (Exception e) {
            log.error("翻倍计划-进行补单-获取赔率异常 平台用户:{},游戏:{},期数:{},方案:{}", username, lottery, drawNumber, "合并型方案");
            // 释放锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
            JSONObject failed = new JSONObject();
            failed.putOpt("status", 1);
            failed.putOpt("lottery", lottery);
            failed.putOpt("drawNumber", drawNumber);
            failed.putOpt("planName", "合并型方案");
            failed.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            failed.putOpt("account", failedAccount.getAccount());
            failed.putOpt("message", "账号获取赔率失败");
            failed.putOpt("isRepair", 1);// 是否为补单 0:否 1:是
            failed.putOpt("type", 2);// 类型 1:方案 2:计划
            // 反补失败返回参数
            businessUserRedissonClient.getBucket(KeyUtil.genKey(
                    RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    "failed",
                    username,
                    sucAccount.getAccount(),
                    "merge"
            )).set(failed, Duration.ofHours(24));
            return false;
        }
        if (StringUtils.isBlank(odds)) {
            log.error("翻倍计划-进行补单-获取赔率失败 游戏:{},期数:{},平台用户:{},方案:{},释放当前期数锁", lottery, drawNumber, username, "合并型方案");
            // 释放锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
            JSONObject failed = new JSONObject();
            failed.putOpt("status", 1);
            failed.putOpt("lottery", lottery);
            failed.putOpt("drawNumber", drawNumber);
            failed.putOpt("planName", "合并型方案");
            failed.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            failed.putOpt("account", failedAccount.getAccount());
            failed.putOpt("message", "账号获取赔率失败");
            failed.putOpt("isRepair", 1);// 是否为补单 0:否 1:是
            failed.putOpt("type", 2);// 类型 1:方案 2:计划
            // 反补失败返回参数
            businessUserRedissonClient.getBucket(KeyUtil.genKey(
                    RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    "failed",
                    username,
                    sucAccount.getAccount(),
                    "merge"
            )).set(failed, Duration.ofHours(24));
            return false;
        }
        log.info("翻倍计划-进行补单-获取赔率成功 平台用户:{},游戏:{},期数:{},封盘时间:{},方案:{},获取赔率用时:{}ms", username, lottery, drawNumber, LocalDateTimeUtil.of(closeTime), "合并型方案", timerOdds.interval());

        JSONObject oddsJsonNew = JSONUtil.parseObj(odds);

        // 提交反补订单
        String url = sucAccount.getBaseUrl() + "member/bet";

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "*/*");
        headers.put("cookie", "defaultLT=" + lottery + "; token=" + sucAccount.getToken());
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36");
        headers.put("x-requested-with", "XMLHttpRequest");

        // 1. 解析失败请求
        OrderVO orderVO = JSONUtil.toBean(failedReq, OrderVO.class);

        int positiveAmount = 0;
        int reverseAmount = 0;
        for (PlanDoubleVO plan : plans) {
            positiveAmount += formatIds(plan.getPositiveAmount()).get(0);
            reverseAmount += formatIds(plan.getReverseAmount()).get(0);
        }

        // 计算正反投的比例，保留两个小数点
        double ratio;
        if (failedBetType == 1) {
            // 正投失败的比例
            ratio = (double) reverseAmount / positiveAmount;
        } else {
            // 反投失败的比例
            ratio = (double) positiveAmount / reverseAmount;
        }

        // 设置金额
        double finalRatio = ratio;
        orderVO.getBets().forEach(bet -> {
                long amount = Math.round(bet.getAmount() * finalRatio);
                // 保证最低不低于 2
                bet.setAmount(Math.max(amount, 2));
                // 更新赔率
                bet.setOdds(oddsJsonNew.getDouble(bet.getGame()+"_"+bet.getContents()));
            }
        );

        // ✅ 构造 Map 手动保留 null
        Map<String, Object> map = new HashMap<>();
        map.put("account", orderVO.getAccount());
        map.put("token", orderVO.getToken());
        map.put("drawNumber", orderVO.getDrawNumber());
        map.put("ignore", orderVO.isIgnore());
        map.put("lottery", orderVO.getLottery());
        map.put("bets", orderVO.getBets());
        map.put("fastBets", orderVO.isFastBets());  // 即使为 false/null 也可以保留

        // ✅ 用 JSONUtil 构造 JSONObject（此时内部 null 不会变成 JSONNull.INSTANCE）
        JSONObject compensateReqJson = new JSONObject(map);

        boolean resultBol = false;
        // 记录反补的请求参数
        redisson.getBucket(KeyUtil.genKey(
                RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                lottery,
                ToDayRangeUtil.getToDayRange(),
                drawNumber,
                username,
                "merge",
                sucAccount.getAccount(),
                "reverse"
        )).set(JSONUtil.toJsonStr(compensateReqJson), Duration.ofHours(24));

        String result = null;
        String now = LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN);
        try {
            CompletableFuture<OkHttpProxyDispatcher.HttpResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.info("翻倍计划-进行补单-发起补单请求 平台用户:{},游戏:{},期数:{},封盘时间:{},方案:{}", username, lottery, drawNumber, LocalDateTimeUtil.of(closeTime), "合并型方案");
                    return dispatcher.execute("POST", url, compensateReqJson.toString(), headers, sucAccount, false);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });

            // 这里可以根据需要调整超时时间
            OkHttpProxyDispatcher.HttpResult resultHttp = future.get(10, TimeUnit.SECONDS);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;

            log.info("翻倍计划-补单结束, 平台用户:{}, 账号:{}, 期数:{}, 返回信息{}", username, failedAccount.getAccount(), drawNumber, result);
            if (!JSONUtil.isTypeJSONObject(result)) {
                log.error("翻倍计划-补单失败, 平台用户:{}, 账号:{}, 期数:{}, 返回信息{}", username, failedAccount.getAccount(), drawNumber, result);
                JSONObject resultJson = new JSONObject();
                resultJson.putOpt("confirm", 0); // 待确认
                resultJson.putOpt("lottery", lottery);
                resultJson.putOpt("drawNumber", drawNumber);
                resultJson.putOpt("planName", "合并型方案");
                resultJson.putOpt("createTime", now);
                resultJson.putOpt("account", sucAccount.getAccount());
                resultJson.putOpt("message", "代理异常");
                resultJson.putOpt("isRepair", 1);// 是否为补单 0:否 1:是
                resultJson.putOpt("type", 2);// 类型 1:方案 2:计划
                // 反补失败返回参数
                businessUserRedissonClient.getBucket(KeyUtil.genKey(
                        RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        "failed",
                        username,
                        sucAccount.getAccount(),
                        "merge"
                )).set(resultJson, Duration.ofHours(24));

                // 把下单失败的日志加上补单是否成功的信息
                String failedKey = KeyUtil.genKey(
                        RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        "failed",
                        username,
                        failedAccount.getAccount(),
                        "merge"
                );
                JSONObject failedJson = JSONUtil.parseObj(businessUserRedissonClient.getBucket(failedKey).get());
                if (null != failedJson && !failedJson.isEmpty()) {
                    failedJson.putOpt("repair", 0);
                    failedJson.putOpt("repairAccount", (StringUtils.isBlank(failedJson.getStr("repairAccount")) ? "" : failedJson.getStr("repairAccount") + ",") + sucAccount.getAccount());
                    failedJson.putOpt("type", 2);// 类型 1:方案 2:计划
                    businessUserRedissonClient.getBucket(failedKey).set(failedJson, Duration.ofHours(24));
                }
                return resultBol;
            }
        } catch (Exception e) {

            // ---------------------- 进入注单校验逻辑 ----------------------
            log.info("翻倍计划-开始补单确认检查,平台用户:{},盘口账号:{},期数:{},封盘时间:{},进入确认是否需要补单逻辑：下注请求超时，启动最大 15 秒注单匹配流程",
                    username, sucAccount.getAccount(), drawNumber, LocalDateTimeUtil.of(closeTime));
            boolean successBet = false;                     // 标记是否确认投注成功
            long fixStart = System.currentTimeMillis();     // 补单判断阶段起始时间戳
            int maxTotalMillis = 30_000;       // 补单判断总时长限制在关闭投注的5秒前
            long maxDurationMs = 30 * 1000;       // 最大注单匹配时间

            List<Bet> betAll = orderVO.getBets();
            if (CollUtil.isNotEmpty(betAll)) {
                boolean[] found = new boolean[betAll.size()];  // 标记各注单是否匹配成功
                int foundCnt = 0;                            // 已匹配成功注单计数
                int maxRetries = 5;                          // 最大轮询次数
                int retryInterval = 2_000;                   // 每轮查询间隔时间（毫秒）
                int pageSize = 15;                           // 接口分页大小

                // 多轮查询已下注注单，确认是否已成功
                for (int attempt = 1; attempt <= maxRetries && foundCnt < betAll.size(); attempt++) {
                    long elapsed = System.currentTimeMillis() - fixStart;

                    // 超过最大判断时间，提前终止补单判断
                    if (elapsed >= maxTotalMillis) {
                        log.warn("翻倍计划-开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},补单确认阶段超过期数关闭时间，提前结束查询，已匹配 {}/{} 注单",
                                username, sucAccount.getAccount(), drawNumber, "合并型方案", foundCnt, betAll.size());
                        break;
                    }

                    // 若剩余时间不足5秒，避免被 OkHttp 阻塞，跳过当前轮
                    if (maxDurationMs - elapsed < 5000) {
                        log.warn("翻倍计划-开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},剩余时间不足5秒，跳过第{}次注单确认查询",
                                username, sucAccount.getAccount(), drawNumber, "合并型方案", attempt);
                        break;
                    }

                    int curPage = 1, totalPages = 1;

                    try {
                        // 分页查询注单列表
                        while (curPage <= totalPages && foundCnt < betAll.size()) {
                            if (System.currentTimeMillis() - fixStart >= maxTotalMillis) break;

                            long remainingTime = maxDurationMs - (System.currentTimeMillis() - fixStart);
                            int finalCurPage = curPage;

                            // 异步调用 settled() 接口查询注单，控制单次查询超时
                            CompletableFuture<JSONObject> future = CompletableFuture.supplyAsync(() ->
                                    JSONUtil.parseObj(handicapApi.settled(username, sucAccount.getId(), false, finalCurPage, false))
                            );

                            JSONObject settledPg;
                            try {
                                settledPg = future.get(remainingTime, TimeUnit.MILLISECONDS);
                            } catch (TimeoutException ex) {
                                log.info("翻倍计划-开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},settled() 查询超时，终止当前页注单查询",
                                        username, sucAccount.getAccount(), drawNumber, "合并型方案");
                                break;
                            }

                            JSONArray pageList = settledPg.getJSONArray("list");

                            if (CollUtil.isNotEmpty(pageList)) {
                                if (curPage == 1) {
                                    int total = settledPg.getInt("total", 0);
                                    totalPages = Math.max(1, (total + pageSize - 1) / pageSize);
                                }

                                // 遍历当前页注单列表，逐个匹配订单中的下注注单
                                for (Object obj : pageList) {
                                    JSONObject jo = JSONUtil.parseObj(obj);
                                    String draw = jo.getStr("drawNumber", "");
                                    String play = jo.getStr("play", "");
                                    long amount = jo.getLong("amount", -1L);

                                    for (int i = 0; i < betAll.size(); i++) {
                                        if (!found[i]) {
                                            Bet b = betAll.get(i);
                                            String betPlay = handicapApi.getPlayOrderBets(b);
                                            if (draw.contains(drawNumber)
                                                    && play.contains(betPlay)
                                                    && amount == b.getAmount()) {
                                                found[i] = true;
                                                ++foundCnt;
                                                log.info("翻倍计划-开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},匹配注单成功({}/{}): {}",
                                                        username, sucAccount.getAccount(), drawNumber, "合并型方案", foundCnt, betAll.size(), b);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                            curPage++;
                        }
                    } catch (Exception ex) {
                        log.warn("翻倍计划-开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},注单确认查询异常，第{}次尝试失败，错误：{}",
                                username, sucAccount.getAccount(), drawNumber, "合并型方案", attempt, ex.getMessage());
                    }

                    log.info("翻倍计划-开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},注单确认进度: 第{}/{}次尝试，已匹配 {}/{} 注单",
                            username, sucAccount.getAccount(), drawNumber, "合并型方案", attempt, maxRetries, foundCnt, betAll.size());

                    if (foundCnt >= betAll.size()) {
                        break;
                    }

                    if (attempt < maxRetries) {
                        if (System.currentTimeMillis() - fixStart + retryInterval >= maxTotalMillis) {
                            log.warn("翻倍计划-开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},剩余时间不足，放弃后续轮询",
                                    username, sucAccount.getAccount(), drawNumber, "合并型方案");
                            break;
                        }
                        try {
                            Thread.sleep(retryInterval);
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            log.warn("翻倍计划-开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},补单确认轮询被中断",
                                    username, sucAccount.getAccount(), drawNumber, "合并型方案");
                            break;
                        }
                    }
                }

                successBet = (foundCnt >= betAll.size());
            } else {
                log.warn("翻倍计划-开始补单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},订单注单列表为空，无法确认下注成功",
                        username, sucAccount.getAccount(), drawNumber, "合并型方案");
            }

            JSONObject resultObj = new JSONObject();
            if (successBet) {
                log.info("翻倍计划-补单请求超时,但投注成功, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}", username, sucAccount.getAccount(), drawNumber, "合并型方案");
                resultObj.putOpt("status", 0);
                resultObj.putOpt("message", "补单成功");
            } else {
                log.info("翻倍计划-补单请求超时,未投注成功, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}", username, sucAccount.getAccount(), drawNumber, "合并型方案");
                resultObj.putOpt("status", 1);
                resultObj.putOpt("message", "代理链接超时");
            }
            resultObj.putOpt("drawNumber", drawNumber);
            resultObj.putOpt("lottery", lottery);
            resultObj.putOpt("createTime", now);
            resultObj.putOpt("isRepair", 1);// 是否为补单 0:否 1:是

            Throwable cause = e.getCause(); // 获取原始异常原因
            if (cause instanceof UnknownHostException) {
                log.error("翻倍计划-反补请求代理异常, 失败账号:{}, 对冲账号:{}, 期数:{}, 异常:{}", failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, e.getMessage());
            } else if (cause instanceof ConnectException) {
                log.error("翻倍计划-反补请求代理异常, 失败账号:{}, 对冲账号:{}, 期数:{}, 异常:{}", failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, e.getMessage());
            } else if (cause instanceof SocketTimeoutException) {
                log.error("翻倍计划-反补请求代理异常, 失败账号:{}, 对冲账号:{}, 期数:{}, 异常:{}", failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, e.getMessage());
            } else if (cause instanceof IOException) {
                log.error("翻倍计划-反补请求代理异常, 失败账号:{}, 对冲账号:{}, 期数:{}, 异常:{}", failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, e.getMessage());
            } else {
                // 未知异常，记录日志并抛出通用错误码
                log.error("翻倍计划-反补请求代理异常, 失败账号:{}, 对冲账号:{}, 期数:{}, 异常:{}", failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, e.getMessage());
            }
            result = JSONUtil.toJsonStr(resultObj);
        } finally {
            // 释放锁
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
        result = StringUtils.isEmpty(result) ? null : result;
        JSONObject resultJson = JSONUtil.parseObj(result);
        if (!resultJson.isEmpty()) {
            int status = resultJson.getInt("status");
            if (status == 0) {
                resultJson.putOpt("type", 2);// 类型 1:方案 2:计划
                log.info("翻倍计划-补单成功, 平台用户:{}, 失败账号:{}, 对冲账号:{}, 期数:{}", username, failedAccount.getAccount(), sucAccount.getAccount(), drawNumber);
                // 反补成功返回参数
                redisson.getBucket(KeyUtil.genKey(
                        RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        "reverse",
                        username,
                        sucAccount.getAccount(),
                        "merge"
                )).set(JSONUtil.toJsonStr(resultJson), Duration.ofHours(24));

                // 把下单失败的日志加上补单是否成功的信息
                String failedKey = KeyUtil.genKey(
                        RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                        lottery,
                        ToDayRangeUtil.getToDayRange(),
                        drawNumber,
                        "failed",
                        username,
                        failedAccount.getAccount(),
                        "merge"
                );
                JSONObject failedJson = JSONUtil.parseObj(businessUserRedissonClient.getBucket(failedKey).get());
                if (null != failedJson && !failedJson.isEmpty()) {
                    failedJson.putOpt("repair", 1);
                    failedJson.putOpt("repairAccount", sucAccount.getAccount());
                    failedJson.putOpt("type", 2);// 类型 1:方案 2:计划
                    businessUserRedissonClient.getBucket(failedKey).set(failedJson, Duration.ofHours(24));
                }

                reverseCount.incrementAndGet();
                reverseUserCount.incrementAndGet();
                return true; // 反补成功
            } else if (status == 2) {
                // 失败逻辑
                resultJson.putOpt("message", "补单投注超时");
            }
        }
        if (!resultBol) {
            log.error("翻倍计划-补单失败, 平台用户:{}, 失败账号:{}, 对冲账号:{}, 期数:{}, 返回信息{}", username, failedAccount.getAccount(), sucAccount.getAccount(), drawNumber, resultJson);
            resultJson.putOpt("confirm", 0); // 待确认
            resultJson.putOpt("lottery", lottery);
            resultJson.putOpt("drawNumber", drawNumber);
            resultJson.putOpt("planName", "合并型方案");
            resultJson.putOpt("createTime", now);
            resultJson.putOpt("message", "代理异常");
            resultJson.putOpt("account", sucAccount.getAccount());
            resultJson.putOpt("isRepair", 1);// 是否为补单 0:否 1:是
            resultJson.putOpt("type", 2);// 类型 1:方案 2:计划
            // 反补失败返回参数
            businessUserRedissonClient.getBucket(KeyUtil.genKey(
                    RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    "failed",
                    username,
                    sucAccount.getAccount(),
                    "merge"
            )).set(resultJson, Duration.ofHours(24));

            // 把下单失败的日志加上补单是否成功的信息
            String failedKey = KeyUtil.genKey(
                    RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                    lottery,
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    "failed",
                    username,
                    failedAccount.getAccount(),
                    "合并型方案"
            );
            JSONObject failedJson = JSONUtil.parseObj(businessUserRedissonClient.getBucket(failedKey).get());
            if (null != failedJson && !failedJson.isEmpty()) {
                failedJson.putOpt("repair", 0);
                failedJson.putOpt("repairAccount", (StringUtils.isBlank(failedJson.getStr("repairAccount")) ? "" : failedJson.getStr("repairAccount") + ",") + sucAccount.getAccount());
                failedJson.putOpt("type", 2);// 类型 1:方案 2:计划
                businessUserRedissonClient.getBucket(failedKey).set(failedJson, Duration.ofHours(24));
            }

        }
        return resultBol;
    }

    /**
     * 根据odds切割key
     * @param input
     * @return
     */
    public static List<String> oddsKyeSplitter(String input) {
        List<String> parts = new ArrayList<>();
        String[] splitPart = input.split("_");
        parts.add(splitPart[0]);// _前的部分
        parts.add(splitPart[1]);// _后的部分
        return parts;
    }

    /**
     * 获取正反投的数字
     * @param betNum
     * @return
     */
    public static Map<String, List<Integer>> generateBetContents(int betNum) {
        Map<String, List<Integer>> contents = new HashMap<>();
        // 用来存放正投和反投的数字
        Set<Integer> positiveContentsSet = new HashSet<>(); // 使用Set来避免重复
        List<Integer> reverseContents = new ArrayList<>();

        // 创建随机数生成器
        Random random = ThreadLocalRandom.current();

        // 生成 betNum 个 1 到 10 之间的随机数，存入Set来避免重复
        while (positiveContentsSet.size() < betNum) {
            int randomNum = random.nextInt(10) + 1;  // 生成1到10之间的随机数
            positiveContentsSet.add(randomNum);  // 添加到Set中
        }

        // 将Set转换为List以便返回
        List<Integer> positiveContents = new ArrayList<>(positiveContentsSet);

        // 计算剩余的数字，用于反投
        for (int i = 1; i <= 10; i++) {
            if (!positiveContentsSet.contains(i)) {
                reverseContents.add(i);  // 如果正投列表中没有此数字，则加入反投列表
            }
        }

        // 将结果添加到Map中
        contents.put("positive", positiveContents);
        contents.put("reverse", reverseContents);

        return contents;
    }

    /**
     * 分配正反投位置， 根据locationNum正投位置获取list中的数据赋值给positive，剩余数据则赋予reverse，获取的positive不能重复
     * @param list          号码对应的全部位置
     * @param locationNum   正投位置
     * @return
     */
    private static Map<String, List<String>> getOdds(String reverseType, List<String> list, String locationNum) {
//        List<Integer> idsNew = new ArrayList<>();
//        if (reverseType == 1) { // 竖式
//            idsNew = positives;
//        } else if (reverseType == 2) {    // 横式
//            idsNew = IntStream.rangeClosed(1, 10) // 生成 1 到 10 的整数
//                    .boxed() // 转换为 List<Integer>
//                    .filter(id -> !positives.contains(id)) // 过滤掉 `ids` 中已有的值
//                    .toList();
//        }
        Map<String, List<String>> odds = new HashMap<>();
        System.out.println("locationNum: " + locationNum + " list.size: " + list.size());

        // 拷贝list，防止修改原始list
        List<String> positive = new ArrayList<>();
        List<String> reverse = new ArrayList<>();

        // 解析locationNum，将其转为整数列表
        List<Integer> ids = formatIds(locationNum);

        // 通过locationNum对应的正投位置从list中选择元素
        Set<String> positiveSet = new HashSet<>();
        for (Integer id : ids) {
            // 遍历list，根据每个元素的下划线后部分与locationNum匹配
            for (String item : list) {
                String[] parts = item.split("_");
                int position = Integer.parseInt(parts[1]);

                // 如果locationNum包含当前下划线后的数字，则将其加入positive
                if (position == id && !positiveSet.contains(item)) {
                    positive.add(item);
                    positiveSet.add(item);
                    break; // 确保每个位置只匹配一次
                }
            }
        }

        if ("1".equals(reverseType)) { // 竖式
            // 将剩余的元素放入reverse
            for (String item : list) {
                if (!positiveSet.contains(item)) {
                    reverse.add(item);
                }
            }
        } else if ("2".equals(reverseType)) {  // 横式
            reverse.addAll(positiveSet);
        }

        // 将正反投结果存入Map
        odds.put("positive", positive);
        odds.put("reverse", reverse);

        System.out.println("正投赔率key: " + positive);
        System.out.println("反投赔率key: " + reverse);
        return odds;
    }

    public Map<String, Map<String, List<String>>> assignPositions(List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts,
                                                                  Map<String, List<String>> twoSidedDxPositions,
                                                                  Map<String, List<String>> twoSidedDsPositions,
                                                                  Map<String, List<String>> twoSidedLhPositions) {
        Random random = ThreadLocalRandom.current();
        Map<String, Map<String, List<String>>> selections = new HashMap<>();
        // 存储正投和反投结果
        Map<String, List<String>> positiveSelections = new HashMap<>();
        Map<String, List<String>> reverseSelections = new HashMap<>();

        // 记录已分配过的位置，避免重复分配
        Set<String> allocatedPositions = new HashSet<>();

        // 处理所有投项
        processPositions(twoSidedDxPositions, positiveAccounts, reverseAccounts, "_D", "_X", positiveSelections, reverseSelections, allocatedPositions);
        processPositions(twoSidedDsPositions, positiveAccounts, reverseAccounts, "_D", "_S", positiveSelections, reverseSelections, allocatedPositions);
        processPositions(twoSidedLhPositions, positiveAccounts, reverseAccounts, "_L", "_H", positiveSelections, reverseSelections, allocatedPositions);

        selections.put("positive", positiveSelections);
        selections.put("reverse", reverseSelections);
        return selections;
    }

    private void processPositions(Map<String, List<String>> positions, List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts,
                                  String suffixPositiveKey, String suffixReverseKey,
                                  Map<String, List<String>> positiveSelections, Map<String, List<String>> reverseSelections,
                                  Set<String> allocatedPositions) {

        // 按照基础位置（不含后缀）对投项进行分组
        Map<String, List<String>> groupedPositions = new HashMap<>();
        positions.forEach((key, value) -> {
            for (String pos : value) {
                String basePosition = pos.substring(0, pos.lastIndexOf('_')); // 提取基础位置
                groupedPositions.computeIfAbsent(basePosition, k -> new ArrayList<>()).add(pos);
            }

            // 对每个基础位置的投项进行处理
            for (Map.Entry<String, List<String>> entry : groupedPositions.entrySet()) {
                String basePosition = entry.getKey();
                List<String> positionList = entry.getValue();

                // 确保每个位置只分配一次
                if (allocatedPositions.contains(basePosition)) {
                    continue; // 如果该位置已分配，跳过
                }

                // 决定正投和反投的后缀
                boolean nextBoolean = key.contains("大") || key.contains("单") || key.contains("龙");
                String selectedPositiveKey = nextBoolean ? suffixPositiveKey : suffixReverseKey;
                String selectedReverseKey = nextBoolean ? suffixReverseKey : suffixPositiveKey;

                // 存储正投和反投的账户列表
                List<String> positiveSelection = new ArrayList<>();
                List<String> reverseSelection = new ArrayList<>();

                // 遍历该位置的所有投项并进行分配
                for (String pos : positionList) {
                    // 为正投选择账户
                    positiveAccounts.forEach(account -> {
                        if (pos.endsWith(selectedPositiveKey)) {
                            positiveSelection.add(account.getAccount());
                        }
                    });

                    // 为反投选择账户
                    reverseAccounts.forEach(account -> {
                        if (pos.endsWith(selectedReverseKey)) {
                            reverseSelection.add(account.getAccount());
                        }
                    });
                }

                // 如果正投选择不为空，则记录该选择
                if (!positiveSelection.isEmpty()) {
                    positiveSelections.put(basePosition + selectedPositiveKey, positiveSelection);
                }

                // 如果反投选择不为空，则记录该选择
                if (!reverseSelection.isEmpty()) {
                    reverseSelections.put(basePosition + selectedReverseKey, reverseSelection);
                }

                // 标记该基础位置已分配
                allocatedPositions.add(basePosition);
            }
        });

    }

    /**
     * 随机抽取指定数量的投注账户
     */
    private static List<UserConfig> getRandomAccount(List<UserConfig> list, int betNum, int betType) {
        log.info("翻倍计划-betNum:{} list.size:{} betType:{}", betNum, list.size(), betType);

        // 过滤出符合betType的UserConfig列表
        List<UserConfig> filteredList = new ArrayList<>();
        for (UserConfig userConfig : list) {
            if (betType == userConfig.getBetType()) {
                filteredList.add(userConfig);
            }
        }

        // 如果过滤后的list的大小小于betNum，则返回整个过滤后的list
        if (filteredList.size() <= betNum) {
            log.info("翻倍计划-过滤后的list的大小小于betNum，则返回整个过滤后的list 指定账户json key:{}", filteredList);
            return filteredList;
        }

        Random random = ThreadLocalRandom.current();
        List<UserConfig> result = new ArrayList<>();
        List<UserConfig> temp = new ArrayList<>(filteredList); // 创建一个临时副本来进行随机选择

        // 随机选取betNum个元素
        for (int i = 0; i < betNum; i++) {
            int randomIndex = random.nextInt(temp.size()); // 从剩余的元素中随机选择
            result.add(temp.remove(randomIndex)); // 移除已选中的元素，避免重复
        }

        log.info("翻倍计划-指定账户json key:{}", result);
        return result;
    }

    /**
     * 金额分配逻辑，首先遍历oddsMap，获取oddsMap下每个位置key，比如下标1有positive值的B1_1、B1_2、B1_3和reverse值的B1_4、B1_5、B1_6，然后根据userConfigs里的betType对应的账户，然后根据plan的betNum和oddsMap下每个位置key的数量，计算每个账户应该分配的金额，最后将金额分配给每个账户
     *
      */
    public static JSONObject distributeAmount(Map<Integer, Map<String, List<String>>> oddsMap, List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts, PlanDoubleVO plan, Integer rounds) {
        JSONObject resultJson = new JSONObject();

        List<Integer> positiveAmounts = formatIds(plan.getPositiveAmount());
        List<Integer> reverseAmounts = formatIds(plan.getReverseAmount());
        log.info("翻倍计划-正反金额分配-positiveAmounts大小:{}, reverseAmounts大小:{}, 当前rounds:{}", positiveAmounts.size(), reverseAmounts.size(), rounds);
        // 遍历 oddsMap 中的每个 odds 数据
        oddsMap.forEach((oddsKey, v) -> {
            // 分配 positive 账户的金额
            JSONObject posJson = new JSONObject();
            List<String> posOddsKey = v.get("positive");
            posOddsKey.forEach(odds -> {
                JSONObject amountJson = distributeAmountsForAccounts(positiveAccounts, positiveAmounts.get(rounds));
                posJson.putOpt(odds, amountJson);
            });

            // 分配 reverse 账户的金额
            JSONObject revJson = new JSONObject();
            List<String> revOddsKey = v.get("reverse");
            revOddsKey.forEach(odds -> {
                JSONObject amountJson = distributeAmountsForAccounts(reverseAccounts, reverseAmounts.get(rounds));
                revJson.putOpt(odds, amountJson);
            });

            // 创建一个对象包含 pos 和 rev 的分配结果
            JSONObject posRevJson = new JSONObject();
            posRevJson.putOpt("positive", posJson);
            posRevJson.putOpt("reverse", revJson);

            // 将 oddsKey 对应的结果放入最终的结果中
            resultJson.putOpt(String.valueOf(oddsKey), posRevJson);
        });

        return resultJson;
    }

    /**
     * 双面金额分配
     * @param oddsMap
     * @param positiveAccounts
     * @param reverseAccounts
     * @param plan
     * @return
     */
    public static JSONObject twoSidedDistributeAmount(Map<String, Map<String, List<String>>> oddsMap, List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts, PlanDoubleVO plan, Integer rounds) {
        // 遍历 oddsMap 中的每个 odds 数据
        // 分配 positive 账户的金额
        JSONObject posJson = new JSONObject();
        Map<String, List<String>> posOddsKey = oddsMap.get("positive");
        List<Integer> positiveAmounts = formatIds(plan.getPositiveAmount());
        List<Integer> reverseAmounts = formatIds(plan.getReverseAmount());
        posOddsKey.forEach((odds, accounts) -> {
            JSONObject amountJson = distributeAmountsForAccounts(positiveAccounts, positiveAmounts.get(rounds));
            posJson.putOpt(odds, amountJson);
        });

        // 分配 reverse 账户的金额
        JSONObject revJson = new JSONObject();
        Map<String, List<String>> revOddsKey = oddsMap.get("reverse");
        revOddsKey.forEach((odds, accounts) -> {
            JSONObject amountJson = distributeAmountsForAccounts(reverseAccounts, reverseAmounts.get(rounds));
            revJson.putOpt(odds, amountJson);
        });

        // 创建一个对象包含 pos 和 rev 的分配结果
        JSONObject posRevJson = new JSONObject();
        posRevJson.putOpt("positive", posJson);
        posRevJson.putOpt("reverse", revJson);

        return posRevJson;
    }

    private static JSONObject distributeAmountsForAccounts(List<UserConfig> accounts, int totalAmount) {
        JSONObject amountJson = new JSONObject();
        int betNum = accounts.size();

        if (totalAmount <= 0) {
            log.info("翻倍计划-投注金额必须大于0");
            return amountJson;
        }

        if (betNum == 0) {
            log.info("翻倍计划-没有可用的投注账号");
            return amountJson;
        }

        // 初始分配：保证每个账号至少分配2个单位
        int initialShare = 5;
        int remainingAmount = totalAmount - betNum * initialShare;

        if (remainingAmount < 0) {
            log.info("翻倍计划-金额不足以保证每个投注至少 2 元");
            return amountJson;
        }

        // 随机分配权重
        int[] weights = new int[betNum];
        int totalWeight = 0;
        Random random = ThreadLocalRandom.current();
        for (int i = 0; i < betNum; i++) {
            weights[i] = random.nextInt(100) + 1; // 确保权重为正整数
            totalWeight += weights[i];
        }

        // 分配剩余金额
        int[] distributedAmounts = new int[betNum];
        int distributedSum = 0;
        for (int i = 0; i < betNum; i++) {
            if (i == betNum - 1) {
                // 最后一个账号分配剩余金额，避免误差
                distributedAmounts[i] = remainingAmount - distributedSum;
            } else {
                distributedAmounts[i] = (int) Math.floor((double) remainingAmount * weights[i] / totalWeight);
                distributedSum += distributedAmounts[i];
            }
        }

        // 最终金额分配
        for (int i = 0; i < betNum; i++) {
            int finalAmount = distributedAmounts[i] + initialShare;
            amountJson.putOpt(accounts.get(i).getAccount(), finalAmount);
        }

        // 校验并修正总金额
        int actualTotal = amountJson.values().stream().mapToInt(value -> (int) value).sum();
        if (actualTotal != totalAmount) {
            log.warn("翻倍计划-分配金额:{}不等于配置金额:{},进行强制修正", actualTotal, totalAmount);
            int adjustment = totalAmount - actualTotal;

            if (adjustment > 0) {
                // 情况1：actualTotal < totalAmount
                String lastAccount = accounts.get(betNum - 1).getAccount();
                int correctedAmount = amountJson.getInt(lastAccount) + adjustment;

                if (correctedAmount > 0) {
                    amountJson.putOpt(lastAccount, correctedAmount);
                } else {
                    log.warn("翻倍计划-分配金额小于等于0，进行强制修正");
                    // 强制修正逻辑：分配最小值5，剩余分散到其他账户
                    int extraAdjustment = -correctedAmount;
                    amountJson.putOpt(lastAccount, initialShare); // 最小分配5
                    for (int i = 0; i < betNum - 1; i++) {
                        String account = accounts.get(i).getAccount();
                        int currentAmount = amountJson.getInt(account);
                        if (currentAmount > 2) {
                            int toAdjust = Math.min(extraAdjustment, currentAmount - 1);
                            amountJson.putOpt(account, currentAmount - toAdjust);
                            extraAdjustment -= toAdjust;
                            if (extraAdjustment == 0) break;
                        }
                    }
                    if (extraAdjustment > 0) {
                        log.warn("翻倍计划-无法完全分散调整金额，当前剩余差值: {}", extraAdjustment);
                    }
                }
            } else {
                // 情况2：actualTotal > totalAmount
                adjustment = -adjustment; // 转为正值处理
                log.warn("翻倍计划-分配金额超出预算，进行调整");

                // 从金额最大的账号开始调整
                Map<String, Integer> amountMap = new HashMap<>();
                for (String key : amountJson.keySet()) {
                    amountMap.put(key, amountJson.getInt(key));
                }
                List<Map.Entry<String, Integer>> sortedAccounts = amountMap.entrySet().stream()
                        .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue())) // 降序排序
                        .toList();

                for (Map.Entry<String, Integer> entry : sortedAccounts) {
                    String account = entry.getKey();
                    int currentAmount = entry.getValue();

                    if (currentAmount > 1) {
                        int toAdjust = Math.min(adjustment, currentAmount - 1);
                        amountJson.putOpt(account, currentAmount - toAdjust);
                        adjustment -= toAdjust;
                    }

                    if (adjustment == 0) break;
                }

                if (adjustment > 0) {
                    log.error("翻倍计划-调整后仍有剩余差值，逻辑可能有问题，剩余差值: {}", adjustment);
                }
            }
        }

        // 再次校验，确保所有分配金额大于2
        for (int i = 0; i < betNum; i++) {
            String account = accounts.get(i).getAccount();
            int amount = amountJson.getInt(account);
            if (amount < initialShare) {
                log.error("翻倍计划-分配金额出现小于5的情况 accounts大小:{}, totalAmount:{}, 分配结果:{}", accounts.size(), totalAmount, amountJson);
            }
        }
        return amountJson;
    }

}
