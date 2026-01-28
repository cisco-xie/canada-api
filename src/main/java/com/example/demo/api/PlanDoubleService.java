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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@Component
public class PlanDoubleService {

    /**
     * 投注位置信息（新API使用，不再使用betKey格式）
     */
    private static class BettingPositionInfo {
        private Integer positionId;      // 位置ID（和值、球号、位置号等）
        private String locationNum;      // 位置编号（用于位置开号投注等）
        private String positionNum;      // 投注号码
        private String idIndex;         // 新API的idIndex（根据游戏类型计算）
        private String rate;            // 赔率（从rate数组获取）
        private Integer ballNumber;      // 球号（用于OPN等）
        private String betType;         // 投注类型（大/小/单/双/龙/虎，用于双面投注）
        private Integer firstDigit;     // 第一位数字（用于二字定位、三字定位）
        private Integer secondDigit;    // 第二位数字（用于二字定位、三字定位）
        private Integer thirdDigit;     // 第三位数字（用于三字定位）
        
        // Getters and Setters
        public Integer getPositionId() { return positionId; }
        public void setPositionId(Integer positionId) { this.positionId = positionId; }
        public String getLocationNum() { return locationNum; }
        public void setLocationNum(String locationNum) { this.locationNum = locationNum; }
        public String getPositionNum() { return positionNum; }
        public void setPositionNum(String positionNum) { this.positionNum = positionNum; }
        public String getIdIndex() { return idIndex; }
        public void setIdIndex(String idIndex) { this.idIndex = idIndex; }
        public String getRate() { return rate; }
        public void setRate(String rate) { this.rate = rate; }
        public Integer getBallNumber() { return ballNumber; }
        public void setBallNumber(Integer ballNumber) { this.ballNumber = ballNumber; }
        public String getBetType() { return betType; }
        public void setBetType(String betType) { this.betType = betType; }
        public Integer getFirstDigit() { return firstDigit; }
        public void setFirstDigit(Integer firstDigit) { this.firstDigit = firstDigit; }
        public Integer getSecondDigit() { return secondDigit; }
        public void setSecondDigit(Integer secondDigit) { this.secondDigit = secondDigit; }
        public Integer getThirdDigit() { return thirdDigit; }
        public void setThirdDigit(Integer thirdDigit) { this.thirdDigit = thirdDigit; }
    }

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

    private static Map<Integer, List<JSONObject>> getUseMonitorInfos(Map<Integer, List<JSONObject>> useMonitorInfos) {
        return useMonitorInfos;
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
                Map<String, List<BetNew>> finalOrder = new ConcurrentHashMap<>();
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
                            drawNumber.compareAndSet(null, periodJson.getStr("gid"));
                            drawTime.compareAndSet(null, periodJson.getLong("drawTime"));
                            closeTime.compareAndSet(null, periodJson.getLong("closeTime"));
                        }
                        // 校验封盘时间
                        if (!validateCloseTime(closeTime.get(), username, plan.getLottery(), drawNumber.get())) {
                            return;
                        }

                        // 获取期数锁
                        RLock lock = acquireBetLock(drawTime.get(), username, plan, drawNumber.get());
                        if (lock == null) {
                            log.info("翻倍计划-平台用户:{},游戏:{},期数:{},方案:{},当前期数正在执行", username, plan.getLottery(), drawNumber.get(), plan.getName());
                            return;
                        }

                        // 获取正投账号数
                        List<UserConfig> positiveAccounts = getRandomAccount(tokenValidConfigs, plan.getPositiveAccountNum(), 1);
                        // 获取反投账号数
                        // List<UserConfig> reverseAccounts = getRandomAccount(tokenValidConfigs, plan.getReverseAccountNum(), 2);
                        List<UserConfig> reverseAccounts = new ArrayList<>();
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

                        // 投注位置信息（新API：直接存储投注位置，不再使用betKey）
                        // 普通投注位置：Map<位置ID, 投注位置信息>
                        // 位置ID对于不同游戏类型含义不同：
                        // - all: 和值 (0-27)
                        // - OPN: 球号 (1-3)
                        // - TPD/TPC/TPB: 位置号 (1-10)
                        // - DW3: 位置号 (1-10)
                        // - n5: 投注类型索引
                        Map<Integer, BettingPositionInfo> oddsMap = new HashMap<>();
                        // 获取号码
                        List<PositionVO> positions = new ArrayList<>();
                        // 获取大小
                        Map<String, List<Integer>> twoSidedDxPositions = new HashMap<>();
                        // 获取单双
                        Map<String, List<Integer>> twoSidedDsPositions = new HashMap<>();
                        // 获取龙虎
                        Map<String, List<Integer>> twoSidedLhPositions = new HashMap<>();

                        List<Integer> amountKeys = new ArrayList<>();
                        String wtype = plan.getLottery(); // 获取游戏类型
                        
                        if (null != plan.getPositions()) {
                            String lastResult = null;
                            // 如果是类型2则获取最新开奖信息
                            if (1 == plan.getPlanType()) {
                                log.info("翻倍计划-号码投注-平台用户:{},游戏:{},期数:{},wtype:{}", username, plan.getLottery(), drawNumber.get(), wtype);
                                for (PositionVO pos : plan.getPositions()) {
                                    if (pos.getEnable() == 0) continue;
                                    
                                    // 单球1-3（OPN）游戏类型的特殊处理
                                    if ("OPN".equals(wtype)) {
                                        // positionNum 存储的是"第几球"（1、2、3）
                                        // locationNum 存储的是"投注值"（大、小、单、双、0-9，多个值用逗号分隔）
                                        String ballNumStr = pos.getPositionNum();
                                        String betValuesStr = pos.getLocationNum();
                                        
                                        if (StringUtils.isBlank(ballNumStr) || StringUtils.isBlank(betValuesStr)) {
                                            log.warn("翻倍计划-单球1-3-投注位置信息不完整, positionNum:{}, locationNum:{}", ballNumStr, betValuesStr);
                                            continue;
                                        }
                                        
                                        // 解析球号（1、2、3）
                                        int ballNumber;
                                        try {
                                            ballNumber = Integer.parseInt(ballNumStr.trim());
                                            if (ballNumber < 1 || ballNumber > 3) {
                                                log.warn("翻倍计划-单球1-3-球号无效:{}", ballNumber);
                                                continue;
                                            }
                                        } catch (NumberFormatException e) {
                                            log.warn("翻倍计划-单球1-3-球号格式错误:{}", ballNumStr);
                                            continue;
                                        }
                                        
                                        // 解析投注值（大、小、单、双、0-9，多个值用逗号分隔）
                                        List<String> betValues = formatIdsStr(betValuesStr);
                                        for (String betValue : betValues) {
                                            betValue = betValue.trim();
                                            
                                            // 创建投注位置信息
                                            BettingPositionInfo positionInfo = new BettingPositionInfo();
                                            positionInfo.setBallNumber(ballNumber);
                                            positionInfo.setLocationNum(betValuesStr);
                                            positionInfo.setPositionNum(ballNumStr);
                                            
                                            // 判断投注值类型：大、小、单、双、数字（0-9）
                                            if ("大".equals(betValue)) {
                                                positionInfo.setBetType("大");
                                                // 计算idIndex：第一球大=1, 第二球大=15, 第三球大=29
                                                int baseIndex = (ballNumber - 1) * 14 + 1;
                                                positionInfo.setIdIndex(String.valueOf(baseIndex));
                                                positionInfo.setPositionId(baseIndex); // 使用idIndex作为positionId
                                            } else if ("小".equals(betValue)) {
                                                positionInfo.setBetType("小");
                                                // 计算idIndex：第一球小=2, 第二球小=16, 第三球小=30
                                                int baseIndex = (ballNumber - 1) * 14 + 2;
                                                positionInfo.setIdIndex(String.valueOf(baseIndex));
                                                positionInfo.setPositionId(baseIndex);
                                            } else if ("单".equals(betValue)) {
                                                positionInfo.setBetType("单");
                                                // 计算idIndex：第一球单=3, 第二球单=17, 第三球单=31
                                                int baseIndex = (ballNumber - 1) * 14 + 3;
                                                positionInfo.setIdIndex(String.valueOf(baseIndex));
                                                positionInfo.setPositionId(baseIndex);
                                            } else if ("双".equals(betValue)) {
                                                positionInfo.setBetType("双");
                                                // 计算idIndex：第一球双=4, 第二球双=18, 第三球双=32
                                                int baseIndex = (ballNumber - 1) * 14 + 4;
                                                positionInfo.setIdIndex(String.valueOf(baseIndex));
                                                positionInfo.setPositionId(baseIndex);
                                            } else {
                                                // 数字投注（0-9）
                                                try {
                                                    int digit = Integer.parseInt(betValue);
                                                    if (digit < 0 || digit > 9) {
                                                        log.warn("翻倍计划-单球1-3-数字投注值无效:{}", digit);
                                                        continue;
                                                    }
                                                    // 计算idIndex：第一球0-9=5-14, 第二球0-9=19-28, 第三球0-9=33-42
                                                    int baseIndex = (ballNumber - 1) * 14 + 5 + digit;
                                                    positionInfo.setIdIndex(String.valueOf(baseIndex));
                                                    positionInfo.setPositionId(baseIndex);
                                                    positionInfo.setBetType(String.valueOf(digit));
                                                } catch (NumberFormatException e) {
                                                    log.warn("翻倍计划-单球1-3-投注值格式错误:{}", betValue);
                                                    continue;
                                                }
                                            }
                                            
                                            // 获取赔率
                                            if (oddsJson.containsKey("rate")) {
                                                JSONArray rateArray = oddsJson.getJSONArray("rate");
                                                if (rateArray != null) {
                                                    int idIndexInt = Integer.parseInt(positionInfo.getIdIndex());
                                                    if (rateArray.size() > idIndexInt) {
                                                        positionInfo.setRate(rateArray.getStr(idIndexInt));
                                                    }
                                                }
                                            }
                                            
                                            // 使用唯一的positionId作为key（球号*1000 + idIndex）
                                            int uniquePositionId = ballNumber * 1000 + Integer.parseInt(positionInfo.getIdIndex());
                                            oddsMap.put(uniquePositionId, positionInfo);
                                            amountKeys.add(uniquePositionId);
                                        }
                                        positions.add(pos);
                                    } else if ("TPD".equals(wtype)) {
                                        // 检查是否为二字定位（通过检查positionNum是否为TPD/TPC/TPB，或者lottery是否为二字定位类型）
                                        String locationTypeStr = pos.getPositionNum();
                                        boolean isTwoPositionType = "TPD".equals(locationTypeStr) || "TPC".equals(locationTypeStr) || "TPB".equals(locationTypeStr);
                                        boolean isTwoPositionLottery = "TPD".equals(wtype) || "TPC".equals(wtype) || "TPB".equals(wtype);

                                        if (isTwoPositionType || isTwoPositionLottery) {
                                            // 二字定位：positionNum 存储的是定位类型（TPD/TPC/TPB）
                                            // locationNum 存储的是投注位置（0-99，多个值用逗号分隔）
                                            String betPositionsStr = pos.getLocationNum();

                                            if (StringUtils.isBlank(betPositionsStr)) {
                                                log.warn("翻倍计划-二字定位-投注位置不能为空, positionNum:{}, locationNum:{}", locationTypeStr, betPositionsStr);
                                                continue;
                                            }

                                            // 根据positionNum确定实际的定位类型（如果positionNum是TPD/TPC/TPB，则使用它；否则使用wtype）
                                            String actualWtype = wtype;
                                            if (isTwoPositionType) {
                                                actualWtype = locationTypeStr;
                                                log.info("翻倍计划-二字定位-从positionNum提取定位类型, positionNum:{}, actualWtype:{}", locationTypeStr, actualWtype);
                                            } else {
                                                log.warn("翻倍计划-二字定位-positionNum不是有效的定位类型, positionNum:{}, 使用lottery wtype:{}", locationTypeStr, wtype);
                                            }

                                            // 解析投注位置（0-99，多个值用逗号分隔）
                                            List<Integer> betPositions = formatIds(betPositionsStr);
                                            for (Integer betPosition : betPositions) {
                                                // 验证投注位置范围（0-99）
                                                if (betPosition < 0 || betPosition > 99) {
                                                    log.warn("翻倍计划-二字定位-投注位置超出范围，应为0-99:{}", betPosition);
                                                    continue;
                                                }

                                                // 将0-99转换为第一位和第二位数字
                                                // 例如：88 -> 第一位=8, 第二位=8
                                                // 例如：5 -> 第一位=0, 第二位=5
                                                // 例如：99 -> 第一位=9, 第二位=9
                                                int firstDigit = betPosition / 10;
                                                int secondDigit = betPosition % 10;

                                                // 创建投注位置信息
                                                BettingPositionInfo positionInfo = new BettingPositionInfo();
                                                positionInfo.setFirstDigit(firstDigit);
                                                positionInfo.setSecondDigit(secondDigit);
                                                positionInfo.setLocationNum(betPositionsStr);
                                                positionInfo.setPositionNum(locationTypeStr);

                                                // 计算idIndex：idIndex = 第一位 * 10 + 第二位 + 1
                                                int idIndex = firstDigit * 10 + secondDigit + 1;
                                                positionInfo.setIdIndex(String.valueOf(idIndex));

                                                // 使用唯一的positionId作为key（定位类型编码 * 1000 + idIndex）
                                                // TPD=1, TPC=2, TPB=3
                                                int locationTypeCode = 0;
                                                if ("TPD".equals(actualWtype)) {
                                                    locationTypeCode = 1;
                                                } else if ("TPC".equals(actualWtype)) {
                                                    locationTypeCode = 2;
                                                } else if ("TPB".equals(actualWtype)) {
                                                    locationTypeCode = 3;
                                                }
                                                int uniquePositionId = locationTypeCode * 1000 + idIndex;
                                                positionInfo.setPositionId(uniquePositionId);

                                                // 获取赔率（需要根据actualWtype获取对应游戏类型的赔率）
                                                // 注意：这里需要重新获取对应定位类型的赔率，因为不同定位类型的赔率可能不同
                                                if (oddsJson.containsKey("rate")) {
                                                    JSONArray rateArray = oddsJson.getJSONArray("rate");
                                                    if (rateArray != null) {
                                                        if (rateArray.size() > idIndex) {
                                                            positionInfo.setRate(rateArray.getStr(idIndex));
                                                        }
                                                    }
                                                }

                                                oddsMap.put(uniquePositionId, positionInfo);
                                                amountKeys.add(uniquePositionId);
                                            }
                                            positions.add(pos);
                                        }
                                    } else {
                                        // 其他游戏类型的处理（保持原有逻辑）
                                        boolean hasChinese = Validator.hasChinese(pos.getPositionNum());
                                        if (!hasChinese) {
                                            // 没有中文的一概以号码处理
                                            List<Integer> ids = formatIds(pos.getLocationNum());
                                            for (Integer id : ids) {
                                                // 新API：直接根据游戏类型和投注号码计算投注位置信息
                                                BettingPositionInfo positionInfo = new BettingPositionInfo();
                                                positionInfo.setPositionId(id);
                                                positionInfo.setLocationNum(pos.getLocationNum());
                                                positionInfo.setPositionNum(pos.getPositionNum());
                                                
                                                // 根据游戏类型计算idIndex和rate
                                                if ("all".equals(wtype)) {
                                                    // 主势盘：投注号码就是和值
                                                    int sumValue = id;
                                                    positionInfo.setIdIndex(String.valueOf(sumValue + 1));
                                                    // 获取赔率：rate[和值 + 1]
                                                    if (oddsJson.containsKey("rate")) {
                                                        JSONArray rateArray = oddsJson.getJSONArray("rate");
                                                        if (rateArray != null && rateArray.size() > sumValue + 1) {
                                                            positionInfo.setRate(rateArray.getStr(sumValue + 1));
                                                        }
                                                    }
                                                } else {
                                                    // 其他游戏类型：暂时标记，后续在createBetNewList中处理
                                                    positionInfo.setIdIndex(null); // 待计算
                                                    positionInfo.setRate(null); // 待计算
                                                }
                                                
                                                oddsMap.put(id, positionInfo);
                                                amountKeys.add(id);
                                            }
                                            positions.add(pos);
                                        } else {
                                            // 其他游戏类型的双面投注处理
                                            if ("OPN".equals(wtype)) {
                                                // 单球1-3的双面投注：positionNum 是球号，locationNum 是投注类型（大、小、单、双）
                                                String ballNumStr = pos.getPositionNum();
                                                String betTypeStr = pos.getLocationNum();
                                                
                                                if (StringUtils.isBlank(ballNumStr) || StringUtils.isBlank(betTypeStr)) {
                                                    log.warn("翻倍计划-单球1-3双面投注-投注位置信息不完整, positionNum:{}, locationNum:{}", ballNumStr, betTypeStr);
                                                    continue;
                                                }
                                                
                                                // 解析球号（1、2、3）
                                                int ballNumber;
                                                try {
                                                    ballNumber = Integer.parseInt(ballNumStr.trim());
                                                    if (ballNumber < 1 || ballNumber > 3) {
                                                        log.warn("翻倍计划-单球1-3双面投注-球号无效:{}", ballNumber);
                                                        continue;
                                                    }
                                                } catch (NumberFormatException e) {
                                                    log.warn("翻倍计划-单球1-3双面投注-球号格式错误:{}", ballNumStr);
                                                    continue;
                                                }
                                                
                                                // 解析投注类型（大、小、单、双）
                                                List<String> betTypes = formatIdsStr(betTypeStr);
                                                for (String betType : betTypes) {
                                                    betType = betType.trim();
                                                    if ("大".equals(betType) || "小".equals(betType)) {
                                                        // 大小投注
                                                        twoSidedDxPositions.computeIfAbsent(betType, k -> new ArrayList<>()).add(ballNumber);
                                                    } else if ("单".equals(betType) || "双".equals(betType)) {
                                                        // 单双投注
                                                        twoSidedDsPositions.computeIfAbsent(betType, k -> new ArrayList<>()).add(ballNumber);
                                                    } else {
                                                        log.warn("翻倍计划-单球1-3双面投注-投注类型无效:{}", betType);
                                                    }
                                                }
                                            } else {
                                                // 其他游戏类型的双面投注（保持原有逻辑）
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
                                            // 新API：直接根据游戏类型和投注位置创建BettingPositionInfo
                                            BettingPositionInfo positionInfo = new BettingPositionInfo();
                                            positionInfo.setPositionId(id);
                                            positionInfo.setLocationNum(pos.getLocationNum());
                                            positionInfo.setPositionNum(pos.getPositionNum());
                                            
                                            // 位置开号投注：根据开奖号码和位置计算投注位置信息
                                            List<Integer> locations = new ArrayList<>();
                                            List<Integer> locationIds = formatIds(pos.getLocationNum());
                                            for (Integer locationId : locationIds) {
                                                locations.add(amountKeys.get(locationId - 1));
                                            }
                                            
                                            // 根据游戏类型计算idIndex和rate（待实现具体逻辑）
                                            // 暂时标记，后续在createBetNewList中根据实际投注号码计算
                                            positionInfo.setIdIndex(null); // 待计算
                                            positionInfo.setRate(null); // 待计算
                                            
                                            oddsMap.put(id, positionInfo);
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
                                                // 新API：直接根据游戏类型和投注位置创建BettingPositionInfo
                                                BettingPositionInfo positionInfo = new BettingPositionInfo();
                                                positionInfo.setPositionId(posNumId);
                                                positionInfo.setLocationNum(pos.getLocationNum());
                                                positionInfo.setPositionNum(pos.getPositionNum());
                                                
                                                // 根据游戏类型计算idIndex和rate（待实现具体逻辑）
                                                positionInfo.setIdIndex(null); // 待计算
                                                positionInfo.setRate(null); // 待计算
                                                
                                                oddsMap.put(posNumId, positionInfo);
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
                                                // 新API：直接根据游戏类型和投注位置创建BettingPositionInfo
                                                BettingPositionInfo positionInfo = new BettingPositionInfo();
                                                positionInfo.setPositionId(posNumId);
                                                positionInfo.setLocationNum(pos.getLocationNum());
                                                positionInfo.setPositionNum(pos.getPositionNum());
                                                
                                                // 根据游戏类型计算idIndex和rate（待实现具体逻辑）
                                                positionInfo.setIdIndex(null); // 待计算
                                                positionInfo.setRate(null); // 待计算
                                                
                                                oddsMap.put(posNumId, positionInfo);
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
                                        dresults = handicapApi.dresults(username, tokenValidConfigs.get(0).getId(), null, 0);
                                    }
                                    for (Object dresult : dresults) {
                                        JSONObject dresultJson = JSONUtil.parseObj(dresult);
                                        String drawTimeNew = dresultJson.getStr("drawTime");
                                        long between = LocalDateTimeUtil.between(LocalDateTime.now(), LocalDateTimeUtil.parse(drawTimeNew, DatePattern.NORM_DATETIME_PATTERN), ChronoUnit.SECONDS);
                                        // 如果最新的开奖时间小于当前时间，between结果为负
                                        if (between < 0) {
                                            // 获取最新开奖结果并插入缓存中
                                            dresults = handicapApi.dresults(username, tokenValidConfigs.get(0).getId(), null, 0);
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
                                            // 新API：直接根据游戏类型和投注位置创建BettingPositionInfo
                                            BettingPositionInfo positionInfo = new BettingPositionInfo();
                                            positionInfo.setPositionId(id);
                                            positionInfo.setLocationNum(locationNumBuilder.toString());
                                            positionInfo.setPositionNum(pos.getPositionNum());
                                            
                                            // 根据游戏类型计算idIndex和rate（待实现具体逻辑）
                                            positionInfo.setIdIndex(null); // 待计算
                                            positionInfo.setRate(null); // 待计算
                                            
                                            oddsMap.put(id, positionInfo);
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
                                                // 新API：直接根据游戏类型和投注位置创建BettingPositionInfo
                                                BettingPositionInfo positionInfo = new BettingPositionInfo();
                                                positionInfo.setPositionId(location);
                                                positionInfo.setLocationNum(String.valueOf(location));
                                                positionInfo.setPositionNum(pos.getPositionNum());
                                                
                                                // 根据游戏类型计算idIndex和rate（待实现具体逻辑）
                                                positionInfo.setIdIndex(null); // 待计算
                                                positionInfo.setRate(null); // 待计算
                                                
                                                oddsMap.put(location, positionInfo);
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
                            // 记录当前计划对应的monitorInfos（转换为List用于useMonitorInfos）
                            useMonitorInfos.put(plan.getId(), monitorInfos);
                            // 使用 CountDownLatch 等待所有延迟任务完成
                            JSONObject twoSidedAmountJson = twoSidedAmountJson(oddsJson, wtype, twoSidedDxPositions, twoSidedDsPositions, twoSidedLhPositions, allAccounts, plan, rounds);
                            JSONObject amountJson = distributeAmount(oddsMap, oddsJson, wtype, allAccounts, plan, rounds);
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
                            // 记录当前计划对应的monitorInfos（转换为List用于useMonitorInfos）
                            useMonitorInfos.put(plan.getId(), monitorInfos);
                            // 使用 CountDownLatch 等待所有延迟任务完成
                            // CountDownLatch latch = new CountDownLatch(allAccounts.size()); // 初始化为需要执行的任务数
                            JSONObject twoSidedAmountJson = twoSidedAmountJson(oddsJson, wtype, twoSidedDxPositions, twoSidedDsPositions, twoSidedLhPositions, allAccounts, plan, rounds);
                            JSONObject amountJson = distributeAmount(oddsMap, oddsJson, wtype, allAccounts, plan, rounds);
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
                            // 配置监控轮次
                            roundsMap.put(plan.getId(), rounds);
                            // 记录当前计划对应的monitorInfos（转换为List用于useMonitorInfos）
                            getUseMonitorInfos(useMonitorInfos).put(plan.getId(), monitorInfos);
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
                                JSONObject twoSidedAmountJson = twoSidedAmountJson(oddsJson, wtype, twoSidedDxPositions, twoSidedDsPositions, twoSidedLhPositions, allAccounts, plan, rounds);
                                JSONObject amountJson = distributeAmount(oddsMap, oddsJson, wtype, allAccounts, plan, monitorInfoJson.getInt("rounds"));
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
                            String account = userConfig.getAccount();
                            
                            // 对于二字定位，finalOrder的key可能是"account_wtype"格式，需要检查所有相关的key
                            boolean isTwoPosition = "TPD".equals(lottery) || "TPC".equals(lottery) || "TPB".equals(lottery);
                            List<String> orderKeys = new ArrayList<>();
                            
                            if (isTwoPosition) {
                                // 二字定位：检查所有定位类型的key
                                for (String locationType : new String[]{"TPD", "TPC", "TPB"}) {
                                    String key = account + "_" + locationType;
                                    if (finalOrder.containsKey(key) && finalOrder.get(key) != null && !finalOrder.get(key).isEmpty()) {
                                        orderKeys.add(key);
                                    }
                                }
                                // 也检查原始的account key（兼容性）
                                if (finalOrder.containsKey(account) && finalOrder.get(account) != null && !finalOrder.get(account).isEmpty()) {
                                    orderKeys.add(account);
                                }
                            } else {
                                // 非二字定位：使用原有的account key
                                if (finalOrder.containsKey(account) && finalOrder.get(account) != null && !finalOrder.get(account).isEmpty()) {
                                    orderKeys.add(account);
                                }
                            }
                            
                            if (orderKeys.isEmpty()) {
                                log.info("翻倍计划-发起下单,当前合并投注没有该账号的投注请求,无需投注, 平台用户:{}, 账号:{}, 期数:{}, 合并方案:{}", username, account, drawNumber, finalOrder);
                                return;
                            }
                            
                            String isBetRedisKey = KeyUtil.genKey(
                                    RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                                    lottery,
                                    ToDayRangeUtil.getToDayRange(),
                                    drawNumber.get(),
                                    "success",
                                    username,
                                    account,
                                    "merge"
                            );
                            // 判断是否已下注
                            JSONObject exists = JSONUtil.parseObj(redisson.getBucket(isBetRedisKey).get());
                            if (!exists.isEmpty() && exists.getInt("type") == 2) {
                                log.info("翻倍计划-平台用户:{},游戏:{},期数:{},账号:{},方案:{},已下过注,不再重复下注", username, lottery, drawNumber.get(), account
                                        , "合并式方案");
                                return;
                            }
                            
                            log.info("翻倍计划-发起下单, 平台用户:{}, 账号:{}, 期数:{}, 合并方案:{}, 账号token:{}, 订单keys:{}", 
                                    username, account, drawNumber, finalOrder, userConfig.getToken(), orderKeys);
                            
                            // 对于二字定位，需要按定位类型分组提交
                            if (isTwoPosition) {
                                // 按定位类型分组提交
                                boolean allSuccess = true;
                                for (String orderKey : orderKeys) {
                                    List<BetNew> betNewList = finalOrder.get(orderKey);
                                    if (betNewList == null || betNewList.isEmpty()) {
                                        continue;
                                    }
                                    
                                    // 从key中提取定位类型
                                    String actualWtype = lottery; // 默认使用计划的lottery
                                    if (orderKey.contains("_")) {
                                        String[] parts = orderKey.split("_");
                                        if (parts.length >= 2) {
                                            String wtypePart = parts[parts.length - 1];
                                            if ("TPD".equals(wtypePart) || "TPC".equals(wtypePart) || "TPB".equals(wtypePart)) {
                                                actualWtype = wtypePart;
                                            }
                                        }
                                    }
                                    
                                    log.info("翻倍计划-合并下单-二字定位, 平台用户:{}, 账号:{}, 期数:{}, 定位类型:{}, BetNew数量:{}",
                                            username, account, drawNumber, actualWtype, betNewList.size());
                                    
                                    // 为每个定位类型单独调用submitOrderForWtype
                                    String result = submitOrderForWtype(username, userConfig, enabledPlanDoubles.get(0), betNewList, drawNumber.get(), actualWtype, closeTime.get());
                                    if (result != null) {
                                        JSONObject resultJson = JSONUtil.parseObj(result);
                                        if (resultJson.getInt("status", -1) != 0) {
                                            allSuccess = false;
                                        }
                                    } else {
                                        allSuccess = false;
                                    }
                                }
                                
                                // 统一处理结果
                                if (allSuccess) {
                                    JSONObject resultObj = new JSONObject();
                                    resultObj.putOpt("status", 0);
                                    resultObj.putOpt("lottery", lottery);
                                    resultObj.putOpt("drawNumber", drawNumber.get());
                                    resultObj.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                                    resultObj.putOpt("account", account);
                                    resultObj.putOpt("isRepair", 0);
                                    resultObj.putOpt("message", "盘口投注成功");
                                    handleOrderResult(username, JSONUtil.toJsonStr(resultObj), userConfig, lottery, drawNumber.get(), enabledPlanDoubles, successAccounts, failedAccounts, successCount, failureCount, successUserCount, failureUserCount);
                                } else {
                                    JSONObject resultObj = new JSONObject();
                                    resultObj.putOpt("status", 1);
                                    resultObj.putOpt("lottery", lottery);
                                    resultObj.putOpt("drawNumber", drawNumber.get());
                                    resultObj.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
                                    resultObj.putOpt("account", account);
                                    resultObj.putOpt("isRepair", 0);
                                    resultObj.putOpt("message", "部分定位类型投注失败");
                                    handleOrderResult(username, JSONUtil.toJsonStr(resultObj), userConfig, lottery, drawNumber.get(), enabledPlanDoubles, successAccounts, failedAccounts, successCount, failureCount, successUserCount, failureUserCount);
                                }
                            } else {
                                // 非二字定位：使用原有逻辑
                                String result = mergeSubmitOrder(username, userConfig, enabledPlanDoubles, finalOrder.get(account), lottery, drawNumber.get(), useMonitorIds, closeTime.get());
                                // 结果解析
                                handleOrderResult(username, result, userConfig, lottery, drawNumber.get(), enabledPlanDoubles, successAccounts, failedAccounts, successCount, failureCount, successUserCount, failureUserCount);
                            }
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

                    long drawTimeDelay = drawTime.get();
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
    private void retryBet(String username, PlanDoubleVO plan, Map<String, List<BetNew>> finalOrder, String monitorId, JSONObject monitorInfo, String drawNumber, long closeTime, Long drawTime,
                          JSONObject oddsJson, List<UserConfig> allAccounts, List<UserConfig> positiveAccounts, List<UserConfig> reverseAccounts, List<UserConfig> successAccounts, List<UserConfig> failedAccounts, Map<String, Map<Integer, String>> useMonitorIds, Map<String, Map<Integer, String>> failedMonitorIds, List<PositionVO> positions, JSONObject amountJson, JSONObject twoSidedAmountJson,
                          AtomicInteger successCount, AtomicInteger failureCount, AtomicInteger successUserCount, AtomicInteger failureUserCount, List<Integer> amountKeys, ExecutorService executorBetService) {

        log.info("翻倍计划-开始执行投注, 平台用户:{},游戏:{},期数:{},方案:{}, 监控ID: {}, 监控信息: {}, 执行用户: {}", username, plan.getLottery(), drawNumber, plan.getName(), monitorId, monitorInfo, allAccounts);
        // 获取游戏类型
        String wtype = plan.getLottery();
        // 计算新的投注金额（合并正投和反投金额，统一使用所有账号）
        List<Integer> positiveAmounts = formatIds(plan.getPositiveAmount());
        int totalAmount = positiveAmounts.get(monitorInfo.getInt("rounds"));
        JSONObject newTotalAmountJson = distributeAmountsForAccounts(allAccounts, totalAmount);

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

                        // 创建下注请求参数（使用BetNew）
                        String orderKey = KeyUtil.genKey(
                                RedisConstants.USER_BET_ORDER_DOUBLE_PREFIX,
                                plan.getLottery(),
                                username,
                                String.valueOf(plan.getId()),
                                "all", // 不再区分正投/反投
                                userConfig.getAccount(),
                                monitorInfo.getStr("id")
                        );
                        
                        List<BetNew> betNewList;
                        if (1 == plan.getDoubleType()) {
                            if (plan.getMonitorNum() <= 1) {
                                betNewList = createBetNewList(username, monitorId, plan, oddsJsonNew, drawNumber, amountJson, twoSidedAmountJson, userConfig, wtype);
                            } else {
                                Object objRedis = businessUserRedissonClient.getBucket(orderKey).get();
                                if (Objects.isNull(objRedis)) {
                                    betNewList = createBetNewList(username, monitorId, plan, oddsJsonNew, drawNumber, amountJson, twoSidedAmountJson, userConfig, wtype);
                                    businessUserRedissonClient.getBucket(orderKey).set(JSONUtil.toJsonStr(betNewList));
                                } else {
                                    log.info("翻倍老号-开始获取老号的下单记录, 执行用户: {}, 老号下单记录: {}", userConfig.getAccount(), objRedis);
                                    // 获取老号的下单记录
                                    betNewList = JSONUtil.toList(String.valueOf(objRedis), BetNew.class);
                                    // 更新金额
                                    int accountAmount = newTotalAmountJson.getInt(userConfig.getAccount());
                                    for (BetNew betNew : betNewList) {
                                        betNew.setAmount(String.valueOf(accountAmount));
                                    }
                                }
                            }
                        } else if (2 == plan.getDoubleType()) {
                            betNewList = createBetNewList(username, monitorId, plan, oddsJsonNew, drawNumber, amountJson, twoSidedAmountJson, userConfig, wtype);
                        } else if (3 == plan.getDoubleType()) {
                            if (plan.getMonitorNum() <= 1) {
                                betNewList = createBetNewList(username, monitorId, plan, oddsJsonNew, drawNumber, amountJson, twoSidedAmountJson, userConfig, wtype);
                            } else {
                                Object objRedis = businessUserRedissonClient.getBucket(orderKey).get();
                                if (Objects.isNull(objRedis)) {
                                    betNewList = createBetNewList(username, monitorId, plan, oddsJsonNew, drawNumber, amountJson, twoSidedAmountJson, userConfig, wtype);
                                    businessUserRedissonClient.getBucket(orderKey).set(JSONUtil.toJsonStr(betNewList));
                                } else {
                                    log.info("翻老投新-开始获取老号的下单记录, 执行用户: {}, 老号下单记录: {}", userConfig.getAccount(), objRedis);
                                    // 获取翻老投新的下单记录
                                    betNewList = JSONUtil.toList(String.valueOf(objRedis), BetNew.class);
                                    // 更新金额
                                    int accountAmount = newTotalAmountJson.getInt(userConfig.getAccount());
                                    for (BetNew betNew : betNewList) {
                                        betNew.setAmount(String.valueOf(accountAmount));
                                    }
                                }
                            }
                        } else {
                            betNewList = createBetNewList(username, monitorId, plan, oddsJsonNew, drawNumber, amountJson, twoSidedAmountJson, userConfig, wtype);
                        }
                        
                        if (betNewList == null || betNewList.isEmpty()) {
                            log.info("翻倍计划-平台用户:{},游戏:{},期数:{},账号:{},方案:{},没有可下注的号码", username, plan.getLottery(), drawNumber, userConfig.getAccount(), plan.getName());
                            return;
                        }
                        
                        // 检查是否为二字定位（通过检查amountJson中的positionId是否包含定位类型编码）
                        // 对于二字定位，positionId = locationTypeCode * 1000 + idIndex（TPD=1, TPC=2, TPB=3）
                        boolean isTwoPosition = false;
                        Map<String, List<BetNew>> betNewListGrouped = new HashMap<>();
                        
                        if (amountJson != null && !amountJson.isEmpty()) {
                            for (String positionId : amountJson.keySet()) {
                                try {
                                    int posId = Integer.parseInt(positionId);
                                    int locationTypeCode = posId / 1000;
                                    if (locationTypeCode >= 1 && locationTypeCode <= 3) {
                                        isTwoPosition = true;
                                        break;
                                    }
                                } catch (NumberFormatException e) {
                                    // 不是数字，跳过
                                }
                            }
                        }
                        
                        if (isTwoPosition) {
                            // 二字定位：按定位类型分组
                            if (amountJson != null && !amountJson.isEmpty()) {
                                amountJson.forEach((positionId, positionValueObj) -> {
                                    JSONObject positionValue = JSONUtil.parseObj(positionValueObj);
                                    positionValue.forEach((idIndex, amountValueObj) -> {
                                        JSONObject amountValue = JSONUtil.parseObj(amountValueObj);
                                        Long amount = amountValue.getLong(userConfig.getAccount());
                                        if (amount == null || amount <= 0) {
                                            return;
                                        }
                                        
                                        // 从positionId提取定位类型
                                        String actualWtype = wtype;
                                        try {
                                            int posId = Integer.parseInt(positionId);
                                            int locationTypeCode = posId / 1000;
                                            if (locationTypeCode == 1) {
                                                actualWtype = "TPD";
                                            } else if (locationTypeCode == 2) {
                                                actualWtype = "TPC";
                                            } else if (locationTypeCode == 3) {
                                                actualWtype = "TPB";
                                            }
                                        } catch (NumberFormatException e) {
                                            // 如果解析失败，使用默认的wtype
                                        }
                                        
                                        // 获取赔率
                                        String rate = null;
                                        if (oddsJsonNew.containsKey("rate")) {
                                            JSONArray rateArray = oddsJsonNew.getJSONArray("rate");
                                            if (rateArray != null) {
                                                try {
                                                    int index = Integer.parseInt(idIndex);
                                                    if (index >= 0 && index < rateArray.size()) {
                                                        rate = rateArray.getStr(index);
                                                    }
                                                } catch (NumberFormatException e) {
                                                    log.warn("翻倍计划-idIndex不是数字:{}", idIndex);
                                                }
                                            }
                                        }
                                        
                                        // 创建BetNew对象
                                        BetNew betNew = new BetNew();
                                        betNew.setIdIndex(idIndex);
                                        betNew.setAmount(String.valueOf(amount));
                                        betNew.setRate(rate != null ? rate : "0");
                                        
                                        // 按定位类型分组
                                        betNewListGrouped.computeIfAbsent(actualWtype, k -> new ArrayList<>()).add(betNew);
                                    });
                                });
                            }
                            
                            // 为每个定位类型单独处理
                            for (Map.Entry<String, List<BetNew>> entry : betNewListGrouped.entrySet()) {
                                String actualWtype = entry.getKey();
                                List<BetNew> groupedBetNewList = entry.getValue();
                                
                                if (groupedBetNewList == null || groupedBetNewList.isEmpty()) {
                                    continue;
                                }
                                
                                log.info("翻倍计划-平台用户:{},游戏:{},期数:{},账号:{},方案:{},定位类型:{},BetNew数量:{},将投注请求插入到finalOrder中", 
                                        username, plan.getLottery(), drawNumber, userConfig.getAccount(), plan.getName(), actualWtype, groupedBetNewList.size());
                                
                                // 为每个定位类型创建唯一的key（account + wtype）
                                String finalOrderKey = account + "_" + actualWtype;
                                
                                // 安全获取或创建BetNew列表
                                finalOrder.compute(finalOrderKey, (k, existingList) -> {
                                    if (existingList == null) {
                                        return new ArrayList<>(groupedBetNewList);
                                    } else {
                                        existingList.addAll(groupedBetNewList);
                                        return existingList;
                                    }
                                });
                                
                                // 记录订单请求（每个定位类型单独记录）
                                JSONObject orderRecord = new JSONObject();
                                orderRecord.put("lottery", plan.getLottery());
                                orderRecord.put("drawNumber", drawNumber);
                                orderRecord.put("wtype", actualWtype); // 使用实际的定位类型
                                orderRecord.put("betNewList", groupedBetNewList);
                                
                                redisson.getBucket(KeyUtil.genKey(
                                        RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                                        plan.getLottery(),
                                        ToDayRangeUtil.getToDayRange(),
                                        drawNumber,
                                        username,
                                        String.valueOf(plan.getId()),
                                        monitorId,
                                        userConfig.getAccount(),
                                        actualWtype // 添加定位类型后缀
                                )).set(JSONUtil.toJsonStr(orderRecord), Duration.ofHours(24));
                            }
                        } else {
                            // 非二字定位：使用原有逻辑
                            log.info("翻倍计划-平台用户:{},游戏:{},期数:{},账号:{},方案:{},BetNew数量:{},将投注请求插入到finalOrder中,插入前:{}", 
                                    username, plan.getLottery(), drawNumber, userConfig.getAccount(), plan.getName(), betNewList.size(), finalOrder);
                            
                            // 安全获取或创建BetNew列表
                            finalOrder.compute(account, (k, existingList) -> {
                                if (existingList == null) {
                                    return new ArrayList<>(betNewList);
                                } else {
                                    existingList.addAll(betNewList);
                                    return existingList;
                                }
                            });
                            
                            log.info("翻倍计划-平台用户:{},游戏:{},期数:{},账号:{},方案:{},BetNew数量:{},将投注请求插入到finalOrder中,插入后:{}", 
                                    username, plan.getLottery(), drawNumber, userConfig.getAccount(), plan.getName(), betNewList.size(), finalOrder);

                            // 记录订单请求
                            JSONObject orderRecord = new JSONObject();
                            orderRecord.put("lottery", plan.getLottery());
                            orderRecord.put("drawNumber", drawNumber);
                            orderRecord.put("wtype", wtype);
                            orderRecord.put("betNewList", betNewList);
                            
                            redisson.getBucket(KeyUtil.genKey(
                                    RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                                    plan.getLottery(),
                                    ToDayRangeUtil.getToDayRange(),
                                    drawNumber,
                                    username,
                                    String.valueOf(plan.getId()),
                                    monitorId,
                                    userConfig.getAccount()
                            )).set(JSONUtil.toJsonStr(orderRecord), Duration.ofHours(24));
                        }

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
    /**
     * 计算单个球的中奖idIndex（用于单球1-3游戏类型）
     * @param ballNumber 球号（1、2、3）
     * @param ballValue 开奖号码（0-9）
     * @param winningIdIndexes 中奖idIndex集合（输出参数）
     */
    private void calculateWinningIdIndexesForBall(int ballNumber, int ballValue, Set<String> winningIdIndexes) {
        if (ballNumber < 1 || ballNumber > 3 || ballValue < 0 || ballValue > 9) {
            log.warn("翻倍计划-单球1-3中号检测-参数无效, ballNumber:{}, ballValue:{}", ballNumber, ballValue);
            return;
        }
        
        int baseIndex = (ballNumber - 1) * 14;
        
        // 计算大小单双的中奖idIndex
        // 大：>=5，小：<5
        if (ballValue >= 5) {
            winningIdIndexes.add(String.valueOf(baseIndex + 1)); // 大
        } else {
            winningIdIndexes.add(String.valueOf(baseIndex + 2)); // 小
        }
        
        // 单双：单数=1,3,5,7,9，双数=0,2,4,6,8
        if (ballValue % 2 == 1) {
            winningIdIndexes.add(String.valueOf(baseIndex + 3)); // 单
        } else {
            winningIdIndexes.add(String.valueOf(baseIndex + 4)); // 双
        }
        
        // 数字投注：0-9对应的idIndex = baseIndex + 5 + ballValue
        winningIdIndexes.add(String.valueOf(baseIndex + 5 + ballValue));
    }
    
    private void winCheck(String username, List<UserConfig> positiveAccounts, PlanDoubleVO plan, String drawNumber, String monitorId, int monitorInfoIndex, String orderPositiveKey, String orderReverseKey) {

        Map<String, List<String>> gameContentsMap = new HashMap<>();
        List<String> balls = new ArrayList<>();
        for (UserConfig userConfig : positiveAccounts) {
            // 获取指定期数开奖信息（使用新API的odds方法）
            String drawKey = KeyUtil.genKey(
                    RedisConstants.DRAW_RESULT_PREFIX,
                    plan.getLottery(),
                    drawNumber
            );
            JSONObject drawResult = JSONUtil.parseObj(businessUserRedissonClient.getBucket(drawKey).get());
            
            // 如果缓存中没有开奖信息，或者开奖信息不完整，则调用odds API获取
            if (drawResult == null || drawResult.isEmpty() || 
                StringUtils.isBlank(drawResult.getStr("bnum")) || 
                !drawNumber.equals(drawResult.getStr("gid"))) {
                
                // 使用odds方法获取开奖信息（默认使用主势盘all）
                String wtype = "all"; // 默认使用主势盘
                try {
                    String oddsResult = handicapApi.odds(username, userConfig.getId(), wtype);
                    JSONObject oddsJson = JSONUtil.parseObj(oddsResult);
                    
                    if (oddsJson.getInt("title", 0) == 1) {
                        // 检查返回的期数是否匹配
                        String currentGid = oddsJson.getStr("gid");
                        String currentBr = oddsJson.getStr("br"); // 上一期期数
                        String currentBnum = oddsJson.getStr("bnum"); // 上一期开奖号码
                        
                        // 如果当前期数匹配，说明已经开奖，可以从bnum获取开奖结果
                        // 如果br匹配，说明bnum就是我们要找的开奖结果
                        if (drawNumber.equals(currentGid) || drawNumber.equals(currentBr)) {
                            drawResult = new JSONObject();
                            drawResult.putOpt("gid", drawNumber.equals(currentBr) ? currentBr : currentGid);
                            drawResult.putOpt("bnum", currentBnum);
                            drawResult.putOpt("drawNumber", drawNumber);
                            
                            // 解析开奖号码（格式：5,6,6）
                            if (StringUtils.isNotBlank(currentBnum)) {
                                String[] ballArray = currentBnum.split(",");
                                JSONArray ballsArray = new JSONArray();
                                for (String ball : ballArray) {
                                    ballsArray.add(ball.trim());
                                }
                                drawResult.putOpt("balls", ballsArray);
                            }
                        } else {
                            // 如果期数不匹配，可能需要查询下一期的odds来获取当前期数的开奖结果
                            // 或者等待当前期数开奖后再查询
                            log.warn("翻倍计划-期数不匹配-平台用户:{},账号:{}, 期数:{}, 方案:{}, 当前gid:{}, br:{}", 
                                    username, userConfig.getAccount(), drawNumber, plan.getName(), currentGid, currentBr);
                            // 尝试从br和bnum获取（如果br是我们要找的期数）
                            if (drawNumber.equals(currentBr) && StringUtils.isNotBlank(currentBnum)) {
                                drawResult = new JSONObject();
                                drawResult.putOpt("gid", currentBr);
                                drawResult.putOpt("bnum", currentBnum);
                                drawResult.putOpt("drawNumber", drawNumber);
                                
                                // 解析开奖号码
                                String[] ballArray = currentBnum.split(",");
                                JSONArray ballsArray = new JSONArray();
                                for (String ball : ballArray) {
                                    ballsArray.add(ball.trim());
                                }
                                drawResult.putOpt("balls", ballsArray);
                            } else {
                                log.info("翻倍计划-获取开奖信息失败-平台用户:{},账号:{}, 期数:{}, 方案:{}, 期数不匹配或未开奖", 
                                        username, userConfig.getAccount(), drawNumber, plan.getName());
                                return;
                            }
                        }
                    } else {
                        log.warn("翻倍计划-odds API返回失败-平台用户:{},账号:{}, 期数:{}, 方案:{}", 
                                username, userConfig.getAccount(), drawNumber, plan.getName());
                        return;
                    }
                } catch (Exception e) {
                    log.error("翻倍计划-调用odds API异常-平台用户:{},账号:{}, 期数:{}, 方案:{}", 
                            username, userConfig.getAccount(), drawNumber, plan.getName(), e);
                    return;
                }
            }
            
            // 验证开奖信息是否完整
            JSONArray ballsArray = drawResult.getJSONArray("balls");
            if (drawResult == null || drawResult.isEmpty() || ballsArray == null || ballsArray.isEmpty()) {
                log.info("翻倍计划-获取开奖信息失败-平台用户:{},账号:{}, 期数:{}, 方案:{}, 开奖信息不完整:{}", 
                        username, userConfig.getAccount(), drawNumber, plan.getName(), drawResult);
                return;
            }
            
            log.info("翻倍计划-获取开奖信息-平台用户:{},账号:{}, 期数:{}, 方案:{}, 开奖信息{}", 
                    username, userConfig.getAccount(), drawNumber, plan.getName(), drawResult);
            
            // 每期开奖信息永久保存（移除Duration限制）
            businessUserRedissonClient.getBucket(drawKey).set(drawResult);
            
            // 解析开奖单号
            balls = ballsArray.toList(String.class);
            
            // 对于二字定位，需要检查所有可能的订单请求key（包括带定位类型后缀的）
            boolean isTwoPosition = "TPD".equals(plan.getLottery()) || "TPC".equals(plan.getLottery()) || "TPB".equals(plan.getLottery());
            List<String> orderReqKeys = new ArrayList<>();
            
            if (isTwoPosition) {
                // 二字定位：检查所有定位类型的key
                for (String locationType : new String[]{"TPD", "TPC", "TPB"}) {
                    String key = KeyUtil.genKey(
                            RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                            plan.getLottery(),
                            ToDayRangeUtil.getToDayRange(),
                            drawNumber,
                            username,
                            String.valueOf(plan.getId()),
                            monitorId,
                            userConfig.getAccount(),
                            locationType
                    );
                    orderReqKeys.add(key);
                }
            }
            
            // 也检查原始的key（兼容性）
            String baseOrderReqKey = KeyUtil.genKey(
                    RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                    plan.getLottery(),
                    ToDayRangeUtil.getToDayRange(),
                    drawNumber,
                    username,
                    String.valueOf(plan.getId()),
                    monitorId,
                    userConfig.getAccount()
            );
            orderReqKeys.add(baseOrderReqKey);
            
            // 遍历所有可能的订单请求key，收集所有投注的idIndex
            List<String> betIdIndexes = new ArrayList<>();
            Map<String, String> wtypeMap = new HashMap<>(); // 存储每个idIndex对应的wtype
            
            for (String orderReqKey : orderReqKeys) {
                JSONObject orderReq = JSONUtil.parseObj(redisson.getBucket(orderReqKey).get());
                if (orderReq.isEmpty()) {
                    continue; // 跳过不存在的订单请求
                }
                
                // 获取订单请求中的wtype（实际的定位类型）
                String orderWtype = orderReq.getStr("wtype");
                if (StringUtils.isBlank(orderWtype)) {
                    orderWtype = plan.getLottery(); // 如果没有wtype字段，使用plan.getLottery()
                }
                
                // 新API订单结构：betNewList包含BetNew对象数组
                JSONArray betNewListArray = orderReq.getJSONArray("betNewList");
                if (betNewListArray != null && !betNewListArray.isEmpty()) {
                    // 遍历 betNewList 数组，提取投注的idIndex
                    for (int i = 0; i < betNewListArray.size(); i++) {
                        JSONObject betNew = betNewListArray.getJSONObject(i);
                        String idIndex = betNew.getStr("idIndex");
                        if (StringUtils.isNotBlank(idIndex)) {
                            betIdIndexes.add(idIndex);
                            // 存储idIndex对应的wtype（用于二字定位的中号检测）
                            wtypeMap.put(idIndex, orderWtype);
                        }
                    }
                }
            }
            
            if (betIdIndexes.isEmpty()) {
                log.info("翻倍计划-校验中号结果-所有订单请求中betNewList为空-平台用户:{},账号:{}, 期数:{}, 方案:{}", 
                        username, userConfig.getAccount(), drawNumber, plan.getName());
                return;
            }
            
            // 存入 map（key为账号，value为投注的idIndex列表和对应的wtype）
            gameContentsMap.put(userConfig.getAccount(), betIdIndexes);
            // 将wtypeMap存储到gameContentsMap中（使用特殊key）
            gameContentsMap.put(userConfig.getAccount() + "_wtypeMap", new ArrayList<>(wtypeMap.values()));
        }
        
        // 计算开奖和值（加拿大28：三个球的和值）
        int drawSumValue = -1;
        if (CollUtil.isNotEmpty(balls) && balls.size() >= 3) {
            try {
                int sum = 0;
                for (String ball : balls) {
                    sum += Integer.parseInt(ball.trim());
                }
                drawSumValue = sum;
                log.info("翻倍计划-校验中号结果-平台用户:{}, 期数:{}, 方案:{}, 开奖号码:{}, 和值:{}", 
                        username, drawNumber, plan.getName(), balls, drawSumValue);
            } catch (NumberFormatException e) {
                log.error("翻倍计划-计算开奖和值失败-平台用户:{}, 期数:{}, 方案:{}, 开奖号码:{}", 
                        username, drawNumber, plan.getName(), balls, e);
                return;
            }
        } else {
            log.warn("翻倍计划-开奖号码数量不足-平台用户:{}, 期数:{}, 方案:{}, 开奖号码:{}", 
                    username, drawNumber, plan.getName(), balls);
            return;
        }
        
        log.info("翻倍计划-校验中号结果-平台用户:{}, 期数:{}, 方案:{}, 开奖和值:{}, 投注位置:{}", 
                username, drawNumber, plan.getName(), drawSumValue, gameContentsMap);

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
        
        if (drawSumValue >= 0 && CollUtil.isNotEmpty(gameContentsMap)) {
            String wtype = plan.getLottery(); // 获取游戏类型
            
            // 根据游戏类型计算中奖的idIndex
            Set<String> winningIdIndexes = new HashSet<>();
            
            // 对于二字定位，需要为每个定位类型单独检测中号
            boolean isTwoPosition = "TPD".equals(wtype) || "TPC".equals(wtype) || "TPB".equals(wtype);
            
            if ("all".equals(wtype)) {
                // 主势盘：idIndex对应和值（idIndex=1对应和值0，idIndex=2对应和值1，以此类推）
                // 开奖和值对应的idIndex = drawSumValue + 1
                winningIdIndexes.add(String.valueOf(drawSumValue + 1));
            } else if ("OPN".equals(wtype)) {
                // 单球1-3：根据开奖号码计算每个球的中奖idIndex
                if (CollUtil.isNotEmpty(balls) && balls.size() >= 3) {
                    try {
                        int ball1 = Integer.parseInt(balls.get(0).trim());
                        int ball2 = Integer.parseInt(balls.get(1).trim());
                        int ball3 = Integer.parseInt(balls.get(2).trim());
                        
                        // 计算每个球的中奖idIndex
                        // 第一球
                        calculateWinningIdIndexesForBall(1, ball1, winningIdIndexes);
                        // 第二球
                        calculateWinningIdIndexesForBall(2, ball2, winningIdIndexes);
                        // 第三球
                        calculateWinningIdIndexesForBall(3, ball3, winningIdIndexes);
                        
                        log.info("翻倍计划-单球1-3中号检测-平台用户:{}, 期数:{}, 方案:{}, 开奖号码:{}={},{},{}, 中奖idIndex:{}", 
                                username, drawNumber, plan.getName(), balls, ball1, ball2, ball3, winningIdIndexes);
                    } catch (NumberFormatException e) {
                        log.error("翻倍计划-单球1-3中号检测-解析开奖号码失败-平台用户:{}, 期数:{}, 方案:{}, 开奖号码:{}", 
                                username, drawNumber, plan.getName(), balls, e);
                        return;
                    }
                }
            } else if (isTwoPosition) {
                // 二字定位：需要为每个定位类型单独检测中号
                // 从gameContentsMap中获取所有可能的定位类型（通过检查订单请求中的wtype）
                Set<String> locationTypes = new HashSet<>();
                for (UserConfig userConfig : positiveAccounts) {
                    // 检查所有可能的订单请求key
                    for (String locationType : new String[]{"TPD", "TPC", "TPB"}) {
                        String orderReqKey = KeyUtil.genKey(
                                RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                                plan.getLottery(),
                                ToDayRangeUtil.getToDayRange(),
                                drawNumber,
                                username,
                                String.valueOf(plan.getId()),
                                monitorId,
                                userConfig.getAccount(),
                                locationType
                        );
                        JSONObject orderReq = JSONUtil.parseObj(redisson.getBucket(orderReqKey).get());
                        if (!orderReq.isEmpty()) {
                            String orderWtype = orderReq.getStr("wtype");
                            if (StringUtils.isNotBlank(orderWtype) && 
                                ("TPD".equals(orderWtype) || "TPC".equals(orderWtype) || "TPB".equals(orderWtype))) {
                                locationTypes.add(orderWtype);
                            }
                        }
                    }
                }
                
                // 如果没有找到任何定位类型，使用plan.getLottery()作为默认值
                if (locationTypes.isEmpty()) {
                    locationTypes.add(wtype);
                }
                
                // 为每个定位类型计算中奖idIndex
                if (CollUtil.isNotEmpty(balls) && balls.size() >= 3) {
                    try {
                        int ball1 = Integer.parseInt(balls.get(0).trim());
                        int ball2 = Integer.parseInt(balls.get(1).trim());
                        int ball3 = Integer.parseInt(balls.get(2).trim());
                        
                        for (String locationType : locationTypes) {
                            int firstDigit, secondDigit;
                            if ("TPD".equals(locationType)) {
                                // 佰拾定位：第一位和第二位
                                firstDigit = ball1;
                                secondDigit = ball2;
                            } else if ("TPC".equals(locationType)) {
                                // 佰个定位：第一位和第三位
                                firstDigit = ball1;
                                secondDigit = ball3;
                            } else if ("TPB".equals(locationType)) {
                                // 拾个定位：第二位和第三位
                                firstDigit = ball2;
                                secondDigit = ball3;
                            } else {
                                continue; // 跳过未知的定位类型
                            }
                            
                            // 计算中奖idIndex：idIndex = 第一位 * 10 + 第二位 + 1
                            int winningIdIndex = firstDigit * 10 + secondDigit + 1;
                            if (winningIdIndex >= 1 && winningIdIndex <= 100) {
                                winningIdIndexes.add(String.valueOf(winningIdIndex));
                                log.info("翻倍计划-二字定位中号检测-平台用户:{}, 期数:{}, 方案:{}, 定位类型:{}, 开奖号码:{}={},{},{}, 第一位:{}, 第二位:{}, 中奖idIndex:{}", 
                                        username, drawNumber, plan.getName(), locationType, balls, ball1, ball2, ball3, firstDigit, secondDigit, winningIdIndex);
                            } else {
                                log.warn("翻倍计划-二字定位中号检测-计算出的idIndex超出范围:{}", winningIdIndex);
                            }
                        }
                    } catch (NumberFormatException e) {
                        log.error("翻倍计划-二字定位中号检测-解析开奖号码失败-平台用户:{}, 期数:{}, 方案:{}, 开奖号码:{}", 
                                username, drawNumber, plan.getName(), balls, e);
                        return;
                    }
                } else {
                    log.warn("翻倍计划-二字定位中号检测-开奖号码数量不足:{}", balls);
                }
            } else {
                // 其他游戏类型：暂时使用主势盘的逻辑（后续实现）
                winningIdIndexes.add(String.valueOf(drawSumValue + 1));
                log.warn("翻倍计划-中号检测-游戏类型{}暂未实现完整逻辑，使用主势盘逻辑", wtype);
            }
            
            // 遍历所有账号的投注位置
            gameContentsMap.forEach((account, idIndexes) -> {
                // 检查该账号的投注idIndex列表中是否包含中奖的idIndex
                if (idIndexes != null) {
                    for (String idIndex : idIndexes) {
                        if (winningIdIndexes.contains(idIndex)) {
                            // 当前投注中了
                            winNum.getAndIncrement();
                            log.info("翻倍计划-账号中号-平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 投注idIndex:{}, 中奖idIndex:{}", 
                                    username, account, drawNumber, plan.getName(), idIndex, winningIdIndexes);
                            break; // 每个账号只计算一次中号
                        }
                    }
                }
            });
            log.info("翻倍计划-校验中号结果-平台用户:{}, 期数:{}, 方案:{}, 游戏类型:{}, 开奖和值:{}, 中奖idIndex:{}, 实际中号数:{}", 
                    username, drawNumber, plan.getName(), wtype, drawSumValue, winningIdIndexes, winNum.get());

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
            RList<JSONObject> monitorInfos = businessUserRedissonClient.getList(monitorKey);
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
     * @param closeTime
     * @param username
     * @param lottery
     * @param drawNumber
     * @return
     */
    private boolean validateCloseTime(long closeTime, String username, String lottery, String drawNumber) {
        if (closeTime < 15) {
            log.info("翻倍计划-平台用户:{}, 游戏:{}, 期数:{}, 剩余封盘时间:{}, 即将封盘, 不进行下注", username, lottery, drawNumber, closeTime);
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
    private RLock acquireBetLock(long closeTime, String username, PlanDoubleVO plan, String drawNumber) {
        RLock lock = businessUserRedissonClient.getLock(KeyUtil.genKey(RedisConstants.USER_BET_DOUBLE_PERIOD_PREFIX, plan.getLottery(), drawNumber, username, String.valueOf(plan.getId())));
        try {
            // 5秒内尝试获取锁，持锁时间到封盘时间+10秒
            if (lock.tryLock(5, closeTime + 10, TimeUnit.SECONDS)) {
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
     * @param plan
     * @return
     */
    private JSONObject twoSidedAmountJson(JSONObject oddsJson, String wtype, Map<String, List<Integer>> twoSidedDxPositions, Map<String, List<Integer>> twoSidedDsPositions, Map<String, List<Integer>> twoSidedLhPositions, List<UserConfig> allAccounts, PlanDoubleVO plan, Integer rounds) {
        JSONObject twoSidedAmountJson = null;
        if (CollUtil.isNotEmpty(twoSidedDxPositions) || CollUtil.isNotEmpty(twoSidedDsPositions) || CollUtil.isNotEmpty(twoSidedLhPositions)) {
            // 获取双面-大小投注的位置 key
            Map<String, List<String>> twoSidedDxOdds = new HashMap<>();
            // 获取双面-单双投注的位置 key
            Map<String, List<String>> twoSidedDsOdds = new HashMap<>();
            // 获取双面-龙虎投注的位置 key
            Map<String, List<String>> twoSidedLhOdds = new HashMap<>();

            // 新API：不再从oddsJson.keySet()匹配betKey，直接根据投注类型和游戏类型计算idIndex
            // 合并所有双面投注信息（不再区分正投/反投）
            Map<String, List<BettingPositionInfo>> twoSidedMap = new HashMap<>();
            
            // 处理大小投注
            if (null != twoSidedDxPositions) {
                twoSidedDxPositions.forEach((betType, positions) -> {
                    for (int pos : positions) {
                        BettingPositionInfo positionInfo = new BettingPositionInfo();
                        positionInfo.setPositionId(pos);
                        positionInfo.setBetType(betType); // "大" 或 "小"
                        // 根据游戏类型计算idIndex和rate（待实现具体逻辑）
                        positionInfo.setIdIndex(null); // 待计算
                        positionInfo.setRate(null); // 待计算
                        
                        twoSidedMap.computeIfAbsent(betType, k -> new ArrayList<>()).add(positionInfo);
                    }
                });
            }
            
            // 处理单双投注
            if (null != twoSidedDsPositions) {
                twoSidedDsPositions.forEach((betType, positions) -> {
                    for (int pos : positions) {
                        BettingPositionInfo positionInfo = new BettingPositionInfo();
                        positionInfo.setPositionId(pos);
                        positionInfo.setBetType(betType); // "单" 或 "双"
                        // 根据游戏类型计算idIndex和rate（待实现具体逻辑）
                        positionInfo.setIdIndex(null); // 待计算
                        positionInfo.setRate(null); // 待计算
                        
                        twoSidedMap.computeIfAbsent(betType, k -> new ArrayList<>()).add(positionInfo);
                    }
                });
            }
            
            // 处理龙虎投注
            if (null != twoSidedLhPositions) {
                twoSidedLhPositions.forEach((betType, positions) -> {
                    for (int pos : positions) {
                        BettingPositionInfo positionInfo = new BettingPositionInfo();
                        positionInfo.setPositionId(pos);
                        positionInfo.setBetType(betType); // "龙" 或 "虎"
                        // 根据游戏类型计算idIndex和rate（待实现具体逻辑）
                        positionInfo.setIdIndex(null); // 待计算
                        positionInfo.setRate(null); // 待计算
                        
                        twoSidedMap.computeIfAbsent(betType, k -> new ArrayList<>()).add(positionInfo);
                    }
                });
            }
            
            // 获取双面金额分配（统一使用所有账号）
            twoSidedAmountJson = twoSidedDistributeAmount(twoSidedMap, oddsJson, wtype, allAccounts, plan, rounds);
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
     * 创建BetNew列表（新API格式）
     * @param username 用户名
     * @param monitorId 监控ID
     * @param plan 翻倍计划
     * @param oddsJson 赔率JSON
     * @param drawNumber 期数
     * @param amountJson 金额JSON（不再区分positive/reverse）
     * @param twoSidedAmountJson 双面金额JSON（不再区分positive/reverse）
     * @param userConfig 用户配置
     * @param amountKeys 金额键列表
     * @return BetNew列表
     */
    /**
     * 创建BetNew列表（新API：直接根据投注位置信息创建，不再使用betKey格式）
     * @param username 用户名
     * @param monitorId 监控ID
     * @param plan 计划
     * @param oddsJson 赔率JSON
     * @param drawNumber 期数
     * @param amountJson 金额JSON（新结构：{"位置ID": {"idIndex": {"account": amount}}}）
     * @param twoSidedAmountJson 双面金额JSON（新结构：{"投注类型": {"idIndex": {"account": amount}}}）
     * @param userConfig 用户配置
     * @param wtype 游戏类型
     * @return BetNew列表
     */
    private List<BetNew> createBetNewList(String username, String monitorId, PlanDoubleVO plan,
                                           JSONObject oddsJson, String drawNumber,
                                           JSONObject amountJson, JSONObject twoSidedAmountJson,
                                           UserConfig userConfig, String wtype) {
        List<BetNew> betNewList = new ArrayList<>();

        log.info("翻倍计划-创建BetNew列表, 账号:{}, 期数:{}, 方案:{}, 游戏类型:{}, amountJson:{}, twoSidedAmountJson:{}",
                userConfig.getAccount(), drawNumber, plan.getName(), wtype, amountJson, twoSidedAmountJson);

        // 处理普通投注（amountJson）
        // 新结构: {"位置ID": {"idIndex": {"account": amount}}}
        if (amountJson != null && !amountJson.isEmpty()) {
            amountJson.forEach((positionId, positionValueObj) -> {
                JSONObject positionValue = JSONUtil.parseObj(positionValueObj);
                positionValue.forEach((idIndex, amountValueObj) -> {
                    JSONObject amountValue = JSONUtil.parseObj(amountValueObj);
                    Long amount = amountValue.getLong(userConfig.getAccount());
                    if (amount == null || amount <= 0) {
                        return; // 跳过金额为0或null的投注
                    }

                    // 获取赔率（从oddsJson的rate数组获取）
                    String rate = null;
                    if (oddsJson.containsKey("rate")) {
                        JSONArray rateArray = oddsJson.getJSONArray("rate");
                        if (rateArray != null) {
                            try {
                                int index = Integer.parseInt(idIndex);
                                if (index >= 0 && index < rateArray.size()) {
                                    rate = rateArray.getStr(index);
                                }
                            } catch (NumberFormatException e) {
                                log.warn("翻倍计划-idIndex不是数字:{}", idIndex);
                            }
                        }
                    }

                    // 创建BetNew对象
                    BetNew betNew = new BetNew();
                    betNew.setIdIndex(idIndex);
                    betNew.setAmount(String.valueOf(amount));
                    betNew.setRate(rate != null ? rate : "0"); // 如果获取不到赔率，使用默认值0
                    
                    betNewList.add(betNew);
                });
            });
        }

        // 处理双面投注（twoSidedAmountJson）
        // 新结构: {"投注类型": {"idIndex": {"account": amount}}}
        if (twoSidedAmountJson != null && !twoSidedAmountJson.isEmpty()) {
            twoSidedAmountJson.forEach((betType, betTypeValueObj) -> {
                JSONObject betTypeValue = JSONUtil.parseObj(betTypeValueObj);
                betTypeValue.forEach((idIndex, amountValueObj) -> {
                    JSONObject amountValue = JSONUtil.parseObj(amountValueObj);
                    Long amount = amountValue.getLong(userConfig.getAccount());
                    if (amount == null || amount <= 0) {
                        return;
                    }

                    // 获取赔率（从oddsJson的rate数组获取）
                    String rate = null;
                    if (oddsJson.containsKey("rate")) {
                        JSONArray rateArray = oddsJson.getJSONArray("rate");
                        if (rateArray != null) {
                            try {
                                int index = Integer.parseInt(idIndex);
                                if (index >= 0 && index < rateArray.size()) {
                                    rate = rateArray.getStr(index);
                                }
                            } catch (NumberFormatException e) {
                                log.warn("翻倍计划-idIndex不是数字:{}", idIndex);
                            }
                        }
                    }

                    // 创建BetNew对象
                    BetNew betNew = new BetNew();
                    betNew.setIdIndex(idIndex);
                    betNew.setAmount(String.valueOf(amount));
                    betNew.setRate(rate != null ? rate : "0"); // 如果获取不到赔率，使用默认值0
                    
                    betNewList.add(betNew);
                });
            });
        }

        log.info("翻倍计划-创建BetNew列表完成, 账号:{}, 期数:{}, 方案:{}, BetNew数量:{}",
                userConfig.getAccount(), drawNumber, plan.getName(), betNewList.size());

        return betNewList;
    }

    /**
     * 根据游戏类型和投注位置信息计算idIndex和rate（新API使用）
     * @param positionInfo 投注位置信息
     * @param oddsJson 赔率JSON（包含rate数组）
     * @param wtype 游戏类型（all/OPN/TPD/TPC/TPB/DW3/n5）
     * @return BetNew对象，如果计算失败返回null
     */
    private BetNew calculateBetNewFromPosition(BettingPositionInfo positionInfo, JSONObject oddsJson, String wtype) {
        if (positionInfo == null || oddsJson == null || StringUtils.isBlank(wtype)) {
            return null;
        }
        
        BetNew betNew = new BetNew();
        
        try {
            if ("all".equals(wtype)) {
                // 主势盘：idIndex = 和值 + 1
                if (positionInfo.getIdIndex() != null) {
                    betNew.setIdIndex(positionInfo.getIdIndex());
                    betNew.setRate(positionInfo.getRate());
                    betNew.setAmount(String.valueOf(positionInfo.getPositionId())); // 临时使用positionId作为amount，后续会更新
                    return betNew;
                }
            } else if ("OPN".equals(wtype)) {
                // 单球1-3：根据球号和投注类型计算idIndex
                // TODO: 实现单球1-3的计算逻辑
                log.warn("翻倍计划-单球1-3计算逻辑待实现, positionInfo:{}", positionInfo);
                return null;
            } else if ("TPD".equals(wtype) || "TPC".equals(wtype) || "TPB".equals(wtype)) {
                // 二字定位：idIndex = 第一位 * 10 + 第二位 + 1
                // TODO: 实现二字定位的计算逻辑
                log.warn("翻倍计划-二字定位计算逻辑待实现, positionInfo:{}, wtype:{}", positionInfo, wtype);
                return null;
            } else if ("DW3".equals(wtype)) {
                // 三字定位：idIndex = 三位数字符串
                // TODO: 实现三字定位的计算逻辑
                log.warn("翻倍计划-三字定位计算逻辑待实现, positionInfo:{}", positionInfo);
                return null;
            } else if ("n5".equals(wtype)) {
                // 两面盘：根据投注类型映射idIndex
                // TODO: 实现两面盘的计算逻辑
                log.warn("翻倍计划-两面盘计算逻辑待实现, positionInfo:{}", positionInfo);
                return null;
            } else {
                log.warn("翻倍计划-未知游戏类型:{}", wtype);
                return null;
            }
        } catch (Exception e) {
            log.error("翻倍计划-计算BetNew异常, positionInfo:{}, wtype:{}, 错误:{}", positionInfo, wtype, e.getMessage(), e);
            return null;
        }
        
        return null;
    }

    /**
     * 将旧格式的投注键转换为BetNew（保留用于兼容，逐步废弃）
     * @param betKey 投注键（如 "B1_01", "DX1_大", "DS1_单", "LH1_龙" 等）
     * @param amount 投注金额
     * @param oddsJson 赔率JSON
     * @param wtype 游戏类型（all/OPN/TPD/TPC/TPB/DW3/n5）
     * @return BetNew对象
     * @deprecated 新API不再使用betKey格式，应使用calculateBetNewFromPosition
     */
    @Deprecated
    private BetNew convertToBetNew(String betKey, Long amount, JSONObject oddsJson, String wtype) {
        BetNew betNew = new BetNew();

        try {
            if ("all".equals(wtype)) {
                // 主势盘：和值投注
                // betKey格式可能是 "B1_01"，需要转换为和值
                // 解析betKey，提取位置和号码
                String[] parts = betKey.split("_");
                if (parts.length != 2) {
                    log.warn("翻倍计划-转换BetNew失败, betKey格式错误:{}", betKey);
                    return null;
                }

                String positionStr = parts[0].replace("B", ""); // 位置（1-10）
                String numberStr = parts[1]; // 号码（01-10）

                int position = Integer.parseInt(positionStr);
                int number = Integer.parseInt(numberStr);

                // 主势盘：投注号码就是和值
                // 例如：投注号码1，对应和值1；投注号码2，对应和值2
                int sumValue = number;

                // idIndex = 和值 + 1
                String idIndex = String.valueOf(sumValue + 1);

                // 获取赔率：rate[和值 + 1]
                if (!oddsJson.containsKey("rate")) {
                    log.warn("翻倍计划-赔率JSON中没有rate数组, betKey:{}", betKey);
                    return null;
                }
                JSONArray rateArray = oddsJson.getJSONArray("rate");
                if (rateArray == null || rateArray.size() < sumValue + 1) {
                    log.warn("翻倍计划-赔率数组长度不足, betKey:{}, sumValue:{}", betKey, sumValue);
                    return null;
                }
                String rate = rateArray.getStr(sumValue + 1);

                betNew.setIdIndex(idIndex);
                betNew.setAmount(String.valueOf(amount));
                betNew.setRate(rate);

            } else if ("OPN".equals(wtype)) {
                // 单球1-3：需要根据球号、投注类型计算idIndex
                // TODO: 实现单球1-3的转换逻辑
                // betKey格式可能是 "B1_01"（第一球号码1）或 "DX1_大"（第一球大）
                log.warn("翻倍计划-单球1-3转换逻辑待实现, betKey:{}", betKey);
                return null;

            } else if ("TPD".equals(wtype) || "TPC".equals(wtype) || "TPB".equals(wtype)) {
                // 二字定位：idIndex = 第一位 * 10 + 第二位 + 1
                // TODO: 实现二字定位的转换逻辑
                log.warn("翻倍计划-二字定位转换逻辑待实现, betKey:{}, wtype:{}", betKey, wtype);
                return null;

            } else if ("DW3".equals(wtype)) {
                // 三字定位：idIndex直接是三位数字符串
                // TODO: 实现三字定位的转换逻辑
                log.warn("翻倍计划-三字定位转换逻辑待实现, betKey:{}", betKey);
                return null;

            } else if ("n5".equals(wtype)) {
                // 两面盘：需要根据投注类型计算idIndex
                // TODO: 实现两面盘的转换逻辑
                log.warn("翻倍计划-两面盘转换逻辑待实现, betKey:{}", betKey);
                return null;

            } else {
                log.warn("翻倍计划-未知游戏类型:{}", wtype);
                return null;
            }

        } catch (Exception e) {
            log.error("翻倍计划-转换BetNew异常, betKey:{}, wtype:{}, 错误:{}", betKey, wtype, e.getMessage(), e);
            return null;
        }

        return betNew;
    }

    /**
     * 创建失败结果JSON
     * @param lottery 游戏类型
     * @param drawNumber 期数
     * @param account 账号
     * @param message 错误消息
     * @return JSON字符串
     */
    private String createFailedResult(String lottery, String drawNumber, String account, String message) {
        JSONObject failed = new JSONObject();
        failed.putOpt("status", 1);
        failed.putOpt("lottery", lottery);
        failed.putOpt("drawNumber", drawNumber);
        failed.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
        failed.putOpt("account", account);
        failed.putOpt("isRepair", 0);
        failed.putOpt("message", message);
        return JSONUtil.toJsonStr(failed);
    }

    /**
     * 封装发起提交下注
     * @param betNewList BetNew列表
     * @param userConfig 用户配置
     * @param plan 翻倍计划
     * @param drawNumber 期数
     * @return
     */
    /**
     * 为指定wtype提交投注（用于二字定位等需要指定定位类型的场景）
     * @param username 用户名
     * @param userConfig 用户配置
     * @param plan 翻倍计划
     * @param betNewList BetNew列表
     * @param drawNumber 期数
     * @param wtype 游戏类型（定位类型：TPD/TPC/TPB）
     * @param closeTime 封盘时间
     * @return 投注结果JSON字符串
     */
    private String submitOrderForWtype(String username, UserConfig userConfig, PlanDoubleVO plan,
                                       List<BetNew> betNewList, String drawNumber, String wtype, long closeTime) {
        log.info("翻倍计划-发起下单(指定wtype), 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 定位类型:{}, 账号token:{}, BetNew数量:{}",
                username, userConfig.getAccount(), drawNumber, plan.getName(), wtype, userConfig.getToken(),
                betNewList != null ? betNewList.size() : 0);

        if (betNewList == null || betNewList.isEmpty()) {
            log.info("翻倍计划-下单失败, BetNew列表为空, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 定位类型:{}",
                    username, userConfig.getAccount(), drawNumber, plan.getName(), wtype);
            return createFailedResult(plan.getLottery(), drawNumber, userConfig.getAccount(),
                    "投注列表为空");
        }

        TimeInterval timeBet = DateUtil.timer();
        String url = userConfig.getBaseUrl() + "app/control/bet.php";

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "application/json, text/javascript, */*; q=0.01");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("cache-control", "no-cache");
        headers.put("content-type", "application/x-www-form-urlencoded; charset=UTF-8");
        headers.put("origin", userConfig.getBaseUrl());
        headers.put("pragma", "no-cache");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36");
        headers.put("x-requested-with", "XMLHttpRequest");

        // 构建URL参数
        String params;
        try {
            params = "uid=" + userConfig.getToken()
                    + "&gid=" + drawNumber
                    + "&gtype=9"
                    + "&wtype=" + wtype  // 使用实际的定位类型
                    + "&ltype=1"
                    + "&ratechg=1"
                    + "&orders=" + URLEncoder.encode(JSONUtil.toJsonStr(betNewList), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("翻倍计划-构建URL参数失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 定位类型:{}",
                     username, userConfig.getAccount(), drawNumber, plan.getName(), wtype, e);
            return createFailedResult(plan.getLottery(), drawNumber, userConfig.getAccount(),
                    "构建URL参数失败: " + e.getMessage());
        }

        String result = null;
        boolean success = false;
        try {
            CompletableFuture<OkHttpProxyDispatcher.HttpResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.info("翻倍计划-进行下单-发起下单请求 平台用户:{},游戏:{},期数:{},封盘时间:{},方案:{},定位类型:{},参数:{}",
                            username, plan.getLottery(), drawNumber, LocalDateTimeUtil.of(closeTime), plan.getName(), wtype, params);
                    return dispatcher.execute("POST", url, params, headers, userConfig, false);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });

            OkHttpProxyDispatcher.HttpResult resultHttp = future.get(10, TimeUnit.SECONDS);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
            log.info("翻倍计划-下单结果, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 定位类型:{}, 返回信息:{}",
                    username, userConfig.getAccount(), drawNumber, plan.getName(), wtype, result);

            if (StringUtils.isBlank(result)) {
                log.info("翻倍计划-下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 定位类型:{}, 返回信息为空",
                        username, userConfig.getAccount(), drawNumber, plan.getName(), wtype);
                return createFailedResult(plan.getLottery(), drawNumber, userConfig.getAccount(),
                        "账号异常,请检查登录状态以及账号是否正常");
            }

            if (!JSONUtil.isTypeJSONObject(result)) {
                log.info("翻倍计划-下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 定位类型:{}, 返回信息格式错误:{}",
                        username, userConfig.getAccount(), drawNumber, plan.getName(), wtype, result);
                return createFailedResult(plan.getLottery(), drawNumber, userConfig.getAccount(), "代理异常");
            }

            JSONObject resultJson = JSONUtil.parseObj(result);
            int title = resultJson.getInt("title", -1);
            if (title != 1) {
                log.info("翻倍计划-下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 定位类型:{}, title:{}",
                        username, userConfig.getAccount(), drawNumber, plan.getName(), wtype, title);
                return createFailedResult(plan.getLottery(), drawNumber, userConfig.getAccount(),
                        "投注失败, title=" + title);
            }

            log.info("翻倍计划-下单成功, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 定位类型:{}, 下单花费:{}毫秒",
                    username, userConfig.getAccount(), drawNumber, plan.getName(), wtype, timeBet.interval());
            success = true;
            JSONObject resultObj = new JSONObject();
            resultObj.putOpt("status", 0);
            resultObj.putOpt("lottery", plan.getLottery());
            resultObj.putOpt("drawNumber", drawNumber);
            resultObj.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            resultObj.putOpt("account", userConfig.getAccount());
            resultObj.putOpt("isRepair", 0);
            resultObj.putOpt("message", "盘口投注成功");
            return JSONUtil.toJsonStr(resultObj);
        } catch (Exception e) {
            log.error("翻倍计划-下单异常, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 定位类型:{}",
                     username, userConfig.getAccount(), drawNumber, plan.getName(), wtype, e);
            return createFailedResult(plan.getLottery(), drawNumber, userConfig.getAccount(),
                    "投注异常: " + e.getMessage());
        } finally {
            log.info("翻倍计划-下单结束, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 定位类型:{}, 返回信息{}, 网络请求成功:{}, 下单花费:{}毫秒",
                    username, userConfig.getAccount(), drawNumber, plan.getName(), wtype, result, success, timeBet.intervalMs());
        }
    }

    private String submitOrder(String username, List<BetNew> betNewList, UserConfig userConfig, PlanDoubleVO plan, String drawNumber, String monitorId, long closeTime) {
        log.info("翻倍计划-发起下单, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 账号token:{}, BetNew数量:{}",
                username, userConfig.getAccount(), drawNumber, plan.getName(), userConfig.getToken(),
                betNewList != null ? betNewList.size() : 0);

        if (betNewList == null || betNewList.isEmpty()) {
            log.info("翻倍计划-下单失败, BetNew列表为空, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}",
                    username, userConfig.getAccount(), drawNumber, plan.getName());
            return createFailedResult(plan.getLottery(), drawNumber, userConfig.getAccount(),
                    "投注列表为空");
        }

        TimeInterval timeBet = DateUtil.timer();
        String url = userConfig.getBaseUrl() + "app/control/bet.php";
        String wtype = plan.getLottery(); // 获取游戏类型

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "application/json, text/javascript, */*; q=0.01");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("cache-control", "no-cache");
        headers.put("content-type", "application/x-www-form-urlencoded; charset=UTF-8");
        headers.put("origin", userConfig.getBaseUrl());
        headers.put("pragma", "no-cache");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
        headers.put("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36");
        headers.put("x-requested-with", "XMLHttpRequest");

        // 构建URL参数
        String params;
        try {
            params = "uid=" + userConfig.getToken()
                    + "&gid=" + drawNumber
                    + "&gtype=9"
                    + "&wtype=" + wtype
                    + "&ltype=1"
                    + "&ratechg=1"
                    + "&orders=" + URLEncoder.encode(JSONUtil.toJsonStr(betNewList), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("翻倍计划-构建URL参数失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}", 
                     username, userConfig.getAccount(), drawNumber, plan.getName(), e);
            return createFailedResult(plan.getLottery(), drawNumber, userConfig.getAccount(),
                    "构建URL参数失败: " + e.getMessage());
        }

        // 记录订单请求（保存为JSON格式便于查看）
        JSONObject orderRecord = new JSONObject();
        orderRecord.put("lottery", plan.getLottery());
        orderRecord.put("drawNumber", drawNumber);
        orderRecord.put("wtype", wtype);
        orderRecord.put("betNewList", betNewList);

        redisson.getBucket(KeyUtil.genKey(
                RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                plan.getLottery(),
                ToDayRangeUtil.getToDayRange(),
                drawNumber,
                username,
                String.valueOf(plan.getId()),
                monitorId,
                userConfig.getAccount()
        )).set(JSONUtil.toJsonStr(orderRecord), Duration.ofHours(24));
        
        String result = null;
        boolean success = false;
        try {
            // ✅ 使用 OkHttpProxyDispatcher 调用，并封装异步 + 超时
            CompletableFuture<OkHttpProxyDispatcher.HttpResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.info("翻倍计划-进行下单-发起下单请求 平台用户:{},游戏:{},期数:{},封盘时间:{},方案:{},参数:{}",
                            username, plan.getLottery(), drawNumber, LocalDateTimeUtil.of(closeTime), plan.getName(), params);
                    return dispatcher.execute("POST", url, params, headers, userConfig, false);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });
            
            OkHttpProxyDispatcher.HttpResult resultHttp = future.get(10, TimeUnit.SECONDS);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
            log.info("翻倍计划-下单结果, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息:{}",
                    username, userConfig.getAccount(), drawNumber, plan.getName(), result);

            if (StringUtils.isBlank(result)) {
                log.info("翻倍计划-下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息为空",
                        username, userConfig.getAccount(), drawNumber, plan.getName());
                return createFailedResult(plan.getLottery(), drawNumber, userConfig.getAccount(),
                        "账号异常,请检查登录状态以及账号是否正常");
            }
            
            if (!JSONUtil.isTypeJSONObject(result)) {
                log.info("翻倍计划-下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息格式错误:{}",
                        username, userConfig.getAccount(), drawNumber, plan.getName(), result);
                return createFailedResult(plan.getLottery(), drawNumber, userConfig.getAccount(), "代理异常");
            }
            
            JSONObject resultJson = JSONUtil.parseObj(result);
            int title = resultJson.getInt("title", -1);
            if (title != 1) {
                log.info("翻倍计划-下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, title:{}",
                        username, userConfig.getAccount(), drawNumber, plan.getName(), title);
                return createFailedResult(plan.getLottery(), drawNumber, userConfig.getAccount(),
                        "投注失败, title=" + title);
            }
            
            log.info("翻倍计划-下单成功, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 下单花费:{}毫秒",
                    username, userConfig.getAccount(), drawNumber, plan.getName(), timeBet.interval());
            success = true;
        } catch (Exception e) {
            // ---------------------- 进入注单校验逻辑 ----------------------
            log.info("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},封盘时间:{},进入确认是否需要补单逻辑：下注请求超时，启动最大 15 秒注单匹配流程",
                    username, userConfig.getAccount(), drawNumber, plan.getName(), LocalDateTimeUtil.of(closeTime));

            long betTime = closeTime - System.currentTimeMillis();
            boolean successBet = false;                     // 标记是否确认投注成功
            long fixStart = System.currentTimeMillis();     // 补单判断阶段起始时间戳
            int maxTotalMillis = (int) (betTime - 5 * 1000);       // 补单判断总时长限制在关闭投注的5秒前
            long maxDurationMs = betTime - 5 * 1000;        // 最大注单匹配时间

            // 新API使用BetNew，注单校验逻辑需要根据新API的返回结果调整
            // TODO: 实现新API的注单校验逻辑
            if (CollUtil.isNotEmpty(betNewList)) {
                boolean[] found = new boolean[betNewList.size()];  // 标记各注单是否匹配成功
                int foundCnt = 0;                            // 已匹配成功注单计数
                int maxRetries = 5;                          // 最大轮询次数
                int retryInterval = 2_000;                   // 每轮查询间隔时间 2秒
                int pageSize = 15;                           // 接口分页大小

                // 多轮查询已下注注单，确认是否已成功
                for (int attempt = 1; attempt <= maxRetries && foundCnt < betNewList.size(); attempt++) {
                    long elapsed = System.currentTimeMillis() - fixStart;

                    // 超过最大判断时间，提前终止补单判断
                    if (elapsed >= maxTotalMillis) {
                        log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},补单确认阶段超过20秒，提前结束查询，已匹配 {}/{} 注单",
                                username, userConfig.getAccount(), drawNumber, plan.getName(), foundCnt, betNewList.size());
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
                        while (curPage <= totalPages && foundCnt < betNewList.size()) {
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

                                    // TODO: 实现新API的注单匹配逻辑
                                    // 新API的注单格式与旧API不同，需要根据实际返回结果调整匹配逻辑
                                    for (int i = 0; i < betNewList.size(); i++) {
                                        if (!found[i]) {
                                            BetNew betNew = betNewList.get(i);
                                            // 根据新API的注单格式进行匹配
                                            // 需要查看新API返回的注单数据结构来确定匹配逻辑
                                            // 暂时简化处理：如果期数匹配且金额匹配，认为匹配成功
                                            if (draw.contains(drawNumber) && amount == Long.parseLong(betNew.getAmount())) {
                                                found[i] = true;
                                                ++foundCnt;
                                                log.info("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},匹配注单成功({}/{}): {}",
                                                        username, userConfig.getAccount(), drawNumber, plan.getName(), foundCnt, betNewList.size(), betNew);
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
                            username, userConfig.getAccount(), drawNumber, plan.getName(), attempt, maxRetries, foundCnt, betNewList.size());

                    if (foundCnt >= betNewList.size()) {
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

                successBet = (foundCnt >= betNewList.size());
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

    private String mergeSubmitOrder(String username, UserConfig userConfig, List<PlanDoubleVO> plans, List<BetNew> mergedBetNewList, String lottery, String drawNumber, Map<String, Map<Integer, String>> useMonitorIds, long closeTime) {
        log.info("翻倍计划-发起合并下单, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}, BetNew数量:{}",
                username, userConfig.getAccount(), drawNumber, plans.size(),
                mergedBetNewList != null ? mergedBetNewList.size() : 0);

        if (mergedBetNewList == null || mergedBetNewList.isEmpty()) {
            log.info("翻倍计划-合并下单, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}, BetNew列表为空",
                    username, userConfig.getAccount(), drawNumber, plans.size());
            return createFailedResult(lottery, drawNumber, userConfig.getAccount(), "投注列表为空");
        }

        TimeInterval timeBet = DateUtil.timer();
        String url = userConfig.getBaseUrl() + "app/control/bet.php";
        // 获取第一个计划的wtype（所有计划应该使用相同的游戏类型）
        String wtype = plans.get(0).getLottery();

        Map<String, String> headers = new HashMap<>();
        headers.put("accept", "application/json, text/javascript, */*; q=0.01");
        headers.put("accept-language", "zh-CN,zh;q=0.9");
        headers.put("cache-control", "no-cache");
        headers.put("content-type", "application/x-www-form-urlencoded; charset=UTF-8");
        headers.put("origin", userConfig.getBaseUrl());
        headers.put("pragma", "no-cache");
        headers.put("sec-ch-ua-mobile", "?0");
        headers.put("sec-ch-ua-platform", "\"Windows\"");
        headers.put("sec-fetch-dest", "empty");
        headers.put("sec-fetch-mode", "cors");
        headers.put("sec-fetch-site", "same-origin");
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

        // 记录订单请求
        JSONObject orderRecord = new JSONObject();
        orderRecord.putOpt("lottery", lottery);
        orderRecord.putOpt("drawNumber", drawNumber);
        orderRecord.putOpt("wtype", wtype);
        orderRecord.putOpt("betNewList", mergedBetNewList);
        
        redisson.getBucket(KeyUtil.genKey(
                RedisConstants.USER_BET_DOUBLE_PERIOD_REQ_PREFIX,
                lottery,
                ToDayRangeUtil.getToDayRange(),
                drawNumber,
                username,
                "merge",
                userConfig.getAccount()
        )).set(JSONUtil.toJsonStr(orderRecord), Duration.ofHours(24));
        
        // 构建URL参数
        String params;
        try {
            params = "uid=" + userConfig.getToken()
                    + "&gid=" + drawNumber
                    + "&gtype=9"
                    + "&wtype=" + wtype
                    + "&ltype=1"
                    + "&ratechg=1"
                    + "&orders=" + URLEncoder.encode(JSONUtil.toJsonStr(mergedBetNewList), StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.error("翻倍计划-构建URL参数失败, 平台用户:{}, 账号:{}, 期数:{}, 方案数:{}", 
                     username, userConfig.getAccount(), drawNumber, plans.size(), e);
            return createFailedResult(lottery, drawNumber, userConfig.getAccount(),
                    "构建URL参数失败: " + e.getMessage());
        }
        
        String result = null;
        boolean success = false;
        try {
            CompletableFuture<OkHttpProxyDispatcher.HttpResult> future = CompletableFuture.supplyAsync(() -> {
                try {
                    log.info("翻倍计划-进行合并下单-发起下单请求 平台用户:{},游戏:{},期数:{},封盘时间:{},方案:{},参数:{}",
                            username, lottery, drawNumber, LocalDateTimeUtil.of(closeTime), "合并型方案", params);
                    return dispatcher.execute("POST", url, params, headers, userConfig, false);
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });
            
            OkHttpProxyDispatcher.HttpResult resultHttp = future.get(10, TimeUnit.SECONDS);
            result = StringUtils.isNotBlank(resultHttp.getBody()) ? resultHttp.getBody() : null;
            log.info("翻倍计划-合并下单结果, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息:{}",
                    username, userConfig.getAccount(), drawNumber, "合并型方案", result);

            if (StringUtils.isBlank(result)) {
                log.info("翻倍计划-合并下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息为空",
                        username, userConfig.getAccount(), drawNumber, "合并型方案");
                return createFailedResult(lottery, drawNumber, userConfig.getAccount(),
                        "账号异常,请检查登录状态以及账号是否正常");
            }
            
            if (!JSONUtil.isTypeJSONObject(result)) {
                log.info("翻倍计划-合并下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 返回信息格式错误:{}",
                        username, userConfig.getAccount(), drawNumber, "合并型方案", result);
                return createFailedResult(lottery, drawNumber, userConfig.getAccount(), "代理异常");
            }
            
            JSONObject resultJson = JSONUtil.parseObj(result);
            int title = resultJson.getInt("title", -1);
            if (title != 1) {
                log.info("翻倍计划-合并下单失败, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, title:{}",
                        username, userConfig.getAccount(), drawNumber, "合并型方案", title);
                return createFailedResult(lottery, drawNumber, userConfig.getAccount(),
                        "投注失败, title=" + title);
            }
            
            log.info("翻倍计划-合并下单成功, 平台用户:{}, 账号:{}, 期数:{}, 方案:{}, 下单花费:{}毫秒",
                    username, userConfig.getAccount(), drawNumber, "合并型方案", timeBet.interval());
            success = true;
            JSONObject resultObj = new JSONObject();
            resultObj.putOpt("status", 0);
            resultObj.putOpt("lottery", lottery);
            resultObj.putOpt("drawNumber", drawNumber);
            resultObj.putOpt("createTime", LocalDateTimeUtil.format(LocalDateTime.now(), DatePattern.NORM_DATETIME_PATTERN));
            resultObj.putOpt("account", userConfig.getAccount());
            resultObj.putOpt("isRepair", 0);// 是否为补单 0:否 1:是
            resultObj.putOpt("message", "盘口投注成功");
            return JSONUtil.toJsonStr(resultObj);
        } catch (Exception e) {
            // ---------------------- 进入注单校验逻辑 ----------------------
            log.info("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},封盘时间:{},进入确认是否需要补单逻辑：下注请求超时，启动最大 15 秒注单匹配流程",
                    username, userConfig.getAccount(), drawNumber, "合并型方案", LocalDateTimeUtil.of(closeTime));

            long betTime = closeTime - System.currentTimeMillis();
            boolean successBet = false;                     // 标记是否确认投注成功
            long fixStart = System.currentTimeMillis();     // 补单判断阶段起始时间戳
            int maxTotalMillis = (int) (betTime - 5 * 1000);       // 补单判断总时长限制在关闭投注的5秒前
            long maxDurationMs = betTime - 5 * 1000;        // 最大注单匹配时间

            // 新API使用BetNew，注单校验逻辑需要根据新API的返回结果调整
            // TODO: 实现新API的注单校验逻辑
            if (CollUtil.isNotEmpty(mergedBetNewList)) {
                boolean[] found = new boolean[mergedBetNewList.size()];  // 标记各注单是否匹配成功
                int foundCnt = 0;                            // 已匹配成功注单计数
                int maxRetries = 5;                          // 最大轮询次数
                int retryInterval = 2_000;                   // 每轮查询间隔时间 2秒
                int pageSize = 15;                           // 接口分页大小

                // 多轮查询已下注注单，确认是否已成功
                for (int attempt = 1; attempt <= maxRetries && foundCnt < mergedBetNewList.size(); attempt++) {
                    long elapsed = System.currentTimeMillis() - fixStart;

                    // 超过最大判断时间，提前终止补单判断
                    if (elapsed >= maxTotalMillis) {
                        log.warn("翻倍计划-开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},补单确认阶段超过20秒，提前结束查询，已匹配 {}/{} 注单",
                                username, userConfig.getAccount(), drawNumber, "合并型方案", foundCnt, mergedBetNewList.size());
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
                        while (curPage <= totalPages && foundCnt < mergedBetNewList.size()) {
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

                                    // TODO: 实现新API的注单匹配逻辑
                                    for (int i = 0; i < mergedBetNewList.size(); i++) {
                                        if (!found[i]) {
                                            BetNew betNew = mergedBetNewList.get(i);
                                            // 根据新API的注单格式进行匹配
                                            if (draw.contains(drawNumber) && amount == Long.parseLong(betNew.getAmount())) {
                                                found[i] = true;
                                                ++foundCnt;
                                                log.info("开始注单确认检查,平台用户:{},盘口账号:{},期数:{},方案:{},匹配注单成功({}/{}): {}",
                                                        username, userConfig.getAccount(), drawNumber, "合并型方案", foundCnt, mergedBetNewList.size(), betNew);
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
                            username, userConfig.getAccount(), drawNumber, "合并型方案", attempt, maxRetries, foundCnt, mergedBetNewList.size());

                    if (foundCnt >= mergedBetNewList.size()) {
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

                successBet = (foundCnt >= mergedBetNewList.size());
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
     * 金额分配逻辑（新版本：统一使用所有账号，不再区分正投/反投）
     * 首先遍历oddsMap，获取oddsMap下每个位置key，然后根据所有账号和plan的金额配置，计算每个账户应该分配的金额
     * 新API：直接根据BettingPositionInfo分配金额，不再使用betKey格式
     */
    public static JSONObject distributeAmount(Map<Integer, BettingPositionInfo> oddsMap, JSONObject oddsJson, String wtype, List<UserConfig> allAccounts, PlanDoubleVO plan, Integer rounds) {
        JSONObject resultJson = new JSONObject();

        // 合并正投和反投金额，统一使用
        List<Integer> positiveAmounts = formatIds(plan.getPositiveAmount());
        int totalAmount = positiveAmounts.get(rounds);
        
        log.info("翻倍计划-统一金额分配-总金额:{}, 当前rounds:{}, 账号数:{}, wtype:{}", totalAmount, rounds, allAccounts.size(), wtype);
        
        // 遍历 oddsMap 中的每个投注位置信息
        oddsMap.forEach((positionId, positionInfo) -> {
            // 如果idIndex或rate还未计算，则根据游戏类型计算
            if (positionInfo.getIdIndex() == null || positionInfo.getRate() == null) {
                // 根据游戏类型和投注位置计算idIndex和rate
                calculateIdIndexAndRate(positionInfo, oddsJson, wtype);
            }
            
            // 如果计算失败，跳过该位置
            if (positionInfo.getIdIndex() == null || positionInfo.getRate() == null) {
                log.warn("翻倍计划-计算idIndex或rate失败, positionId:{}, positionInfo:{}", positionId, positionInfo);
                return;
            }
            
            // 统一分配金额给所有账号
            JSONObject accountAmountJson = distributeAmountsForAccounts(allAccounts, totalAmount);
            
            // 使用idIndex作为key，存储金额分配结果
            // 结构: {"位置ID": {"idIndex": {"account": amount}}}
            JSONObject idIndexJson = new JSONObject();
            idIndexJson.putOpt(positionInfo.getIdIndex(), accountAmountJson);
            
            resultJson.putOpt(String.valueOf(positionId), idIndexJson);
        });

        return resultJson;
    }
    
    /**
     * 根据游戏类型和投注位置信息计算idIndex和rate
     */
    private static void calculateIdIndexAndRate(BettingPositionInfo positionInfo, JSONObject oddsJson, String wtype) {
        if (positionInfo == null || oddsJson == null || StringUtils.isBlank(wtype)) {
            return;
        }
        
        try {
            if ("all".equals(wtype)) {
                // 主势盘：idIndex = 和值 + 1
                int sumValue = positionInfo.getPositionId();
                positionInfo.setIdIndex(String.valueOf(sumValue + 1));
                // 获取赔率：rate[和值 + 1]
                if (oddsJson.containsKey("rate")) {
                    JSONArray rateArray = oddsJson.getJSONArray("rate");
                    if (rateArray != null && rateArray.size() > sumValue + 1) {
                        positionInfo.setRate(rateArray.getStr(sumValue + 1));
                    }
                }
            } else if ("OPN".equals(wtype)) {
                // 单球1-3：根据球号和投注类型计算idIndex
                // 如果已经设置了idIndex和rate，则直接返回
                if (positionInfo.getIdIndex() != null && positionInfo.getRate() != null) {
                    return;
                }
                
                // 从positionInfo中获取球号和投注类型
                Integer ballNumber = positionInfo.getBallNumber();
                String betType = positionInfo.getBetType();
                String locationNum = positionInfo.getLocationNum();
                
                if (ballNumber == null || ballNumber < 1 || ballNumber > 3) {
                    log.warn("翻倍计划-单球1-3-球号无效, ballNumber:{}", ballNumber);
                    return;
                }
                
                // 如果betType已设置，直接使用
                if (StringUtils.isNotBlank(betType)) {
                    int baseIndex;
                    if ("大".equals(betType)) {
                        baseIndex = (ballNumber - 1) * 14 + 1;
                    } else if ("小".equals(betType)) {
                        baseIndex = (ballNumber - 1) * 14 + 2;
                    } else if ("单".equals(betType)) {
                        baseIndex = (ballNumber - 1) * 14 + 3;
                    } else if ("双".equals(betType)) {
                        baseIndex = (ballNumber - 1) * 14 + 4;
                    } else {
                        // 数字投注（0-9）
                        try {
                            int digit = Integer.parseInt(betType);
                            if (digit < 0 || digit > 9) {
                                log.warn("翻倍计划-单球1-3-数字投注值无效:{}", digit);
                                return;
                            }
                            baseIndex = (ballNumber - 1) * 14 + 5 + digit;
                        } catch (NumberFormatException e) {
                            log.warn("翻倍计划-单球1-3-投注类型格式错误:{}", betType);
                            return;
                        }
                    }
                    positionInfo.setIdIndex(String.valueOf(baseIndex));
                    
                    // 获取赔率
                    if (oddsJson.containsKey("rate")) {
                        JSONArray rateArray = oddsJson.getJSONArray("rate");
                        if (rateArray != null && rateArray.size() > baseIndex) {
                            positionInfo.setRate(rateArray.getStr(baseIndex));
                        }
                    }
                } else if (StringUtils.isNotBlank(locationNum)) {
                    // 如果betType未设置，尝试从locationNum解析
                    // locationNum可能是单个值或多个值（用逗号分隔）
                    List<String> betValues = formatIdsStr(locationNum);
                    if (betValues != null && !betValues.isEmpty()) {
                        // 取第一个值进行计算（如果有多个值，应该在调用前拆分）
                        String betValue = betValues.get(0).trim();
                        int baseIndex;
                        if ("大".equals(betValue)) {
                            baseIndex = (ballNumber - 1) * 14 + 1;
                        } else if ("小".equals(betValue)) {
                            baseIndex = (ballNumber - 1) * 14 + 2;
                        } else if ("单".equals(betValue)) {
                            baseIndex = (ballNumber - 1) * 14 + 3;
                        } else if ("双".equals(betValue)) {
                            baseIndex = (ballNumber - 1) * 14 + 4;
                        } else {
                            // 数字投注（0-9）
                            try {
                                int digit = Integer.parseInt(betValue);
                                if (digit < 0 || digit > 9) {
                                    log.warn("翻倍计划-单球1-3-数字投注值无效:{}", digit);
                                    return;
                                }
                                baseIndex = (ballNumber - 1) * 14 + 5 + digit;
                            } catch (NumberFormatException e) {
                                log.warn("翻倍计划-单球1-3-投注值格式错误:{}", betValue);
                                return;
                            }
                        }
                        positionInfo.setIdIndex(String.valueOf(baseIndex));
                        
                        // 获取赔率
                        if (oddsJson.containsKey("rate")) {
                            JSONArray rateArray = oddsJson.getJSONArray("rate");
                            if (rateArray != null && rateArray.size() > baseIndex) {
                                positionInfo.setRate(rateArray.getStr(baseIndex));
                            }
                        }
                    }
                } else {
                    log.warn("翻倍计划-单球1-3-投注类型和位置编号都为空, positionInfo:{}", positionInfo);
                }
            } else if ("TPD".equals(wtype) || "TPC".equals(wtype) || "TPB".equals(wtype)) {
                // 二字定位：idIndex = 第一位 * 10 + 第二位 + 1 (1-100)
                // positionInfo中应该包含firstDigit和secondDigit，或者从locationNum解析
                Integer firstDigit = positionInfo.getFirstDigit();
                Integer secondDigit = positionInfo.getSecondDigit();
                
                // 如果firstDigit和secondDigit未设置，尝试从locationNum解析
                if (firstDigit == null || secondDigit == null) {
                    String locationNum = positionInfo.getLocationNum();
                    if (StringUtils.isNotBlank(locationNum)) {
                        // locationNum存储的是投注位置（0-99，多个值用逗号分隔）
                        List<Integer> betPositions = formatIds(locationNum);
                        if (betPositions != null && !betPositions.isEmpty()) {
                            // 取第一个值进行解析（如果有多个值，应该在调用前拆分）
                            int betPosition = betPositions.get(0);
                            if (betPosition < 0 || betPosition > 99) {
                                log.warn("翻倍计划-二字定位-投注位置超出范围，应为0-99:{}", betPosition);
                                return;
                            }
                            // 将0-99转换为第一位和第二位数字
                            firstDigit = betPosition / 10;
                            secondDigit = betPosition % 10;
                            positionInfo.setFirstDigit(firstDigit);
                            positionInfo.setSecondDigit(secondDigit);
                        }
                    }
                }
                
                if (firstDigit == null || secondDigit == null) {
                    log.warn("翻倍计划-二字定位-无法解析第一位和第二位数字, positionInfo:{}", positionInfo);
                    return;
                }
                
                // 计算idIndex：idIndex = 第一位 * 10 + 第二位 + 1
                int idIndex = firstDigit * 10 + secondDigit + 1;
                if (idIndex < 1 || idIndex > 100) {
                    log.warn("翻倍计划-二字定位-idIndex超出范围, idIndex:{}, firstDigit:{}, secondDigit:{}", idIndex, firstDigit, secondDigit);
                    return;
                }
                
                positionInfo.setIdIndex(String.valueOf(idIndex));
                
                // 获取赔率
                if (oddsJson.containsKey("rate")) {
                    JSONArray rateArray = oddsJson.getJSONArray("rate");
                    if (rateArray != null && rateArray.size() > idIndex) {
                        positionInfo.setRate(rateArray.getStr(idIndex));
                    }
                }
            } else if ("DW3".equals(wtype)) {
                // 三字定位：idIndex = 三位数字符串
                // TODO: 实现三字定位的计算逻辑
                log.warn("翻倍计划-三字定位计算逻辑待实现, positionInfo:{}", positionInfo);
            } else if ("n5".equals(wtype)) {
                // 两面盘：根据投注类型映射idIndex
                // TODO: 实现两面盘的计算逻辑
                log.warn("翻倍计划-两面盘计算逻辑待实现, positionInfo:{}", positionInfo);
            } else {
                log.warn("翻倍计划-未知游戏类型:{}", wtype);
            }
        } catch (Exception e) {
            log.error("翻倍计划-计算idIndex和rate异常, positionInfo:{}, wtype:{}, 错误:{}", positionInfo, wtype, e.getMessage(), e);
        }
    }

    /**
     * 双面金额分配（新版本：统一使用所有账号，不再区分正投/反投）
     */
    /**
     * 双面金额分配（新版本：统一使用所有账号，不再区分正投/反投）
     * 新API：直接根据BettingPositionInfo分配金额，不再使用betKey格式
     */
    public static JSONObject twoSidedDistributeAmount(Map<String, List<BettingPositionInfo>> twoSidedMap, JSONObject oddsJson, String wtype, List<UserConfig> allAccounts, PlanDoubleVO plan, Integer rounds) {
        // 合并正投和反投金额，统一使用
        List<Integer> positiveAmounts = formatIds(plan.getPositiveAmount());
        int totalAmount = positiveAmounts.get(rounds);
        
        // 统一分配金额给所有账号
        JSONObject resultJson = new JSONObject();
        twoSidedMap.forEach((betType, positionInfos) -> {
            // 对每个投注类型的位置信息，计算idIndex和rate，然后分配金额
            for (BettingPositionInfo positionInfo : positionInfos) {
                // 如果idIndex或rate还未计算，则根据游戏类型计算
                if (positionInfo.getIdIndex() == null || positionInfo.getRate() == null) {
                    calculateTwoSidedIdIndexAndRate(positionInfo, oddsJson, wtype, betType);
                }
                
                // 如果计算失败，跳过该位置
                if (positionInfo.getIdIndex() == null || positionInfo.getRate() == null) {
                    log.warn("翻倍计划-计算双面投注idIndex或rate失败, betType:{}, positionInfo:{}", betType, positionInfo);
                    continue;
                }
                
                // 统一分配金额给所有账号
                JSONObject accountAmountJson = distributeAmountsForAccounts(allAccounts, totalAmount);
                
                // 使用idIndex作为key，存储金额分配结果
                // 结构: {"投注类型": {"idIndex": {"account": amount}}}
                JSONObject betTypeJson = resultJson.getJSONObject(betType);
                if (betTypeJson == null) {
                    betTypeJson = new JSONObject();
                    resultJson.putOpt(betType, betTypeJson);
                }
                betTypeJson.putOpt(positionInfo.getIdIndex(), accountAmountJson);
            }
        });

        return resultJson;
    }
    
    /**
     * 根据游戏类型和双面投注类型计算idIndex和rate
     */
    private static void calculateTwoSidedIdIndexAndRate(BettingPositionInfo positionInfo, JSONObject oddsJson, String wtype, String betType) {
        if (positionInfo == null || oddsJson == null || StringUtils.isBlank(wtype) || StringUtils.isBlank(betType)) {
            return;
        }
        
        try {
            if ("all".equals(wtype)) {
                // 主势盘：双面投注暂不支持，需要根据实际需求实现
                log.warn("翻倍计划-主势盘双面投注计算逻辑待实现, betType:{}", betType);
            } else if ("OPN".equals(wtype)) {
                // 单球1-3：根据球号和投注类型计算idIndex
                // 对于双面投注，positionId 是球号（1、2、3），betType 是投注类型（大、小、单、双）
                Integer ballNumber = positionInfo.getPositionId();
                if (ballNumber == null || ballNumber < 1 || ballNumber > 3) {
                    log.warn("翻倍计划-单球1-3双面投注-球号无效, ballNumber:{}", ballNumber);
                    return;
                }
                
                // 根据投注类型计算idIndex
                int baseIndex;
                if ("大".equals(betType)) {
                    baseIndex = (ballNumber - 1) * 14 + 1;
                } else if ("小".equals(betType)) {
                    baseIndex = (ballNumber - 1) * 14 + 2;
                } else if ("单".equals(betType)) {
                    baseIndex = (ballNumber - 1) * 14 + 3;
                } else if ("双".equals(betType)) {
                    baseIndex = (ballNumber - 1) * 14 + 4;
                } else {
                    log.warn("翻倍计划-单球1-3双面投注-投注类型无效, betType:{}", betType);
                    return;
                }
                
                positionInfo.setIdIndex(String.valueOf(baseIndex));
                positionInfo.setBallNumber(ballNumber);
                positionInfo.setBetType(betType);
                
                // 获取赔率
                if (oddsJson.containsKey("rate")) {
                    JSONArray rateArray = oddsJson.getJSONArray("rate");
                    if (rateArray != null && rateArray.size() > baseIndex) {
                        positionInfo.setRate(rateArray.getStr(baseIndex));
                    }
                }
            } else if ("TPD".equals(wtype) || "TPC".equals(wtype) || "TPB".equals(wtype)) {
                // 二字定位：双面投注暂不支持，需要根据实际需求实现
                log.warn("翻倍计划-二字定位双面投注计算逻辑待实现, betType:{}, positionInfo:{}, wtype:{}", betType, positionInfo, wtype);
            } else if ("n5".equals(wtype)) {
                // 两面盘：根据投注类型映射idIndex
                // TODO: 实现两面盘的计算逻辑
                log.warn("翻倍计划-两面盘计算逻辑待实现, betType:{}, positionInfo:{}", betType, positionInfo);
            } else {
                log.warn("翻倍计划-未知游戏类型或双面投注不支持:{}", wtype);
            }
        } catch (Exception e) {
            log.error("翻倍计划-计算双面投注idIndex和rate异常, positionInfo:{}, wtype:{}, betType:{}, 错误:{}", positionInfo, wtype, betType, e.getMessage(), e);
        }
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
        int initialShare = 1;
        int remainingAmount = totalAmount - betNum * initialShare;

        if (remainingAmount < 0) {
            log.info("翻倍计划-金额不足以保证每个投注至少 1 元");
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
                log.error("翻倍计划-分配金额出现小于2的情况 accounts大小:{}, totalAmount:{}, 分配结果:{}", accounts.size(), totalAmount, amountJson);
            }
        }
        return amountJson;
    }

}
