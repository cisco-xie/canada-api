package com.example.demo.api;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.IdUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.example.demo.common.constants.RedisConstants;
import com.example.demo.common.enmu.SystemError;
import com.example.demo.common.utils.KeyUtil;
import com.example.demo.common.utils.ToDayRangeUtil;
import com.example.demo.core.exception.BusinessException;
import com.example.demo.core.model.UserConfig;
import com.example.demo.model.dto.AdminLoginDTO;
import com.example.demo.model.vo.*;
import com.example.demo.model.vo.plandouble.PlanDoubleVO;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.redisson.api.*;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@Component
public class ConfigService {

    @Resource(name = "defaultRedissonClient")
    private RedissonClient redisson;

    @Resource(name = "businessUserRedissonClient")
    private RedissonClient businessUserRedissonClient;

    /**
     * 获取平台用户是否开启自动下注
     * @param username
     * @return
     */
    public int getAutoBet(String username) {
        String admin = String.valueOf(businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, username)).get());
        return JSONUtil.parseObj(admin).getInt("autoBet");
    }

    /**
     * 设置平台用户是否开启自动下注
     * @param username
     * @return
     */
    public void autoBet(String username, int autoBet) {
        String admin = String.valueOf(businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, username)).get());
        JSONObject json = JSONUtil.parseObj(admin);
        json.putOpt("autoBet", autoBet);
        if (0 == autoBet) {
            // 如果是关闭自动下注，则把所有的计划监控期数清空
            String pattern = KeyUtil.genKey(RedisConstants.USER_PLAN_DOUBLE_PREFIX, username, "*");
            // 使用通配符获取所有匹配的键
            for (String key : businessUserRedissonClient.getKeys().getKeysByPattern(pattern)) {
                // 根据键值获取数据
                String configJson = (String) businessUserRedissonClient.getBucket(key).get();
                if (configJson != null) {
                    // 将 JSON 转换为对象
                    PlanDoubleVO configPlan = JSONUtil.toBean(configJson, PlanDoubleVO.class);
                    clearMonitor(username, configPlan.getLottery(), String.valueOf(configPlan.getId()));
                }
            }
        }
        businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, username)).set(json);
    }

    /**
     * 获取平台用户是否开启计划超时跳过
     * @param username
     * @return
     */
    public int getSkip(String username) {
        String admin = String.valueOf(businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, username)).get());
        return JSONUtil.parseObj(admin).getInt("skip");
    }

    /**
     * 设置平台用户是否开启计划超时跳过
     * @param username
     * @return
     */
    public void skip(String username, int skip) {
        String admin = String.valueOf(businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, username)).get());
        JSONObject json = JSONUtil.parseObj(admin);
        json.putOpt("skip", skip);
        businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, username)).set(json);
    }

    /**
     * 设置账号代理
     * @param userProxy
     */
    public String editAccount(String username, ConfigUserVO userProxy) {
        // 校验并修正 baseUrl
        String baseUrl = userProxy.getBaseUrl();
        if (StringUtils.isNotBlank(baseUrl) && !baseUrl.endsWith("/")) {
            baseUrl += "/";
            userProxy.setBaseUrl(baseUrl);
        }

        // VO 转实体
        UserConfig proxy = BeanUtil.copyProperties(userProxy, UserConfig.class);
        proxy.setBaseUrl(baseUrl);

        String id = StringUtils.isNotBlank(proxy.getId()) ? proxy.getId() : IdUtil.getSnowflakeNextIdStr();
        // Redis 键值生成
        String redisKey = KeyUtil.genKey(
                RedisConstants.USER_PROXY_PREFIX,
                username,
                id
        );

        // 操作类型处理
        if ("add".equalsIgnoreCase(userProxy.getOperationType())) {
            String json = (String) businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, username)).get();
            AdminLoginDTO admin = JSONUtil.toBean(json, AdminLoginDTO.class);
//            if (admin.getRoles().contains("agent")) {
//                // 获取与模式匹配的所有键
//                Iterable<String> keys = businessUserRedissonClient.getKeys().getKeysByPattern(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, "*"));
//                // 判断是否存在账户
//                boolean hasExistingProxy = false;
//                for (String key : keys) {
//                    if (key != null && !key.isEmpty()) {
//                        hasExistingProxy = true;
//                        break; // 找到匹配的键，提前退出循环
//                    }
//                }
//                if (hasExistingProxy) {
//                    throw new BusinessException(SystemError.USER_1014);  // 自定义错误提示
//                }
//            }
            if (businessUserRedissonClient.getBucket(redisKey).isExists()) {
                log.warn("用户 [{}] 已存在，无法新增。", userProxy.getAccount());
                throw new BusinessException(SystemError.USER_1003, userProxy.getAccount());
            }

            // 设置 ID 并保存到 Redis
            proxy.setId(id);
            businessUserRedissonClient.getBucket(redisKey).set(JSONUtil.toJsonStr(proxy));
            log.info("用户 [{}] 新增成功。", userProxy.getAccount());
        } else if ("update".equalsIgnoreCase(userProxy.getOperationType())) {
            if (!businessUserRedissonClient.getBucket(redisKey).isExists()) {
                log.warn("用户 [{}] 不存在，无法修改。", userProxy.getAccount());
                throw new BusinessException(SystemError.USER_1004, userProxy.getAccount());
            }

            // 保留 token 并更新
            UserConfig existingConfig = JSONUtil.toBean(
                    JSONUtil.parseObj(businessUserRedissonClient.getBucket(redisKey).get()),
                    UserConfig.class
            );
            proxy.setIsAutoLogin(existingConfig.getIsAutoLogin());
            proxy.setIsTokenValid(existingConfig.getIsTokenValid());
            proxy.setToken(existingConfig.getToken());
            proxy.setBalance(existingConfig.getBalance());
            proxy.setBetting(existingConfig.getBetting());
            proxy.setAmount(existingConfig.getAmount());
            proxy.setResult(existingConfig.getResult());
            businessUserRedissonClient.getBucket(redisKey).set(JSONUtil.toJsonStr(proxy));
            log.info("用户 [{}] 修改成功。", userProxy.getAccount());

        } else {
            throw new BusinessException(SystemError.SYS_400, "不支持的操作类型: " + userProxy.getOperationType());
        }
        return id;
    }

    /**
     * 删除账号
     * @param id
     */
    public void deleteAccount(String username, String id) {
        businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, id)).delete();
    }

    /**
     * 获取所有账号
     */
    public List<UserConfig> accounts(String username, String id) {
        // 使用通配符获取所有匹配的键
        Iterator<String> keys = businessUserRedissonClient.getKeys().getKeysByPattern(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, StringUtils.isNotBlank(id) ? id : "*")).iterator();

        List<UserConfig> userList = new ArrayList<>();
        while (keys.hasNext()) {
            // 根据键值获取数据
            String userJson = (String) businessUserRedissonClient.getBucket(keys.next()).get();
            if (userJson != null) {
                // 将 JSON 转换为对象
                UserConfig userConfig = JSONUtil.toBean(userJson, UserConfig.class);
                userList.add(userConfig);
            }
        }
        // 返回所有用户列表
        return userList;
    }

    /**
     * 获取所有账号 - 使用RBatch批量执行
     */
    public List<UserConfig> accountsRBatch(String username, String id) {
        // 使用通配符获取所有匹配的键
        Iterator<String> keys = businessUserRedissonClient.getKeys().getKeysByPattern(KeyUtil.genKey(RedisConstants.USER_PROXY_PREFIX, username, StringUtils.isNotBlank(id) ? id : "*")).iterator();

        // 创建 RBatch 批处理对象
        RBatch batch = businessUserRedissonClient.createBatch();

        // 存储 RBucket 实例
        List<RBucket<String>> buckets = new ArrayList<>();

        // 将每个操作添加到批处理队列
        while (keys.hasNext()) {
            RBucket<String> bucket = businessUserRedissonClient.getBucket(keys.next()); // 获取 RBucket 对象
            bucket.getAsync(); // 为每个键添加获取操作
            buckets.add(bucket);
        };

        // 提交所有批量请求
        batch.execute();

        // 收集结果
        List<UserConfig> userList = new ArrayList<>();
        buckets.forEach(bucket -> {
            String userJson = bucket.get();  // 获取异步任务的结果
            if (userJson != null) {
                // 将 JSON 转换为对象
                userList.add(JSONUtil.toBean(userJson, UserConfig.class));
            }
        });

        return userList;
    }

    /**
     * 一键启用停用方案
     * @param username
     * @param enable
     */
    public void plan(String username, Integer enable) {
        // 设置自增 ID 的 Redis key
        String pattern = KeyUtil.genKey(RedisConstants.USER_PLAN_PREFIX, username, "*");
        // 使用通配符获取所有匹配的键
        for (String key : businessUserRedissonClient.getKeys().getKeysByPattern(pattern)) {
            // 根据键值获取数据
            String configJson = (String) businessUserRedissonClient.getBucket(key).get();
            if (configJson != null) {
                // 将 JSON 转换为对象
                ConfigPlanVO configPlan = JSONUtil.toBean(configJson, ConfigPlanVO.class);
                configPlan.setEnable(enable);
                // 将更新后的列表保存回 Redis
                businessUserRedissonClient.getBucket(key).set(JSONUtil.toJsonStr(configPlan));
            }
        }
    }

    /**
     * 关闭计划时清空所有监控信息
     * @param username
     * @param planId
     */
    public void clearMonitor(String username, String lottery, String planId) {
        String monitorKey = KeyUtil.genKey(
                RedisConstants.USER_MONITOR_PREFIX,
                username,
                planId
        );
        businessUserRedissonClient.getList(monitorKey).clear();
        String orderKey = KeyUtil.genKey(
                RedisConstants.USER_BET_ORDER_DOUBLE_PREFIX,
                lottery,
                username,
                planId,
                "*",
                "*"
        );
        // 删除老订单信息
        businessUserRedissonClient.getKeys().deleteByPattern(orderKey);
    }

    /**
     * 一键启用停用翻倍方案
     * @param username
     * @param enable
     */
    public void planDouble(String username, Integer enable) {
        // 设置自增 ID 的 Redis key
        String pattern = KeyUtil.genKey(RedisConstants.USER_PLAN_DOUBLE_PREFIX, username, "*");
        // 使用通配符获取所有匹配的键
        for (String key : businessUserRedissonClient.getKeys().getKeysByPattern(pattern)) {
            // 根据键值获取数据
            String configJson = (String) businessUserRedissonClient.getBucket(key).get();
            if (configJson != null) {
                // 将 JSON 转换为对象
                PlanDoubleVO configPlan = JSONUtil.toBean(configJson, PlanDoubleVO.class);
                configPlan.setEnable(enable);
                if (0 == enable) {
                    clearMonitor(username, configPlan.getLottery(), String.valueOf(configPlan.getId()));
                }
                // 将更新后的列表保存回 Redis
                businessUserRedissonClient.getBucket(key).set(JSONUtil.toJsonStr(configPlan));
            }
        }
    }

    /**
     * 设置账号方案
     * @param plan
     */
    public void plan(String username, ConfigPlanVO plan) {
        String key = null;
        if (BeanUtil.isNotEmpty(plan)) {
            if (CollUtil.isEmpty(plan.getPositions())
                    && CollUtil.isEmpty(plan.getTwoSidedDxPositions())
                    && CollUtil.isEmpty(plan.getTwoSidedDsPositions())
                    && CollUtil.isEmpty(plan.getTwoSidedLhPositions())) {
                throw new BusinessException(SystemError.USER_1013);
            }
            // 设置自增 ID 的 Redis key
            String idKey = KeyUtil.genKey(RedisConstants.USER_PLAN_ID_PREFIX, username);
            ConfigPlanVO configPlan = new ConfigPlanVO();
            if (plan.getId() == null) {
                // 如果传入的 ID 为空，表示新增
                long newId = businessUserRedissonClient.getAtomicLong(idKey).incrementAndGet(); // 生成自增 ID
                if (newId > Integer.MAX_VALUE) {
                    log.error("生成的 ID 超出 Integer 范围: {}", newId);
                    throw new BusinessException(SystemError.SYS_400);
                }
                plan.setId((int) newId); // 将 long 转为 Integer 类型
            }
            // 更新对象
            configPlan = plan;
            key = KeyUtil.genKey(RedisConstants.USER_PLAN_PREFIX, username, plan.getLottery(), String.valueOf(plan.getId()));
            // 将更新后的列表保存回 Redis
            businessUserRedissonClient.getBucket(key).set(JSONUtil.toJsonStr(configPlan));
        }
    }

    /**
     * 设置翻倍方案
     * @param plan
     */
    public void planDouble(String username, PlanDoubleVO plan) {
        String key = null;
        if (BeanUtil.isNotEmpty(plan)) {
            if (CollUtil.isEmpty(plan.getPositions())
                    && CollUtil.isEmpty(plan.getTwoSidedDxPositions())
                    && CollUtil.isEmpty(plan.getTwoSidedDsPositions())
                    && CollUtil.isEmpty(plan.getTwoSidedLhPositions())) {
                throw new BusinessException(SystemError.USER_1013);
            }
            // 设置自增 ID 的 Redis key
            String idKey = KeyUtil.genKey(RedisConstants.USER_PLAN_DOUBLE_ID_PREFIX, username);
            PlanDoubleVO configPlan;
            if (plan.getId() == null) {
                // 如果传入的 ID 为空，表示新增
                long newId = businessUserRedissonClient.getAtomicLong(idKey).incrementAndGet(); // 生成自增 ID
                if (newId > Integer.MAX_VALUE) {
                    log.error("生成的 ID 超出 Integer 范围: {}", newId);
                    throw new BusinessException(SystemError.SYS_400);
                }
                plan.setId((int) newId); // 将 long 转为 Integer 类型
            }
            // 更新对象
            configPlan = plan;
            key = KeyUtil.genKey(RedisConstants.USER_PLAN_DOUBLE_PREFIX, username, plan.getLottery(), String.valueOf(plan.getId()));
            if (0 == plan.getEnable()) {
                clearMonitor(username, configPlan.getLottery(), String.valueOf(configPlan.getId()));
            }
            // 将更新后的列表保存回 Redis
            businessUserRedissonClient.getBucket(key).set(JSONUtil.toJsonStr(configPlan));
        }
    }

    /**
     * 删除翻倍配置
     * @param plans
     */
    public void planDoubleDel(String username, List<PlanDoubleVO> plans) {
        if (CollUtil.isEmpty(plans)) {
            throw new BusinessException(SystemError.SYS_402);
        }
        plans.forEach(plan -> {
            // 检查传入计划对象是否为空
            if (BeanUtil.isEmpty(plan) || plan.getId() == null) {
                throw new BusinessException(SystemError.SYS_402);
            }

            // 生成 Redis 键
            String key = KeyUtil.genKey(RedisConstants.USER_PLAN_DOUBLE_PREFIX, username, plan.getLottery(), String.valueOf(plan.getId()));
            businessUserRedissonClient.getBucket(key).delete();
        });
    }

    /**
     * 删除配置
     * @param plans
     */
    public void planDel(String username, List<ConfigPlanVO> plans) {
        if (CollUtil.isEmpty(plans)) {
            throw new BusinessException(SystemError.SYS_402);
        }
        plans.forEach(plan -> {
            // 检查传入计划对象是否为空
            if (BeanUtil.isEmpty(plan) || plan.getId() == null) {
                throw new BusinessException(SystemError.SYS_402);
            }

            // 生成 Redis 键
            String key = KeyUtil.genKey(RedisConstants.USER_PLAN_PREFIX, username, plan.getLottery(), String.valueOf(plan.getId()));
            businessUserRedissonClient.getBucket(key).delete();
        });
    }

    /**
     * 获取用户的所有方案
     */
    public List<ConfigPlanVO> getAllPlans(String username, String lottery) {
        // 匹配所有用户和 lottery 的 Redis Key
        String pattern = KeyUtil.genKey(RedisConstants.USER_PLAN_PREFIX, StringUtils.isNotBlank(username) ? username : "*", StringUtils.isNotBlank(lottery) ? lottery : "*", "*");

        // 使用 Redisson 执行扫描操作
        RKeys keys = businessUserRedissonClient.getKeys();
        Iterator<String> iterableKeys = keys.getKeysByPattern(pattern).iterator();

        // 批量获取所有方案
        List<ConfigPlanVO> plans = new ArrayList<>();
        while (iterableKeys.hasNext()) {
            // 使用 Redisson 获取每个 key 的数据
            String json = (String) businessUserRedissonClient.getBucket(iterableKeys.next()).get();
            if (json != null) {
                // 解析 JSON 为 ConfigPlanVO 对象
                plans.add(JSONUtil.toBean(json, ConfigPlanVO.class));
            }
        }

        return plans;
    }

    /**
     * 获取用户的所有翻倍方案 - 使用RBatch批量执行
     * @param username
     * @param lottery
     * @return
     */
    public List<PlanDoubleVO> getAllPlanDoublesRBatch(String username, String lottery) {
        // 匹配所有用户和 lottery 的 Redis Key
        String pattern = KeyUtil.genKey(RedisConstants.USER_PLAN_DOUBLE_PREFIX,
                StringUtils.isNotBlank(username) ? username : "*",
                StringUtils.isNotBlank(lottery) ? lottery : "*",
                "*");

        // 使用 Redisson 执行扫描操作
        RKeys keys = businessUserRedissonClient.getKeys();
        Iterator<String> iterator = keys.getKeysByPattern(pattern).iterator();  // 使用 scanIterator 替代 getKeysByPattern

        List<String> keysList = new ArrayList<>();

        // 增量遍历键
        while (iterator.hasNext()) {
            String key = iterator.next();
            keysList.add(key);
        }

        // 创建一个 RBatch 批处理对象
        RBatch batch = businessUserRedissonClient.createBatch();

        // 用于存储每个 Redis 获取操作的结果
        List<RBucket<String>> buckets = new ArrayList<>();

        // 添加批量获取操作
        keysList.forEach(key -> {
            RBucket<String> bucket = businessUserRedissonClient.getBucket(key); // 获取 RBucket 对象
            bucket.getAsync(); // 为每个键添加获取操作
            buckets.add(bucket);
        });

        // 提交所有批量请求
        batch.execute();

        // 收集结果
        List<PlanDoubleVO> plans = new ArrayList<>();
        buckets.forEach(bucket -> {
            String json = bucket.get();  // 获取每个键的值
            if (json != null) {
                plans.add(JSONUtil.toBean(json, PlanDoubleVO.class));
            }
        });

        return plans;
    }

    /**
     * 获取用户的所有方案 - 使用RBatch批量执行
     * @param username
     * @param lottery
     * @return
     */
    public List<ConfigPlanVO> getAllPlansRBatch(String username, String lottery) {
        // 匹配所有用户和 lottery 的 Redis Key
        String pattern = KeyUtil.genKey(RedisConstants.USER_PLAN_PREFIX,
                StringUtils.isNotBlank(username) ? username : "*",
                StringUtils.isNotBlank(lottery) ? lottery : "*",
                "*");

        // 使用 Redisson 执行扫描操作
        RKeys keys = businessUserRedissonClient.getKeys();
        Iterator<String> iterator = keys.getKeysByPattern(pattern).iterator();  // 使用 scanIterator 替代 getKeysByPattern

        List<String> keysList = new ArrayList<>();

        // 增量遍历键
        while (iterator.hasNext()) {
            String key = iterator.next();
            keysList.add(key);
        }

        // 创建一个 RBatch 批处理对象
        RBatch batch = businessUserRedissonClient.createBatch();

        // 用于存储每个 Redis 获取操作的结果
        List<RBucket<String>> buckets = new ArrayList<>();

        // 添加批量获取操作
        keysList.forEach(key -> {
            RBucket<String> bucket = businessUserRedissonClient.getBucket(key); // 获取 RBucket 对象
            bucket.getAsync(); // 为每个键添加获取操作
            buckets.add(bucket);
        });

        // 提交所有批量请求
        batch.execute();

        // 收集结果
        List<ConfigPlanVO> plans = new ArrayList<>();
        buckets.forEach(bucket -> {
            String json = bucket.get();  // 获取每个键的值
            if (json != null) {
                plans.add(JSONUtil.toBean(json, ConfigPlanVO.class));
            }
        });

        return plans;
    }

    /**
     * 获取用户失败投注记录列表
     * @param username
     * @return
     */
    public JSONArray failedBet(String username) {
        JSONArray result = new JSONArray();
        result.addAll(getFailedRecordsByPattern(RedisConstants.USER_BET_PERIOD_RES_PREFIX, username));
        result.addAll(getFailedRecordsByPattern(RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX, username));
        return result;
    }

    private List<JSONObject> getFailedRecordsByPattern(String prefix, String username) {
        List<JSONObject> result = new ArrayList<>();

        String pattern = KeyUtil.genKey(
                prefix, "*", ToDayRangeUtil.getToDayRange(), "*", "failed", username, "*"
        );

        RKeys keys = businessUserRedissonClient.getKeys();
        Iterator<String> keyIterator = keys.getKeysByPattern(pattern).iterator();

        int maxLimit = 500; // 最多处理 500 条，避免性能问题
        int count = 0;

        while (keyIterator.hasNext() && count < maxLimit) {
            String key = keyIterator.next();
            Object value = businessUserRedissonClient.getBucket(key).get();

            if (value != null) {
                JSONObject jsonObject = JSONUtil.parseObj(value.toString());
                JSONObject msg = new JSONObject();

                String[] parts = key.split(":");
                String account = parts.length > 8 ? parts[8] : "unknown";

                msg.putOpt("account", account);
                msg.putOpt("message", jsonObject.getStr("message"));
                msg.putOpt("lottery", jsonObject.getStr("lottery"));
                msg.putOpt("drawNumber", jsonObject.getStr("drawNumber"));
                msg.putOpt("createTime", jsonObject.getStr("createTime"));
                msg.putOpt("isRepair", jsonObject.getInt("isRepair"));
                msg.putOpt("repair", jsonObject.getInt("repair"));
                msg.putOpt("repairAccount", jsonObject.getStr("repairAccount"));
                msg.putOpt("planName", jsonObject.getStr("planName"));
                msg.putOpt("type", jsonObject.getInt("type"));

                result.add(msg);
                count++;
            }
        }

        return result;
    }

    /**
     * 获取失败投注最新
     * @param username
     * @return
     */
    public JSONObject failedBetLatestNomore(String username) {
        JSONObject maxCreateTimeObj = new JSONObject();
        String maxCreateTime = null;
        JSONObject maxCreateTimeJson = null;

        // Redis key pattern
        String pattern = KeyUtil.genKey(
                RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                "*",
                ToDayRangeUtil.getToDayRange(),
                "*",
                "failed",
                username,
                "*"
        );

        RKeys keys = redisson.getKeys();
        Iterator<String> iterableKeys = keys.getKeysByPattern(pattern).iterator();
        List<String> keysList = new ArrayList<>();
        while (iterableKeys.hasNext()) {
            keysList.add(iterableKeys.next());
        }

        if (!keysList.isEmpty()) {
            // 创建批量任务
            for (String key : keysList) {
                String json = (String) redisson.getBucket(key).get();
                JSONObject jsonObject = JSONUtil.parseObj(json);
                String currentCreateTime = jsonObject.getStr("createTime");
                // 查找最大的 createTime
                if (maxCreateTime == null || currentCreateTime.compareTo(maxCreateTime) > 0) {
                    maxCreateTime = currentCreateTime;
                    maxCreateTimeJson = jsonObject;
                    maxCreateTimeObj.putOpt("key", key);
                }
            };
            // 校验最大时间记录的 confirm 字段是否为 0
            if (maxCreateTimeJson != null && maxCreateTimeJson.containsKey("confirm") && maxCreateTimeJson.getInt("confirm") == 0) {
                maxCreateTimeObj.putOpt("value", maxCreateTimeJson);
            } else {
                // 如果没有符合条件的记录，返回空对象
                maxCreateTimeObj.clear();
            }
        }

        return maxCreateTimeObj;
    }

    /**
     * 获取最新失败投注
     * @param username
     * @return
     */
    public String failedBetLatest(String username) {
        JSONObject maxCreateTimeObj = new JSONObject();
        String maxCreateTime = null;
        JSONObject maxCreateTimeJson = null;

        // Redis key pattern
        String pattern = KeyUtil.genKey(
                RedisConstants.USER_BET_PERIOD_RES_PREFIX,
                "*",
                ToDayRangeUtil.getToDayRange(),
                "*",
                "failed",
                username,
                "*"
        );

        RKeys keys = businessUserRedissonClient.getKeys();
        Iterator<String> iterableKeys = keys.getKeysByPattern(pattern).iterator();
        List<String> keysList = new ArrayList<>();
        while (iterableKeys.hasNext()) {
            keysList.add(iterableKeys.next());
        }

        String patternPlanDouble = KeyUtil.genKey(
                RedisConstants.USER_BET_DOUBLE_PERIOD_RES_PREFIX,
                "*",
                ToDayRangeUtil.getToDayRange(),
                "*",
                "failed",
                username,
                "*"
        );

        RKeys keysPlanDouble = businessUserRedissonClient.getKeys();
        Iterator<String> iterableKeysPlanDouble = keysPlanDouble.getKeysByPattern(patternPlanDouble).iterator();
        while (iterableKeysPlanDouble.hasNext()) {
            keysList.add(iterableKeysPlanDouble.next());
        }

        if (!keysList.isEmpty()) {
            // 创建批量任务
            RBatch batch = businessUserRedissonClient.createBatch();
            Map<String, RFuture<Object>> futures = new HashMap<>();

            // 将所有 Redis Key 加入批量任务
            for (String key : keysList) {
                RFuture<Object> future = batch.getBucket(key).getAsync();
                futures.put(key, future);
            }

            // 执行批量任务
            batch.execute();

            // 遍历批量结果
            for (Map.Entry<String, RFuture<Object>> entry : futures.entrySet()) {
                String key = entry.getKey();
                try {
                    Object value = entry.getValue().get(); // 获取异步结果
                    if (value != null) {
                        String json = value.toString();
                        JSONObject jsonObject = JSONUtil.parseObj(json);
                        String currentCreateTime = jsonObject.getStr("createTime");

                        // 查找最大的 createTime
                        if (maxCreateTime == null || currentCreateTime.compareTo(maxCreateTime) > 0) {
                            maxCreateTime = currentCreateTime;
                            maxCreateTimeJson = jsonObject;
                            maxCreateTimeObj.putOpt("key", key);
                        }
                    }
                } catch (InterruptedException | ExecutionException e) {
                    log.error("Redis 批量处理获取下注失败数据时发生错误 Redis key: {}", entry.getKey(), e);
                }
            }

            // 校验最大时间记录的 confirm 字段是否为 0
            if (maxCreateTimeJson != null && maxCreateTimeJson.containsKey("confirm") && maxCreateTimeJson.getInt("confirm") == 0) {
                maxCreateTimeObj.putOpt("value", maxCreateTimeJson);
            } else {
                // 如果没有符合条件的记录，返回空对象
                maxCreateTimeObj.clear();
            }
        }

        return JSONUtil.toJsonStr(maxCreateTimeObj);
    }


    /**
     * 确认下注失败弹窗提示
     * @param redisKey
     */
    public void failedBetLatestConfirm(String redisKey) {
        if (!businessUserRedissonClient.getBucket(redisKey).isExists()) {
            log.warn("redis key [{}] 不存在", redisKey);
        }
        JSONObject jsonObject = JSONUtil.parseObj(businessUserRedissonClient.getBucket(redisKey).get());
        jsonObject.putOpt("confirm", 1);
        businessUserRedissonClient.getBucket(redisKey).set(jsonObject);
    }

    /**
     * 获取当前平台用户
     * @return
     */
    public AdminLoginDTO getUser(String username) {
        String json = (String) businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, username)).get();
        return JSONUtil.toBean(json, AdminLoginDTO.class);
    }

    /**
     * 获取所有平台用户
     * @return
     */
    public List<AdminLoginDTO> getUsers(String group) {
        // 匹配所有平台用户的 Redis Key
        String pattern = KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, "*");
        // 使用 Redisson 执行扫描所有平台用户操作
        RKeys keys = businessUserRedissonClient.getKeys();
        Iterator<String> iterableKeys = keys.getKeysByPattern(pattern).iterator();
        List<String> keysList = new ArrayList<>();
        while (iterableKeys.hasNext()) {
            keysList.add(iterableKeys.next());
        }
        return keysList.stream()
                .map(key -> {
                    String json = (String) businessUserRedissonClient.getBucket(key).get();
                    return JSONUtil.toBean(json, AdminLoginDTO.class);
                })
                .filter(user -> StringUtils.isBlank(group) || group.equals(user.getGroup())) // 如果 group 为空，跳过过滤
                .collect(Collectors.toList());
    }

    public void add(List<AdminLoginVO> admins) {
        for (AdminLoginVO admin : admins) {
            if (null == admin.getUsername()) {
                continue;
            }
            List<String> roles = new ArrayList<>();
            if (null == admin.getRoles() || admin.getRoles().isEmpty()) {
                // 默认普通用户角色
                roles = List.of("common");
            } else {
                roles = List.of(admin.getRoles());
            }
            String key = KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, admin.getUsername());
            AdminLoginDTO adminLoginDTO = new AdminLoginDTO();
            adminLoginDTO.setUsername(admin.getUsername());
            adminLoginDTO.setNickname(admin.getUsername());
            adminLoginDTO.setPassword(admin.getPassword());
            adminLoginDTO.setGroup(admin.getGroup());
            adminLoginDTO.setExpires("2030/10/30 00:00:00");
            adminLoginDTO.setRoles(roles);
            adminLoginDTO.setPermissions(List.of("*:*:*"));
            businessUserRedissonClient.getBucket(key).set(JSONUtil.toJsonStr(adminLoginDTO));
        };
    }

    /**
     * 更新安全码
     * @param username
     * @param securityCode
     */
    public void updateSecurityCode(String username, String securityCode) {
        String key = KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, username);

        // 获取现有用户数据
        RBucket<String> bucket = businessUserRedissonClient.getBucket(key);
        String userJson = bucket.get();

        if (userJson != null) {
            // 将 JSON 转换为对象
            AdminLoginDTO adminLoginDTO = JSONUtil.toBean(userJson, AdminLoginDTO.class);
            // 只更新 securityCode
            adminLoginDTO.setSecurityCode(securityCode);
            // 保存回 Redis
            bucket.set(JSONUtil.toJsonStr(adminLoginDTO));
        } else {
            // 如果用户不存在，可能需要创建或抛异常
            throw new BusinessException(SystemError.USER_1004);
        }
    }

    public void delUser(String username) {
        businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, username)).delete();
    }

    public List<String> getGroup() {
        // 匹配所有平台用户的 Redis Key
        String pattern = KeyUtil.genKey(RedisConstants.USER_ADMIN_GROUP_PREFIX, "*");
        // 使用 Redisson 执行扫描所有平台用户操作
        RKeys keys = businessUserRedissonClient.getKeys();
        Iterator<String> iterableKeys = keys.getKeysByPattern(pattern).iterator();
        List<String> groupList = new ArrayList<>();
        // 遍历所有匹配的键
        while (iterableKeys.hasNext()) {
            String[] parts = iterableKeys.next().split(":");
            // 检查数组长度，确保索引存在
            if (parts.length > 2) {
                groupList.add(parts[2]);
            }
        }
        return groupList;
    }

    public void addGroup(String group) {
        businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_ADMIN_GROUP_PREFIX, group)).set(1);
    }

    public void add(String usernames) {
        String[] usernameArray = usernames.split(",");

        for (String username : usernameArray) {
            username = username.trim();

            AdminLoginDTO adminLoginDTO = new AdminLoginDTO();
            adminLoginDTO.setUsername(username);
            adminLoginDTO.setNickname(username);
            adminLoginDTO.setPassword("user123456");
            adminLoginDTO.setRoles(List.of("admin"));
            adminLoginDTO.setPermissions(List.of("*:*:*"));
            adminLoginDTO.setExpires("2030/10/30 00:00:00");

            businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_ADMIN_PREFIX, username)).set(JSONUtil.toJsonStr(adminLoginDTO));
        }
    }

    public void del(String dateStr) {
        String[] datesToDelete = dateStr.split(",");
        for (String date : datesToDelete) {
            String reqPattern = KeyUtil.genKey(RedisConstants.USER_BET_PERIOD_REQ_PREFIX, date, "*");
            String resPattern = KeyUtil.genKey(RedisConstants.USER_BET_PERIOD_RES_PREFIX, date, "*");

            redisson.getKeys().deleteByPattern(reqPattern);
            redisson.getKeys().deleteByPattern(resPattern);
        }
    }

    /**
     * 获取当日投注异常通知
     * @param username
     * @return
     */
    public JSONArray notices(String username) {
        // 获取 Redis 中现有的异常数据，如果存在
        RBucket<String> bucket = businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_BET_SKIP_PREFIX,
                ToDayRangeUtil.getToDayRange(),
                username));

        String existingErrorsStr = bucket.get(); // 获取 Redis 中的字符串数据

        // 如果没有数据，初始化为一个空的 JSON 数组
        JSONArray existingErrors = (existingErrorsStr == null) ? new JSONArray() : JSONUtil.parseArray(existingErrorsStr);

        if (!existingErrors.isEmpty()) {
            // 将 JSONArray 转为 List<JSONObject> 以便排序
            List<JSONObject> errorList = existingErrors.toList(JSONObject.class);

            // 按照 datetime 字段倒序排序
            errorList.sort(new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    // 获取 datetime 字段并进行比较，注意日期格式的解析
                    String datetime1 = o1.getStr("datetime");
                    String datetime2 = o2.getStr("datetime");

                    // 解析成 LocalDateTime 后进行比较，确保是倒序
                    LocalDateTime time1 = LocalDateTime.parse(datetime1, DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
                    LocalDateTime time2 = LocalDateTime.parse(datetime2, DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));

                    return time2.compareTo(time1);  // 倒序排列
                }
            });

            // 排序后再转回 JSONArray
            existingErrors = new JSONArray(errorList);
        }
        JSONArray result = new JSONArray();
        JSONObject jsonObject = new JSONObject();
        jsonObject.putOpt("key", "1");
        jsonObject.putOpt("name", "status.pureNotify");
        jsonObject.putOpt("emptyText", "status.pureNoNotify");
        jsonObject.putOpt("list", existingErrors);
        result.add(jsonObject);
        return result;
    }

    /**
     * 获取最新的异常跳过数据
     * @param username
     * @return
     */
    public JSONObject getMaxDatetimeNotice(String username) {
        // 获取 Redis 中现有的异常数据，如果存在
        RBucket<String> bucket = businessUserRedissonClient.getBucket(KeyUtil.genKey(RedisConstants.USER_BET_SKIP_PREFIX,
                ToDayRangeUtil.getToDayRange(),
                username));

        String existingErrorsStr = bucket.get(); // 获取 Redis 中的字符串数据

        // 如果没有数据，返回 null
        if (existingErrorsStr == null) {
            return null;
        }

        // 将 JSON 字符串转为 JSONArray
        JSONArray existingErrors = JSONUtil.parseArray(existingErrorsStr);

        if (existingErrors.isEmpty()) {
            return null;  // 如果数组为空，返回 null
        }

        // 找到最大 datetime 的 JSONObject
        // 转为 JSONObject
        // 获取 datetime 字段并进行比较，确保是倒序
        // 解析成 LocalDateTime 后进行比较
        // 正序排列：最大值为最新的时间
        // 如果没有元素，返回 null

        return existingErrors.stream()
                .map(obj -> (JSONObject) obj)  // 转为 JSONObject
                .max((o1, o2) -> {
                    // 获取 datetime 字段并进行比较，确保是倒序
                    String datetime1 = o1.getStr("datetime");
                    String datetime2 = o2.getStr("datetime");

                    // 解析成 LocalDateTime 后进行比较
                    LocalDateTime time1 = LocalDateTime.parse(datetime1, DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));
                    LocalDateTime time2 = LocalDateTime.parse(datetime2, DateTimeFormatter.ofPattern(DatePattern.NORM_DATETIME_PATTERN));

                    return time1.compareTo(time2);  // 正序排列：最大值为最新的时间
                })
                .orElse(null);
    }

    public void refreshRedis() {
        try {
            Process process = new ProcessBuilder("sudo", "systemctl", "stop", "redis").start();
            process.waitFor(); // 等待命令执行完成
            log.info("Redis服务已刷新");
        } catch (IOException | InterruptedException e) {
            log.error("刷新Redis服务失败", e);
        }
    }

}
