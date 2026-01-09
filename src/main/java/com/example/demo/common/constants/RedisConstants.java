package com.example.demo.common.constants;

public class RedisConstants {
    /**
     * 平台登录账号前缀
     */
    public static final String USER_ADMIN_PREFIX = "user:admin";
    /**
     * 平台账号小组前缀
     */
    public static final String USER_ADMIN_GROUP_PREFIX = "user:group";
    /**
     * 令牌前缀
     */
    public static final String USER_TOKEN_PREFIX = "user:token";

    /**
     * 代理前缀
     */
    public static final String USER_PROXY_PREFIX = "user:proxy";

    /**
     * 方案前缀
     */
    public static final String USER_PLAN_PREFIX = "user:plan";
    /**
     * 翻倍方案前缀
     */
    public static final String USER_PLAN_DOUBLE_PREFIX = "user:plan-double";
    /**
     * 翻倍方案监控期数前缀
     */
    public static final String USER_MONITOR_PREFIX = "user:monitor";
    /**
     * 方案id前缀
     */
    public static final String USER_PLAN_ID_PREFIX = "user:id:plan";
    /**
     * 翻倍方案id前缀
     */
    public static final String USER_PLAN_DOUBLE_ID_PREFIX = "user:id:plan-double";
    /**
     * 下注期数前缀 - 用于获取当前期数是否已下注
     */
    public static final String USER_BET_PERIOD_PREFIX = "user:bet:period";
    /**
     * 倍投下注期数前缀 - 用于获取当前期数是否已下注
     */
    public static final String USER_BET_DOUBLE_PERIOD_PREFIX = "user:bet-double:period";
    /**
     * 倍投下注订单信息前缀 - 用于获取翻老投新某期的订单信息
     */
    public static final String USER_BET_ORDER_DOUBLE_PREFIX = "user:order:bet-double";
    /**
     * 期数赔率前缀 - 用于获取当前期数的赔率情况
     */
    public static final String USER_BET_ODDS_PREFIX = "user:bet:odds";
    /**
     * 下注参数日志前缀 - 用于获取当前期数下注参数日志
     */
    public static final String USER_BET_PERIOD_REQ_PREFIX = "user:bet:req";
    /**
     * 翻倍下注参数日志前缀 - 用于获取当前期数下注参数日志
     */
    public static final String USER_BET_DOUBLE_PERIOD_REQ_PREFIX = "user:bet-double:req";
    /**
     * 下注结果日志前缀 - 用于获取当前期数下注结果日志
     */
    public static final String USER_BET_PERIOD_RES_PREFIX = "user:bet:res";
    /**
     * 翻倍下注结果日志前缀 - 用于获取当前期数下注结果日志
     */
    public static final String USER_BET_DOUBLE_PERIOD_RES_PREFIX = "user:bet-double:res";
    /**
     * 自动下注前缀 - 用于获取当前是否自动下注
     */
    public static final String USER_BET_AUTO_PREFIX = "user:bet:auto";
    /**
     * 网络异常跳过下注前缀 - 用于获取当前平台用户是否存在网络异常跳过下注的数据
     */
    public static final String USER_BET_SKIP_PREFIX = "user:bet:skip";
    /**
     * 最新期数前缀 - 用于获取当前游戏最新期数
     */
    public static final String PERIOD_PREFIX = "period";
    /**
     * 开奖结果信息 - 用于获取开奖结果信息前缀
     */
    public static final String DRAW_RESULT_PREFIX = "draw:result";
}
