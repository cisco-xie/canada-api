package com.example.demo.common.enmu;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * 游戏类型定义枚举常量
 * 暂时只获取单号参数
 */
@Getter
@AllArgsConstructor
public enum GameType {
    ALL("all", "", "主势盘"),
    OPN("OPN", "", "单球1-3"),
    TPD("TPD", "", "二字定位"),
    DW3("DW3", "", "三字定位"),
    N5("n5", "", "两面盘");

    private final String lottery;
    private final String games;
    private final String description;

    // 使用静态 Map 来缓存 lottery 和对应的 GameType 实例
    private static final Map<String, GameType> LOTTERY_MAP = new HashMap<>();
    private static final Map<String, String> DESCRIPTION_MAP = new HashMap<>();

    // 静态代码块用于初始化映射
    static {
        for (GameType type : values()) {
            LOTTERY_MAP.put(type.getLottery(), type);
            DESCRIPTION_MAP.put(type.getDescription(), type.getGames());
        }
    }

    // 获取所有游戏类型的映射
    public static Map<String, String> getAllGameRulesWithDescriptions() {
        return new HashMap<>(DESCRIPTION_MAP); // 返回副本，避免外部修改
    }

    // 根据lottery获取枚举实例
    public static GameType getByLottery(String lottery) {
        return LOTTERY_MAP.get(lottery); // 快速查找
    }
}

