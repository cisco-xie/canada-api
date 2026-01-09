package com.example.demo.model.vo;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.List;

@Data
public class ConfigPlanVO {
    @Schema(description = "新增不传，更新传入")
    private Integer id;
    @NotBlank(message = "游戏类型不能为空")
    @Schema(description = "游戏类型")
    private String lottery;
    @Schema(description = "方案类型（1自定义2随机）")
    private Integer planType;
    @NotBlank(message = "方案名不能为空")
    @Schema(description = "方案名")
    private String name;
    // 是否启用 0否1是
    @NotNull(message = "是否启用不能为空")
    @Schema(description = "是否启用（0否1是）")
    private Integer enable;
    // 位置数组，1到10
    @Size(max = 10, message = "位置数组最多只能包含10个位置")
    @Schema(description = "位置数组，1到10")
    private List<Integer> positions;
    @Size(max = 10, message = "大小最多只能包含10个位置")
    @Schema(description = "双面数组，大小")
    private List<Integer> twoSidedDxPositions;
    @Schema(description = "双面数组，单双")
    @Size(max = 10, message = "单双最多只能包含10个位置")
    private List<Integer> twoSidedDsPositions;
    @Size(max = 5, message = "龙虎最多只能包含5个位置")
    @Schema(description = "双面数组，龙虎")
    private List<Integer> twoSidedLhPositions;

    @Schema(description = "号码位置数量")
    private Integer positionsNum;
    @Schema(description = "大小位置数量")
    private Integer twoSidedDxNum;
    @Schema(description = "单双位置数量")
    private Integer twoSidedDsNum;
    @Schema(description = "龙虎位置数量")
    private Integer twoSidedLhNum;

    // 正投数量
    @NotNull(message = "正投数量不能为空")
    @Schema(description = "正投数量")
    private Integer positiveNum;
    @NotNull(message = "正投账号数量不能为空")
    @Schema(description = "正投账号数量")
    private Integer positiveAccountNum;
    // 反投数量
    @NotNull(message = "反投数量不能为空")
    @Schema(description = "反投数量")
    private Integer reverseNum;
    @NotNull(message = "反投账号数量不能为空")
    @Schema(description = "反投账号数量")
    private Integer reverseAccountNum;
    // 正投金额
    @NotNull(message = "正投金额不能为空")
    @Schema(description = "正投金额")
    private Integer positiveAmount;
    // 反投金额
    @NotNull(message = "反投金额不能为空")
    @Schema(description = "反投金额")
    private Integer reverseAmount;
}
