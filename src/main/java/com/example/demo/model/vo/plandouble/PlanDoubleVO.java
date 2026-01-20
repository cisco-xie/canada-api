package com.example.demo.model.vo.plandouble;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.List;

@Data
public class PlanDoubleVO {
    @Schema(description = "新增不传，更新传入")
    private Integer id;
    @NotBlank(message = "游戏类型不能为空")
    @Schema(description = "游戏类型")
    private String lottery;
    @Schema(description = "方案类型（1号码投注2位置开号投注3灵活开某投某5号码追踪17冷热号投注25开某投某）")
    private Integer planType;
    @Schema(description = "翻倍类型（1默认翻倍老号 2翻倍新号 3翻老投新）")
    private Integer doubleType = 1;
    @Schema(description = "结算类型 1不中翻倍 2中翻倍")
    private Integer settleType;
    @NotBlank(message = "方案名不能为空")
    @Schema(description = "方案名")
    private String name;
    // 是否启用 0否1是
    @NotNull(message = "是否启用不能为空")
    @Schema(description = "是否启用（0否1是）")
    private Integer enable;
    @Schema(description = "中号数量")
    private Integer winNum;
    @Schema(description = "监控期数")
    private Integer monitorNum;
    @Schema(description = "反投类型 1竖式 2横式")
    private String reverseType = "1";
    @Schema(description = "投注数组")
    private List<PositionVO> positions;
    @Schema(description = "位置数组-用于计划025")
    private List<Integer> locations;
    @Schema(description = "双面数组，大小")
    private List<Integer> twoSidedDxPositions;
    @Schema(description = "双面数组，单双")
    private List<Integer> twoSidedDsPositions;
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

    @NotNull(message = "正投账号数量不能为空")
    @Schema(description = "正投账号数量")
    private Integer positiveAccountNum;
    // 正投金额
    @NotBlank(message = "正投金额不能为空")
    @Schema(description = "正投金额")
    private String positiveAmount;
}
