/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.demo.controller;

import com.example.demo.api.ConfigService;
import com.example.demo.api.HandicapApi;
import com.example.demo.api.PlanDoubleService;
import com.example.demo.common.enmu.SystemError;
import com.example.demo.core.exception.BusinessException;
import com.example.demo.core.result.Result;
import com.example.demo.core.support.BaseController;
import com.example.demo.model.dto.AdminLoginDTO;
import com.example.demo.model.vo.plandouble.PlanDoubleVO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.List;


@Slf4j
@Tag(name = "法拉利项目")
@RequestMapping("/api")
@RestController
public class PlanDoubleController extends BaseController {

    @Resource
    private HandicapApi api;

    @Resource
    private ConfigService configService;

    @Resource
    private PlanDoubleService planDoubleService;

    @Operation(summary = "一键启用翻倍方案配置")
    @GetMapping("/config/plan-double/enable")
    public Result configPlanEnable() {
        AdminLoginDTO admin = getUser();
        configService.planDouble(admin.getUsername(), 1);
        return Result.success();
    }

    @Operation(summary = "一键停用翻倍方案配置")
    @GetMapping("/config/plan-double/unenable")
    public Result configPlanUnenable() {
        AdminLoginDTO admin = getUser();
        configService.planDouble(admin.getUsername(), 0);
        return Result.success();
    }

    @Operation(summary = "翻倍方案配置")
    @PostMapping("/config/plan-double")
    @ResponseBody
    public Result configPlan(@RequestBody @Validated PlanDoubleVO plan) {
        AdminLoginDTO admin = getUser();
        List<Integer> positiveAmounts = formatIds(plan.getPositiveAmount());
        List<Integer> reverseAmounts = formatIds(plan.getReverseAmount());
        if (plan.getMonitorNum() != positiveAmounts.size() || plan.getMonitorNum() != reverseAmounts.size()) {
            throw new BusinessException(SystemError.PLAN_1200);
        }
        configService.planDouble(admin.getUsername(), plan);
        return Result.success();
    }

    @Operation(summary = "删除翻倍方案配置")
    @DeleteMapping("/config/plan-double")
    @ResponseBody
    public Result planDel(@RequestBody List<PlanDoubleVO> plans) {
        AdminLoginDTO admin = getUser();
        configService.planDoubleDel(admin.getUsername(), plans);
        return Result.success();
    }

    @Operation(summary = "获取翻倍方案配置")
    @GetMapping("/config/plan-double")
    public Result configPlan(@RequestParam(value = "lottery", required = false) String lottery) {
        AdminLoginDTO admin = getUser();
        return Result.success(configService.getAllPlanDoublesRBatch(admin.getUsername(), lottery));
    }

}
