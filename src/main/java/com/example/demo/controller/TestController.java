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
import com.example.demo.core.result.Result;
import com.example.demo.core.support.BaseController;
import com.example.demo.model.dto.AdminLoginDTO;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;


@Slf4j
@Tag(name = "法拉利项目")
@RequestMapping("/api/test")
@RestController
public class TestController extends BaseController {

    @Resource
    private HandicapApi api;

    @Resource
    private ConfigService configService;

    @Resource
    private PlanDoubleService planDoubleService;

    @Operation(summary = "")
    @GetMapping("/bet")
    public Result bet() {
        planDoubleService.autoBetCompletableFuturePlanDouble();
        return Result.success();
    }

    @Operation(summary = "")
    @GetMapping("/dresult")
    public Result dresult(@RequestParam(value = "accountId", required = false) String accountId, @RequestParam(value = "lottery", required = false) String lottery, @RequestParam(value = "drawNumber", required = false) String drawNumber) {
        AdminLoginDTO admin = getUser();
        return Result.success(api.dresult(admin.getUsername(), accountId, lottery, drawNumber));
    }

    @Operation(summary = "")
    @GetMapping("/dresults")
    public Result dresults(@RequestParam(value = "accountId", required = false) String accountId, @RequestParam(value = "lottery", required = false) String lottery) {
        AdminLoginDTO admin = getUser();
        return Result.success(api.dresults(admin.getUsername(), accountId, lottery));
    }
}
