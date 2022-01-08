/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flinkx.environment;

import com.dtstack.flinkx.enums.ClusterMode;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.util.PropertiesUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;

import org.apache.commons.lang3.StringUtils;

import java.util.Properties;

/**
 * @program: flinkx
 * @author: wuren
 * @create: 2021/08/04
 */
public class EnvFactory {

    /**
     * 创建StreamExecutionEnvironment
     *
     * @param options
     * @return
     */
    public static StreamExecutionEnvironment createStreamExecutionEnvironment(Options options) {
        Configuration flinkConf = new Configuration();
        if (StringUtils.isNotEmpty(options.getFlinkConfDir())) {
            // 给出flink conf文件夹路径并加载，loadConfiguration重载版本还可以再加一个Configuration对象来补充配置
            flinkConf = GlobalConfiguration.loadConfiguration(options.getFlinkConfDir());
        }
        StreamExecutionEnvironment env;
        if (StringUtils.equalsIgnoreCase(ClusterMode.local.name(), options.getMode())) {
            // local模式下，运行参数-flinkConfDir=path_to_flink_conf给出本地flink配置目录的路径
            // 进而配置flink执行环境
            env = new MyLocalStreamEnvironment(flinkConf);
        } else {
            // 其他运行模式下，运行参数-confProp=<json字符串>给出来配置flink执行环境
            Configuration cfg =
                    Configuration.fromMap(PropertiesUtil.confToMap(options.getConfProp()));
            env = StreamExecutionEnvironment.getExecutionEnvironment(cfg);
        }
        // 关闭闭包清理，user code已经在每个worker节点下？
        env.getConfig().disableClosureCleaner();
        return env;
    }

    public static StreamTableEnvironment createStreamTableEnvironment(
            StreamExecutionEnvironment env, Properties properties, String jobName) {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Configuration configuration = tEnv.getConfig().getConfiguration();
        // Iceberg need this config setting up true.
        configuration.setBoolean(
                TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED.key(), true);
        setTableConfig(tEnv, properties, jobName);
        return tEnv;
    }

    private static void setTableConfig(
            StreamTableEnvironment tEnv, Properties properties, String jobName) {
        Configuration configuration = tEnv.getConfig().getConfiguration();
        properties.entrySet().stream()
                .filter(e -> e.getKey().toString().toLowerCase().startsWith("table."))
                .forEach(
                        e ->
                                configuration.setString(
                                        e.getKey().toString().toLowerCase(),
                                        e.getValue().toString().toLowerCase()));

        configuration.setString(PipelineOptions.NAME, jobName);
    }
}
