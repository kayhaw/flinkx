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
package com.dtstack.flinkx;

import com.dtstack.flinkx.conf.SpeedConf;
import com.dtstack.flinkx.conf.SyncConf;
import com.dtstack.flinkx.constants.ConstantValue;
import com.dtstack.flinkx.enums.EJobType;
import com.dtstack.flinkx.environment.EnvFactory;
import com.dtstack.flinkx.environment.MyLocalStreamEnvironment;
import com.dtstack.flinkx.options.OptionParser;
import com.dtstack.flinkx.options.Options;
import com.dtstack.flinkx.sink.SinkFactory;
import com.dtstack.flinkx.source.SourceFactory;
import com.dtstack.flinkx.sql.parser.SqlParser;
import com.dtstack.flinkx.throwable.FlinkxRuntimeException;
import com.dtstack.flinkx.util.DataSyncFactoryUtil;
import com.dtstack.flinkx.util.ExecuteProcessHelper;
import com.dtstack.flinkx.util.FactoryHelper;
import com.dtstack.flinkx.util.PluginUtil;
import com.dtstack.flinkx.util.PrintUtil;
import com.dtstack.flinkx.util.PropertiesUtil;
import com.dtstack.flinkx.util.TableUtil;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ExpressionParser;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.types.DataType;

import com.google.common.base.Preconditions;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * The main class entry
 *
 * <p>Company: www.dtstack.com
 *
 * @author huyifan.zju@163.com
 */
public class Main {

    public static Logger LOG = LoggerFactory.getLogger(Main.class);

    /** Main类的main方法是整个flinkx作为flink流应用的唯一入口类 */
    public static void main(String[] args) throws Exception {
        LOG.info("------------program params-------------------------");
        Arrays.stream(args).forEach(arg -> LOG.info("{}", arg));
        LOG.info("-------------------------------------------");

        // 又把string字符串转为Options对象
        Options options = new OptionParser(args).getOptions();
        // 将json字符串重新编码为UTF-8，后面gson解析需要
        String job = URLDecoder.decode(options.getJob(), Charsets.UTF_8.name());
        // 环境变量，只用于table执行环境
        Properties confProperties = PropertiesUtil.parseConf(options.getConfProp());
        // env用于执行普通同步任务
        StreamExecutionEnvironment env = EnvFactory.createStreamExecutionEnvironment(options);
        // tenv用于执行flink sql同步任务，依赖于env
        StreamTableEnvironment tEnv =
                EnvFactory.createStreamTableEnvironment(env, confProperties, options.getJobName());

        switch (EJobType.getByName(options.getJobType())) {
            case SQL:
                exeSqlJob(env, tEnv, job, options);
                break;
                // 其实我觉得这里可以改名叫JSON
            case SYNC:
                // 开始正式执行
                exeSyncJob(env, tEnv, job, options);
                break;
            default:
                throw new FlinkxRuntimeException(
                        "unknown jobType: ["
                                + options.getJobType()
                                + "], jobType must in [SQL, SYNC].");
        }

        LOG.info("program {} execution success", options.getJobName());
    }

    /**
     * 执行sql 类型任务
     *
     * @param env
     * @param tableEnv
     * @param job
     * @param options
     * @throws Exception
     */
    private static void exeSqlJob(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            String job,
            Options options) {
        try {
            configStreamExecutionEnvironment(env, options, null);
            List<URL> jarUrlList = ExecuteProcessHelper.getExternalJarUrls(options.getAddjar());
            StatementSet statementSet = SqlParser.parseSql(job, jarUrlList, tableEnv);
            TableResult execute = statementSet.execute();
            if (env instanceof MyLocalStreamEnvironment) {
                Optional<JobClient> jobClient = execute.getJobClient();
                if (jobClient.isPresent()) {
                    PrintUtil.printResult(jobClient.get().getAccumulators().get());
                }
            }
        } catch (Exception e) {
            throw new FlinkxRuntimeException(e);
        } finally {
            FactoryUtil.getFactoryHelperThreadLocal().remove();
            TableFactoryService.getFactoryHelperThreadLocal().remove();
        }
    }

    /**
     * 执行 数据同步类型任务
     *
     * @param env
     * @param tableEnv
     * @param job
     * @param options
     * @throws Exception
     */
    private static void exeSyncJob(
            StreamExecutionEnvironment env,
            StreamTableEnvironment tableEnv,
            String job,
            Options options)
            throws Exception {

        // job字符串是任务配置json，options是应用运行参数，把两者做个整合成配置类SyncConf
        SyncConf config = parseFlinkxConf(job, options);
        // 非常关键的一步，TOP 1！！！会加载插件jar包到env中
        configStreamExecutionEnvironment(env, options, config);

        // sourceFactory，很关键！！！
        SourceFactory sourceFactory = DataSyncFactoryUtil.discoverSource(config, env);
        // 在createSource方法中会调用env的addSource方法，关键！！！
        DataStream<RowData> dataStreamSource = sourceFactory.createSource();

        SpeedConf speed = config.getSpeed();
        // 为source算子设置并行度
        if (speed.getReaderChannel() > 0) {
            dataStreamSource =
                    ((DataStreamSource<RowData>) dataStreamSource)
                            .setParallelism(speed.getReaderChannel());
        }

        DataStream<RowData> dataStream;
        boolean transformer =
                config.getTransformer() != null
                        && StringUtils.isNotBlank(config.getTransformer().getTransformSql());

        // 如果开启transformer，将数据流转为表后使用用户自定义sql进行转换操作
        if (transformer) {
            dataStream = syncStreamToTable(tableEnv, config, dataStreamSource);
        } else {
            dataStream = dataStreamSource;
        }

        if (speed.isRebalance()) {
            // 数据流均匀发送到下一个算子
            dataStream = dataStream.rebalance();
        }

        // 步骤同sourceFactory
        SinkFactory sinkFactory = DataSyncFactoryUtil.discoverSink(config);
        // 层层调用，最后调用DataStream.addSink方法
        DataStreamSink<RowData> dataStreamSink = sinkFactory.createSink(dataStream);
        if (speed.getWriterChannel() > 0) {
            dataStreamSink.setParallelism(speed.getWriterChannel());
        }

        // 执行作业，作业名来自命令行参数
        JobExecutionResult result = env.execute(options.getJobName());
        // 如果是本地执行还打印下统计结果
        if (env instanceof MyLocalStreamEnvironment) {
            PrintUtil.printResult(result.getAllAccumulatorResults());
        }
    }

    /**
     * 将数据同步Stream 注册成table
     *
     * @param tableEnv
     * @param config
     * @param sourceDataStream
     * @return
     */
    private static DataStream<RowData> syncStreamToTable(
            StreamTableEnvironment tableEnv,
            SyncConf config,
            DataStream<RowData> sourceDataStream) {
        // transform sql的字段来自json中fieldNameList配置项
        String fieldNames =
                String.join(ConstantValue.COMMA_SYMBOL, config.getReader().getFieldNameList());
        List<Expression> expressionList = ExpressionParser.parseExpressionList(fieldNames);
        // 相当于执行select语句过滤字段
        Table sourceTable =
                tableEnv.fromDataStream(
                        sourceDataStream, expressionList.toArray(new Expression[0]));
        // 加上表名
        tableEnv.createTemporaryView(config.getReader().getTable().getTableName(), sourceTable);
        // 获取transform sql然后执行得到结果表adaptTable
        String transformSql = config.getJob().getTransformer().getTransformSql();
        Table adaptTable = tableEnv.sqlQuery(transformSql);

        DataType[] tableDataTypes = adaptTable.getSchema().getFieldDataTypes();
        String[] tableFieldNames = adaptTable.getSchema().getFieldNames();
        TypeInformation<RowData> typeInformation =
                TableUtil.getTypeInformation(tableDataTypes, tableFieldNames);
        // 获取结果表的数据流并返回
        DataStream<RowData> dataStream =
                tableEnv.toRetractStream(adaptTable, typeInformation).map(f -> f.f1);
        // 注意这里把表名改成写插件中设置的
        tableEnv.createTemporaryView(config.getWriter().getTable().getTableName(), dataStream);

        /**
         * 总结一下这里Table API转换流程： 1. tableEnv.fromDataStream(): 数据流转原生数据表 2.
         * tableEnv.createTemporaryView(表名，table) + sqlQuery：给原生数据表加上表名后执行sql得到转换数据表 3.
         * tableEnv.toRetractStream(): 获取转换后的数据流 4. 再次给转换后数据流设置表名 5. 最后返回转换后数据流
         */
        return dataStream;
    }

    /**
     * 解析并设置job
     *
     * @param job
     * @param options
     * @return
     */
    public static SyncConf parseFlinkxConf(String job, Options options) {
        SyncConf config;
        try {
            // parseJob实际调用GSON的解析方法，顺便还做了校验
            config = SyncConf.parseJob(job);

            // 手动补充可选参数，flinkxDistDir是本地存放flinkx connector插件jar的目录路径
            if (StringUtils.isNotBlank(options.getFlinkxDistDir())) {
                config.setPluginRoot(options.getFlinkxDistDir());
            }

            // remoteFlinkxDistDir是远程的目录路径
            if (StringUtils.isNotBlank(options.getRemoteFlinkxDistDir())) {
                config.setRemotePluginPath(options.getRemoteFlinkxDistDir());
            }
        } catch (Exception e) {
            throw new FlinkxRuntimeException(e);
        }
        return config;
    }

    /**
     * 配置StreamExecutionEnvironment
     *
     * @param env StreamExecutionEnvironment
     * @param options options
     * @param config FlinkxConf
     */
    private static void configStreamExecutionEnvironment(
            StreamExecutionEnvironment env, Options options, SyncConf config) {
        if (config != null) {
            PluginUtil.registerPluginUrlToCachedFile(config, env);
            // 设置环境默认并行度
            env.setParallelism(config.getSpeed().getChannel());
        } else {
            Preconditions.checkArgument(
                    ExecuteProcessHelper.checkRemoteSqlPluginPath(
                            options.getRemoteFlinkxDistDir(),
                            options.getMode(),
                            options.getPluginLoadMode()),
                    "Non-local mode or shipfile deployment mode, remoteSqlPluginPath is required");
            FactoryHelper factoryHelper = new FactoryHelper();
            factoryHelper.setLocalPluginPath(options.getFlinkxDistDir());
            factoryHelper.setRemotePluginPath(options.getRemoteFlinkxDistDir());
            factoryHelper.setPluginLoadMode(options.getPluginLoadMode());
            factoryHelper.setEnv(env);

            FactoryUtil.setFactoryUtilHelp(factoryHelper);
            TableFactoryService.setFactoryUtilHelp(factoryHelper);
        }
        // env加载shipfile，和registerPluginUrlToCachedFile中加载的文件不同，它是不可执行的
        PluginUtil.registerShipfileToCachedFile(options.getAddShipfile(), env);
    }
}
