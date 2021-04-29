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

package com.dtstack.flinkx.connector.jdbc.table;

import com.dtstack.flinkx.conf.FieldConf;
import com.dtstack.flinkx.connector.jdbc.JdbcDialect;
import com.dtstack.flinkx.connector.jdbc.conf.JdbcConf;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.flinkx.connector.jdbc.sink.JdbcOutputFormatBuilder;
import com.dtstack.flinkx.enums.EWriteMode;
import com.dtstack.flinkx.streaming.api.functions.sink.DtOutputFormatSinkFunction;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * @author chuixue
 * @create 2021-04-12 14:44
 * @description
 **/
public class JdbcDynamicTableSink implements DynamicTableSink {

    private final JdbcConf jdbcConf;
    private final JdbcDialect jdbcDialect;
    private final TableSchema tableSchema;
    private final String dialectName;

    public JdbcDynamicTableSink(
            JdbcConf jdbcConf,
            JdbcDialect jdbcDialect,
            TableSchema tableSchema) {
        this.jdbcConf = jdbcConf;
        this.jdbcDialect = jdbcDialect;
        this.tableSchema = tableSchema;
        this.dialectName = jdbcDialect.dialectName();
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        validatePrimaryKey(requestedMode);
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    private void validatePrimaryKey(ChangelogMode requestedMode) {
        checkState(
                ChangelogMode.insertOnly().equals(requestedMode)
                        || !CollectionUtil.isNullOrEmpty(jdbcConf.getUpdateKey()),
                "please declare primary key for sink table when query contains update/delete record.");
    }

    @Override
    public SinkFunctionProvider getSinkRuntimeProvider(Context context) {
        final TypeInformation<RowData> rowDataTypeInformation =
                context.createTypeInformation(tableSchema.toRowDataType());

        // 通过该参数得到类型转换器，将数据库中的字段转成对应的类型
        final RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();

        JdbcOutputFormatBuilder builder = new JdbcOutputFormatBuilder(new JdbcOutputFormat());

        String[] fieldNames = tableSchema.getFieldNames();
        List<FieldConf> columnList = new ArrayList<>(fieldNames.length);
        for (String name : fieldNames) {
            FieldConf field = new FieldConf();
            field.setName(name);
            columnList.add(field);
        }
        jdbcConf.setColumn(columnList);
        jdbcConf.setMode((CollectionUtil.isNullOrEmpty(jdbcConf.getUpdateKey())) ? EWriteMode.INSERT
                .name() : EWriteMode.UPDATE.name());
        jdbcConf.setUpdateKey(jdbcConf.getUpdateKey());

        builder.setJdbcDialect(jdbcDialect);
        builder.setBatchSize(jdbcConf.getBatchSize());
        builder.setJdbcConf(jdbcConf);
        builder.setRowConverter(jdbcDialect.getRowConverter(rowType));
        builder.setFlushIntervalMillse(jdbcConf.getFlushIntervalMills());

        return SinkFunctionProvider.of(new DtOutputFormatSinkFunction(builder.finish()),
                jdbcConf.getParallelism());
    }

    @Override
    public DynamicTableSink copy() {
        return new JdbcDynamicTableSink(
                jdbcConf,
                jdbcDialect,
                tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "JDBC:" + dialectName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JdbcDynamicTableSink)) {
            return false;
        }
        JdbcDynamicTableSink that = (JdbcDynamicTableSink) o;
        return Objects.equals(jdbcConf, that.jdbcConf)
                && Objects.equals(tableSchema, that.tableSchema)
                && Objects.equals(dialectName, that.dialectName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jdbcConf, tableSchema, dialectName);
    }
}