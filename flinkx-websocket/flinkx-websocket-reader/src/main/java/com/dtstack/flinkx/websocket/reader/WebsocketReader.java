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

package com.dtstack.flinkx.websocket.reader;

import com.dtstack.flinkx.config.DataTransferConfig;
import com.dtstack.flinkx.config.ReaderConfig;
import com.dtstack.flinkx.reader.BaseDataReader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import static com.dtstack.flinkx.websocket.constants.WebSocketConfig.KEY_RETRY_INTERVAL;
import static com.dtstack.flinkx.websocket.constants.WebSocketConfig.KEY_RETRY_TIME;
import static com.dtstack.flinkx.websocket.constants.WebSocketConfig.KEY_WEB_SOCKET_SERVER_URL;
import static com.dtstack.flinkx.websocket.constants.WebSocketConfig.DEFAULT_RETRY_INTERVAL;
import static com.dtstack.flinkx.websocket.constants.WebSocketConfig.DEFAULT_RETRY_TIME;


/** 从入参中获取配置信息
 * @Company: www.dtstack.com
 * @author kunni@dtstack.com
 */

public class WebsocketReader extends BaseDataReader {

    protected String serverUrl;
    protected int retryTime;
    protected int interval;

    public WebsocketReader(DataTransferConfig config, StreamExecutionEnvironment env) {
        super(config, env);
        ReaderConfig readerConfig = config.getJob().getContent().get(0).getReader();
        serverUrl = readerConfig.getParameter().getStringVal(KEY_WEB_SOCKET_SERVER_URL);
        retryTime = readerConfig.getParameter().getIntVal(KEY_RETRY_TIME, DEFAULT_RETRY_TIME);
        interval = readerConfig.getParameter().getIntVal(KEY_RETRY_INTERVAL, DEFAULT_RETRY_INTERVAL);
    }

    @Override
    public DataStream<Row> readData() {
        WebSocketInputFormatBuilder builder = new WebSocketInputFormatBuilder();
        builder.setServerUrl(serverUrl);
        builder.setRetryTime(retryTime);
        builder.setRetryInterval(interval);
        builder.setDataTransferConfig(dataTransferConfig);
        return createInput(builder.finish());
    }
}
