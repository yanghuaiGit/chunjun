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

package com.dtstack.chunjun.connector.httpbinance.source;

import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.http.converter.HttpColumnConverter;
import com.dtstack.chunjun.connector.http.converter.HttpRowConverter;
import com.dtstack.chunjun.connector.http.source.HttpSourceFactory;
import com.dtstack.chunjun.connector.httpbinance.client.HttpBinanceConfig;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

public class HttpbinanceSourceFactory extends HttpSourceFactory {
    HttpBinanceConfig httpBinanceConfig;

    public HttpbinanceSourceFactory(SyncConf config, StreamExecutionEnvironment env) {
        super(config, env);
        httpBinanceConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(config.getReader().getParameter()),
                        HttpBinanceConfig.class);
    }

    @Override
    public DataStream<RowData> createSource() {
        HttpbinanceInputFormatBuilder builder = new HttpbinanceInputFormatBuilder();
        AbstractRowConverter rowConverter = null;
        if (useAbstractBaseColumn) {
            rowConverter = new HttpColumnConverter(httpRestConfig);
        } else {
            final RowType rowType =
                    TableUtil.createRowType(httpRestConfig.getColumn(), getRawTypeConverter());
            rowConverter = new HttpRowConverter(rowType, httpRestConfig);
        }
        builder.setRowConverter(rowConverter, useAbstractBaseColumn);
        builder.setHttpRestConfig(httpBinanceConfig);
        builder.setConfig(httpBinanceConfig);
        return createInput(builder.finish());
    }
}
