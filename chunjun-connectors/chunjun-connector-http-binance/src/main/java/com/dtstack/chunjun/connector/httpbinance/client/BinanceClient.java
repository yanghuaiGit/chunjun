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

package com.dtstack.chunjun.connector.httpbinance.client;

import com.dtstack.chunjun.connector.http.client.HttpClient;
import com.dtstack.chunjun.connector.http.client.HttpRequestParam;
import com.dtstack.chunjun.connector.http.client.ResponseValue;
import com.dtstack.chunjun.converter.AbstractRowConverter;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.element.column.StringColumn;
import com.dtstack.chunjun.util.ExceptionUtil;
import com.dtstack.chunjun.util.GsonUtil;

import com.binance.connector.client.impl.SpotClientImpl;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

public class BinanceClient extends HttpClient {
    private static final Logger LOG = LoggerFactory.getLogger(HttpClient.class);
    private String startTime;
    HttpBinanceConfig httpRestConfig;
    SpotClientImpl client;

    public BinanceClient(HttpBinanceConfig httpRestConfig, AbstractRowConverter converter) {
        super(
                httpRestConfig,
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                converter);
        this.startTime = httpRestConfig.getStartTime();
        client = new SpotClientImpl(httpRestConfig.getApiKey(), httpRestConfig.getSecretKey());
        this.httpRestConfig = httpRestConfig;
    }

    public void execute() {
        if (reachEnd) {
            return;
        }
        try {
            doExecute(3);
        } catch (Throwable e) {
            LOG.warn("请求错误 ", e);
            reachEnd = true;
            processData(new ResponseValue(-1, null, ExceptionUtil.getErrorMessage(e), null, null));
        }
    }

    public void doExecute(int retryTime) {
        if (retryTime == 0) {
            throw new RuntimeException("重试次数到了 任务结束");
        }
        LinkedHashMap<String, Object> parameters = new LinkedHashMap<>();

        // 每次循环 将最后一条数据的close时间作为下一次请求的start时间即可 直到数据返回为空即[]
        parameters.put("symbol", httpRestConfig.getSymbol());
        parameters.put("interval", httpRestConfig.getInterval());
        parameters.put("startTime", startTime);
        parameters.put("endTime", httpRestConfig.getEndTime());
        // Default 500; max 1000.
        parameters.put("limit", "1000");

        //        [
        //  [
        //        1499040000000,      // Kline open time
        //                "0.01634790",       // Open price
        //                "0.80000000",       // High price
        //                "0.01575800",       // Low price
        //                "0.01577100",       // Close price
        //                "148976.11427815",  // Volume
        //                1499644799999,      // Kline Close time
        //                "2434.19055334",    // Quote asset volume
        //                308,                // Number of trades
        //                "1756.87402397",    // Taker buy base asset volume
        //                "28.46694368",      // Taker buy quote asset volume
        //                "0"                 // Unused field, ignore.
        //  ]
        // ]

        List list;
        try {
            String result = client.createMarket().klines(parameters);
            list = GsonUtil.GSON.fromJson(result, List.class);
        } catch (Throwable e) {
            LOG.warn("请求错误 重试 retryTime{}", retryTime, e);
            doExecute(--retryTime);
            return;
        }
        // 每条数据进行处理

        String lastEndTime = startTime;
        if (CollectionUtils.isNotEmpty(list)) {
            for (Object o : list) {
                if (o instanceof List) {
                    ColumnRowData columnRowData = new ColumnRowData(restConfig.getColumn().size());
                    List<Object> data = (List<Object>) o;
                    for (int i = 0; i < restConfig.getColumn().size(); i++) {
                        if (restConfig.getColumn().get(i).getName().equals("symbol")) {
                            columnRowData.addField(new StringColumn(httpRestConfig.getSymbol()));
                        } else if (restConfig.getColumn().get(i).getName().equals("open")) {
                            columnRowData.addField(new StringColumn(data.get(0).toString()));
                        } else if (restConfig.getColumn().get(i).getName().equals("close")) {
                            columnRowData.addField(new StringColumn(data.get(6).toString()));
                        } else if (restConfig.getColumn().get(i).getName().equals("o")) {
                            columnRowData.addField(new StringColumn(data.get(1).toString()));
                        } else if (restConfig.getColumn().get(i).getName().equals("h")) {
                            columnRowData.addField(new StringColumn(data.get(2).toString()));
                        } else if (restConfig.getColumn().get(i).getName().equals("l")) {
                            columnRowData.addField(new StringColumn(data.get(3).toString()));
                        } else if (restConfig.getColumn().get(i).getName().equals("c")) {
                            columnRowData.addField(new StringColumn(data.get(4).toString()));
                        } else if (restConfig.getColumn().get(i).getName().equals("interval")) {
                            columnRowData.addField(new StringColumn(httpRestConfig.getInterval()));
                        } else if (restConfig.getColumn().get(i).getName().equals("volume")) {
                            columnRowData.addField(new StringColumn(data.get(5).toString()));
                        }
                    }
                    lastEndTime = data.get(6).toString();
                    processData(
                            new ResponseValue(
                                    columnRowData, HttpRequestParam.httpRequestParam, null));
                } else {
                    throw new RuntimeException("返回值不是数组格式");
                }
            }
            startTime = lastEndTime;
        }

        if (CollectionUtils.isEmpty(list) || list.size() < 1000) {
            processData(new ResponseValue(2, null, null, HttpRequestParam.httpRequestParam, null));
            reachEnd = true;
        }
    }

    public void close() {
        super.close();
    }
}
