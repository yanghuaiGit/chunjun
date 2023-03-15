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

package com.dtstack.chunjun.connector.httpbinance;

import com.dtstack.chunjun.connector.http.client.HttpClient;
import com.dtstack.chunjun.connector.http.inputformat.HttpInputFormat;

import com.dtstack.chunjun.connector.httpbinance.client.BinanceClient;

import com.dtstack.chunjun.connector.httpbinance.client.HttpBinanceConfig;

import org.apache.flink.core.io.InputSplit;

public class HttpbinanceInputFormat extends HttpInputFormat {

    @Override
    @SuppressWarnings("unchecked")
    protected void openInternal(InputSplit inputSplit) {
        myHttpClient =
                new BinanceClient((HttpBinanceConfig)httpRestConfig,  rowConverter);
        if (state != null) {
            myHttpClient.initPosition(state.getRequestParam(), state.getOriginResponseValue());
        }

        myHttpClient.start();
    }



}
