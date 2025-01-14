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

import com.dtstack.chunjun.connector.httpbinance.HttpbinanceInputFormat;
import com.dtstack.chunjun.connector.httpbinance.client.HttpBinanceConfig;
import com.dtstack.chunjun.source.format.BaseRichInputFormatBuilder;

public class HttpbinanceInputFormatBuilder
        extends BaseRichInputFormatBuilder<HttpbinanceInputFormat> {
    public HttpbinanceInputFormatBuilder() {
        super(new HttpbinanceInputFormat());
    }

    public void setHttpRestConfig(HttpBinanceConfig httpRestConfig) {
        this.format.setHttpRestConfig(httpRestConfig);
    }

    @Override
    protected void checkFormat() {}
}
