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

package com.dtstack.chunjun.connector.file.sink;

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.file.config.FileConfig;
import com.dtstack.chunjun.connector.file.converter.FtpColumnConverter;
import com.dtstack.chunjun.connector.file.converter.FtpRawTypeConverter;
import com.dtstack.chunjun.converter.RawTypeConverter;
import com.dtstack.chunjun.sink.SinkFactory;
import com.dtstack.chunjun.util.JsonUtil;
import com.dtstack.chunjun.util.TableUtil;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @program: flinkx
 * @author: xiuzhu
 * @create: 2021/06/19
 */
public class FileSinkFactory extends SinkFactory {

    private List<String> columnName;
    private List<String> columnType;
    private FileConfig fileConfig;

    public FileSinkFactory(SyncConf syncConf) {
        super(syncConf);
        fileConfig =
                JsonUtil.toObject(
                        JsonUtil.toJson(syncConf.getWriter().getParameter()), FileConfig.class);

        super.initCommonConf(fileConfig);
    }

    @Override
    public DataStreamSink<RowData> createSink(DataStream<RowData> dataSet) {
        FtpOutputFormatBuilder builder = new FtpOutputFormatBuilder(new FileOutputFormat());
        builder.setConfig(fileConfig);
        builder.setFtpConfig(fileConfig);
        builder.setBaseFileConf(fileConfig);
        List<FieldConf> fieldConfList =
                fileConfig.getColumn().stream()
                        .peek(
                                fieldConf -> {
                                    if (fieldConf.getName() == null) {
                                        fieldConf.setName(String.valueOf(fieldConf.getIndex()));
                                    }
                                })
                        .collect(Collectors.toList());
        fileConfig.setColumn(fieldConfList);
        final RowType rowType =
                TableUtil.createRowType(fileConfig.getColumn(), getRawTypeConverter());
        builder.setRowConverter(new FtpColumnConverter(rowType, fileConfig), useAbstractBaseColumn);

        return createOutput(dataSet, builder.finish());
    }

    @Override
    public RawTypeConverter getRawTypeConverter() {
        return FtpRawTypeConverter::apply;
    }
}
