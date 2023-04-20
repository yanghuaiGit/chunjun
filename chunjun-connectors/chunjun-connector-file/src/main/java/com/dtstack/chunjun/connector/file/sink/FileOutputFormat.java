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

import com.dtstack.chunjun.connector.file.config.FileConfig;
import com.dtstack.chunjun.sink.format.BaseFileOutputFormat;
import com.dtstack.chunjun.throwable.ChunJunRuntimeException;
import com.dtstack.chunjun.throwable.WriteRecordException;

import org.apache.flink.table.data.RowData;

import org.apache.commons.io.FileUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class FileOutputFormat extends BaseFileOutputFormat {
    private FileConfig fileConfig;
    long currentFileLength = 0L;
    BufferedWriter bw;

    @Override
    protected void checkOutputDir() {
        File file = new File(tmpPath);
        if (!file.exists()) {
            file.mkdirs();
        }
    }

    @Override
    protected void deleteDataDir() {
        File file = new File(outputFilePath);
        if (file.isFile()) {
            file.deleteOnExit();
        } else {
            File[] files = file.listFiles();
            if (files != null) {
                for (File listFile : files) {
                    listFile.deleteOnExit();
                }
            }
        }
        File file1 = new File(tmpPath);
        if (file1.isFile()) {
            file1.deleteOnExit();
        } else {
            File[] files = file1.listFiles();
            if (files != null) {
                for (File listFile : files) {
                    listFile.deleteOnExit();
                }
            }
        }
    }

    @Override
    protected void deleteTmpDataDir() {
        File file = new File(tmpPath);
        if (file.isFile()) {
            file.deleteOnExit();
        } else {
            File[] files = file.listFiles();
            if (files != null) {
                for (File listFile : files) {
                    listFile.deleteOnExit();
                }
            }
        }
    }

    @Override
    protected void openSource() {}

    @Override
    protected String getExtension() {
        return ".txt";
    }

    @Override
    protected long getCurrentFileSize() {
        return currentFileLength;
    }

    @Override
    protected void writeSingleRecordToFile(RowData rowData) throws WriteRecordException {
        if (bw == null) {
            nextBlock();

            String currentBlockTmpPath = tmpPath + File.separator + currentFileName;
            FileWriter fileWriter = null;
            try {
                fileWriter = new FileWriter(currentBlockTmpPath);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            bw = new BufferedWriter(fileWriter);
            currentFileIndex++;
            LOG.info("nextBlock:Current block writer record:" + rowsOfCurrentBlock);
            LOG.info("Current block file name:" + currentBlockTmpPath);
        }
        String data = "";
        try {
            data = (String) rowConverter.toExternal(rowData, data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            bw.write(data);
            bw.write("\r\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void flushDataInternal() {
        if (bw != null) {
            try {
                bw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            bw = null;
        }
    }

    @Override
    protected List<String> copyTmpDataFileToDir() {
        String filePrefix = jobId + "_" + taskNumber;

        String currentFilePath = "";
        List<String> copyList = new ArrayList<>();
        try {
            for (File file :
                    Objects.requireNonNull(
                            new File(tmpPath)
                                    .listFiles(
                                            new FileFilter() {
                                                @Override
                                                public boolean accept(File pathname) {
                                                    return pathname.getName()
                                                            .startsWith(filePrefix);
                                                }
                                            }))) {
                currentFilePath = file.getName();
                FileUtils.copyFile(
                        file, new File(outputFilePath + File.separator + file.getName()));
                copyList.add(outputFilePath + File.separator + file.getName());
            }

        } catch (Exception e) {
            throw new ChunJunRuntimeException(
                    String.format(
                            "can't copy temp file:[%s] to dir:[%s]",
                            currentFilePath, outputFilePath),
                    e);
        }
        return copyList;
    }

    @Override
    protected void deleteDataFiles(List<String> preCommitFilePathList, String path) {
        String currentFilePath = "";
        for (String fileName : this.preCommitFilePathList) {
            currentFilePath = path + File.separator + fileName;
            new File(currentFilePath).deleteOnExit();
            LOG.info("delete file:{}", currentFilePath);
        }
    }

    @Override
    protected void moveAllTmpDataFileToDir() {
        File tmpDir = new File(tmpPath);
        if (tmpDir.isDirectory()) {
            for (File file : tmpDir.listFiles()) {
                file.renameTo(new File(outputFilePath + File.separator + file.getName()));
            }
        }
    }

    @Override
    protected void closeSource() {
        if (bw != null) {
            try {
                bw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            bw = null;
        }
    }

    @Override
    public float getDeviation() {
        return 1;
    }

    public void setFileConfig(FileConfig fileConfig) {
        this.fileConfig = fileConfig;
    }
}
