/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.chunjun.connector.jdbc.sink;

import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.config.SinkConnectionConfig;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.UUID;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class JdbcWriteKeyValidationTest {
    @Test
    public void updateWithoutAnyKeyFailsBeforePreparingStatements() throws Exception {
        verifyMissingKey("UPDATE");
    }

    @Test
    public void upsertWithoutAnyKeyFailsBeforePreparingStatements() throws Exception {
        verifyMissingKey("UPSERT");
    }

    private void verifyMissingKey(String mode) throws Exception {
        try (Fixture fixture = new Fixture(mode, "UNKEYED")) {
            IllegalArgumentException error =
                    Assert.assertThrows(IllegalArgumentException.class, fixture.format::initialize);
            Assert.assertTrue(error.getMessage(), error.getMessage().contains("uniqueKey"));
            Assert.assertTrue(error.getMessage(), error.getMessage().contains(mode));
            Assert.assertFalse(fixture.format.prepared);
            Assert.assertTrue(
                    "Rejected writers must release their connection",
                    fixture.format.connection.isClosed());
        }
    }

    @Test
    public void insertAndReplaceRemainValidWithoutKeys() throws Exception {
        for (String mode : new String[] {"INSERT", "REPLACE"}) {
            try (Fixture fixture = new Fixture(mode, "UNKEYED")) {
                fixture.format.initialize();
                Assert.assertTrue(fixture.format.prepared);
            }
        }
    }

    @Test
    public void explicitKeyIsAccepted() throws Exception {
        try (Fixture fixture = new Fixture("UPDATE", "UNKEYED")) {
            fixture.format.jdbcConfig.setUniqueKey(Collections.singletonList("ID"));
            fixture.format.initialize();
            Assert.assertTrue(fixture.format.prepared);
            verify(fixture.format.jdbcDialect, never()).getTableIdentify("PUBLIC", "UNKEYED");
        }
    }

    @Test
    public void primaryKeyAndUniqueIndexAreInferred() throws Exception {
        for (String table : new String[] {"WITH_PRIMARY_KEY", "WITH_UNIQUE_INDEX"}) {
            try (Fixture fixture = new Fixture("UPDATE", table)) {
                fixture.format.initialize();
                Assert.assertTrue(fixture.format.prepared);
                Assert.assertEquals(
                        Collections.singletonList("ID"), fixture.format.jdbcConfig.getUniqueKey());
            }
        }
    }

    @Test
    public void dynamicTablesDoNotUseStaticMetadataLookup() throws Exception {
        try (Fixture fixture = new Fixture("UPDATE", "*")) {
            fixture.format.initialize();
            Assert.assertTrue(fixture.format.prepared);
            verify(fixture.format.jdbcDialect, never()).getTableIdentify("PUBLIC", "*");
        }
    }

    private static class Fixture implements AutoCloseable {
        private final Connection anchor;
        private final TestOutputFormat format;

        Fixture(String mode, String table) throws SQLException {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID());
            anchor = dataSource.getConnection();
            try (Statement statement = anchor.createStatement()) {
                statement.execute("CREATE TABLE UNKEYED (ID INT)");
                statement.execute("CREATE TABLE WITH_PRIMARY_KEY (ID INT PRIMARY KEY)");
                statement.execute("CREATE TABLE WITH_UNIQUE_INDEX (ID INT)");
                statement.execute("CREATE UNIQUE INDEX UNIQUE_ID ON WITH_UNIQUE_INDEX (ID)");
            }
            format = new TestOutputFormat(dataSource.getConnection(), mode, table);
        }

        @Override
        public void close() throws SQLException {
            format.connection.close();
            anchor.close();
        }
    }

    private static class TestOutputFormat extends JdbcOutputFormat {
        private final Connection connection;
        private boolean prepared;

        TestOutputFormat(Connection connection, String mode, String table) {
            this.connection = connection;
            SinkConnectionConfig sink = new SinkConnectionConfig();
            sink.setTable(Collections.singletonList(table));
            sink.setSchema("PUBLIC");
            jdbcConfig = new JdbcConfig();
            jdbcConfig.setConnection(Collections.singletonList(sink));
            jdbcConfig.setMode(mode);
            jdbcDialect = mock(JdbcDialect.class, CALLS_REAL_METHODS);
        }

        void initialize() {
            openInternal(0, 1);
        }

        @Override
        protected Connection getConnection() {
            return connection;
        }

        @Override
        public void buildStatementWrapper() {
            prepared = true;
        }
    }
}
