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
package com.dtstack.chunjun.connector.jdbc.util;

import com.dtstack.chunjun.config.SyncConfig;
import com.dtstack.chunjun.connector.jdbc.config.JdbcConfig;
import com.dtstack.chunjun.connector.jdbc.converter.JdbcRawTypeConverterTest;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormat;
import com.dtstack.chunjun.connector.jdbc.source.JdbcInputFormatBuilder;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.chunjun.connector.jdbc.util.key.NumericStringTypeUtil;
import com.dtstack.chunjun.converter.RawTypeMapper;
import com.dtstack.chunjun.element.ColumnRowData;
import com.dtstack.chunjun.restore.FormatState;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;

public class NumericStringIncrementTest {
    @Test
    public void keyUtilityKeepsExactPositionsAndRejectsInvalidValues() throws Exception {
        NumericStringTypeUtil key = new NumericStringTypeUtil();
        String large = "99999999999999999999";
        Assert.assertEquals(new BigInteger(large), key.transToLocationValue(large));
        Assert.assertEquals("2", key.checkAndFormatLocationStr("0002"));
        for (String invalid :
                new String[] {
                    "",
                    "-1",
                    "+1",
                    "1.0",
                    "1e2",
                    "a",
                    " 1",
                    "１２",
                    "1;DROP TABLE T",
                    "100000000000000000000000000000000000000"
                }) {
            Assert.assertThrows(
                    IllegalArgumentException.class, () -> key.checkAndFormatLocationStr(invalid));
        }
        try (Fixture fixture = new Fixture();
                PreparedStatement ps =
                        fixture.anchor.prepareStatement("SELECT CAST(? AS DECIMAL(38,0))")) {
            key.setPsWithLocationStr(ps, 1, large);
            try (ResultSet resultSet = ps.executeQuery()) {
                Assert.assertTrue(resultSet.next());
                Assert.assertEquals(
                        new BigInteger(large), key.getLocationValueFromRs(resultSet, 1));
            }
        }
    }

    @Test
    public void jsonReaderUsesNumericOrderAndExactReportedPosition() throws Exception {
        try (Fixture fixture = new Fixture()) {
            TestFormat format = fixture.reader(false, "0", null, null, true);
            try {
                Assert.assertEquals(
                        Arrays.asList("0002", "10", "9223372036854775808", "99999999999999999999"),
                        format.drain());
                Assert.assertEquals(
                        new BigInteger("99999999999999999999"), format.reportedPosition());
                Assert.assertEquals("99999999999999999999", format.getFormatState().getState());
            } finally {
                format.closeInternal();
            }
        }
    }

    @Test
    public void checkpointResumeAndCustomQueryUseNumericComparison() throws Exception {
        try (Fixture fixture = new Fixture()) {
            TestFormat format =
                    fixture.reader(
                            false, "0", "10", "SELECT \"id\" FROM T ORDER BY \"id\" DESC", true);
            try {
                Assert.assertEquals(
                        Arrays.asList("9223372036854775808", "99999999999999999999"),
                        format.drain());
            } finally {
                format.closeInternal();
            }
        }
    }

    @Test
    public void pollingAfterAnEmptyBatchKeepsItsExactStartingPosition() throws Exception {
        try (Fixture fixture = new Fixture()) {
            String start = "99999999999999999999";
            TestFormat format = fixture.reader(true, start, null, null, true);
            try {
                Assert.assertTrue(format.drain().isEmpty());
                Assert.assertEquals(start, format.getFormatState().getState());
                try (Statement statement = fixture.anchor.createStatement()) {
                    statement.execute("INSERT INTO T VALUES ('100000000000000000000')");
                }
                format.poll();
                Assert.assertEquals(Arrays.asList("100000000000000000000"), format.drain());
                format.poll();
                Assert.assertTrue(format.drain().isEmpty());
            } finally {
                format.closeInternal();
            }
        }
    }

    @Test
    public void disabledModeRetainsTheExistingVarcharRejection() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Assert.assertThrows(
                    RuntimeException.class, () -> fixture.reader(false, "0", null, null, false));
        }
    }

    @Test
    public void unsupportedCombinationsAreRejected() {
        List<Consumer<JdbcConfig>> invalid =
                Arrays.asList(
                        config -> config.setParallelism(2), config -> config.setStartLocation(null),
                        config -> config.setIncreColumn(null), config -> config.setOrderBy(false),
                        config -> config.setSplitPk("other"),
                                config -> config.setOrderByColumn("other"),
                        config -> config.setUseMaxFunc(true),
                                config -> config.setPollingFromMax(true));
        for (Consumer<JdbcConfig> change : invalid) {
            JdbcConfig config = new JdbcConfig();
            config.setNumericStringIncrement(true);
            config.setIncreColumn("id");
            config.setStartLocation("0");
            config.setParallelism(1);
            change.accept(config);
            Assert.assertThrows(
                    IllegalArgumentException.class, config::validateNumericStringIncrement);
        }
        new JdbcConfig().validateNumericStringIncrement();
    }

    private static JdbcDialect dialect() {
        return new JdbcDialect() {
            @Override
            public String dialectName() {
                return "H2 test";
            }

            @Override
            public boolean canHandle(String url) {
                return true;
            }

            @Override
            public RawTypeMapper getRawTypeConverter() {
                return JdbcRawTypeConverterTest::apply;
            }

            @Override
            public java.util.function.Function<
                            org.apache.flink.api.java.tuple.Tuple3<String, Integer, Integer>,
                            com.dtstack.chunjun.config.TypeConfig>
                    typeBuilder() {
                // H2 uses a JDBC type name that differs from MySQL/Oracle character metadata.
                return type ->
                        new com.dtstack.chunjun.config.TypeConfig(
                                "CHARACTER VARYING".equals(type.f0) ? "VARCHAR" : type.f0,
                                type.f1,
                                type.f2);
            }

            @Override
            public String numericStringExpression(String column) {
                return "CAST(" + column + " AS DECIMAL(38,0))";
            }
        };
    }

    private static class Fixture implements AutoCloseable {
        private final JdbcDataSource dataSource = new JdbcDataSource();
        private final Connection anchor;

        Fixture() throws SQLException {
            dataSource.setURL("jdbc:h2:mem:" + UUID.randomUUID());
            anchor = dataSource.getConnection();
            try (Statement statement = anchor.createStatement()) {
                statement.execute("CREATE TABLE T (\"id\" VARCHAR(38))");
                statement.execute(
                        "INSERT INTO T VALUES ('10'), ('0002'), ('99999999999999999999'), ('9223372036854775808'), (NULL)");
            }
        }

        TestFormat reader(
                boolean polling, String start, Object restored, String query, boolean enabled)
                throws Exception {
            JsonObject json =
                    new JsonParser()
                            .parse(
                                    "{\"job\":{\"content\":[{\"reader\":{\"name\":\"mysqlreader\",\"parameter\":{\"username\":\"sa\",\"column\":[{\"name\":\"id\",\"type\":\"varchar\"}],\"increColumn\":\"id\",\"connection\":[{\"jdbcUrl\":[\"unused\"],\"table\":[\"T\"],\"schema\":\"PUBLIC\"}]}},\"writer\":{\"name\":\"streamwriter\",\"parameter\":{}}}],\"setting\":{\"speed\":{\"channel\":1},\"restore\":{}}}}")
                            .getAsJsonObject();
            JsonObject params =
                    json.getAsJsonObject("job")
                            .getAsJsonArray("content")
                            .get(0)
                            .getAsJsonObject()
                            .getAsJsonObject("reader")
                            .getAsJsonObject("parameter");
            params.addProperty("numericStringIncrement", enabled);
            params.addProperty("polling", polling);
            params.addProperty("startLocation", start);
            if (query != null) {
                params.addProperty("customSql", query);
            }
            params.getAsJsonArray("connection")
                    .get(0)
                    .getAsJsonObject()
                    .getAsJsonArray("jdbcUrl")
                    .set(0, new com.google.gson.JsonPrimitive(dataSource.getURL()));
            TestFactory factory =
                    new TestFactory(SyncConfig.parseJob(json.toString()), this, restored);
            factory.createSource();
            TestFormat format = factory.format;
            format.openInternal(format.createInputSplitsInternal(1)[0]);
            return format;
        }

        @Override
        public void close() throws SQLException {
            anchor.close();
        }
    }

    private static class TestFactory extends JdbcSourceFactory {
        private final Fixture fixture;
        private final Object restored;
        private TestFormat format;

        TestFactory(SyncConfig config, Fixture fixture, Object restored) {
            super(config, mock(StreamExecutionEnvironment.class), dialect());
            this.fixture = fixture;
            this.restored = restored;
        }

        @Override
        protected Connection getConn() {
            try {
                return fixture.dataSource.getConnection();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        protected JdbcInputFormatBuilder getBuilder() {
            format = new TestFormat(fixture, restored);
            return new JdbcInputFormatBuilder(format);
        }

        @Override
        protected DataStream<RowData> createInput(InputFormat<RowData, InputSplit> inputFormat) {
            return null;
        }
    }

    private static class TestFormat extends JdbcInputFormat {
        private final Fixture fixture;

        TestFormat(Fixture fixture, Object restored) {
            this.fixture = fixture;
            setFormatState(new FormatState(0, restored));
            setRuntimeContext(mock(RuntimeContext.class));
        }

        @Override
        protected Connection getConnection() throws SQLException {
            return fixture.dataSource.getConnection();
        }

        @Override
        protected boolean useCustomReporter() {
            return false;
        }

        List<String> drain() throws Exception {
            List<String> rows = new ArrayList<>();
            while (hasNext) {
                rows.add(((ColumnRowData) nextRecordInternal(null)).getField(0).asString());
            }
            return rows;
        }

        BigInteger reportedPosition() {
            return endLocationAccumulator.getLocalValue();
        }

        void poll() throws SQLException {
            resultSet.close();
            dbConn.commit();
            queryForPolling(new NumericStringTypeUtil().transToLocationValue(state).toString());
        }
    }
}
