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
package com.dtstack.chunjun.connector.jdbc.util.key;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Exact nonnegative integer positions stored in character columns, limited to 38 digits. */
public class NumericStringTypeUtil extends KeyUtil<String, BigInteger> {
    private static final long serialVersionUID = 1L;

    @Override
    public String getSqlValueFromRs(ResultSet rs, int index) throws SQLException {
        return checkAndFormatLocationStr(rs.getString(index));
    }

    @Override
    public String getSqlValueFromRs(ResultSet rs, String column) throws SQLException {
        return checkAndFormatLocationStr(rs.getString(column));
    }

    @Override
    public void setPsWithSqlValue(PreparedStatement ps, int index, String value)
            throws SQLException {
        ps.setBigDecimal(index, new BigDecimal(checkAndFormatLocationStr(value)));
    }

    @Override
    public void setPsWithLocationStr(PreparedStatement ps, int index, String value)
            throws SQLException {
        setPsWithSqlValue(ps, index, value);
    }

    @Override
    public BigInteger transToLocationValueInternal(Object value) {
        return new BigInteger(checkAndFormatLocationStr(value.toString()));
    }

    @Override
    public String transLocationStrToStatementValue(String value) {
        return checkAndFormatLocationStr(value);
    }

    @Override
    public String transToStatementValueInternal(Object value) {
        return checkAndFormatLocationStr(value.toString());
    }

    @Override
    public String transLocationStrToSqlValue(String value) {
        return checkAndFormatLocationStr(value);
    }

    @Override
    public BigInteger getNullDefaultLocationValue() {
        return BigInteger.valueOf(-1);
    }

    @Override
    public String checkAndFormatLocationStr(String value) {
        if (value == null || !value.matches("[0-9]{1,38}")) {
            throw new IllegalArgumentException(
                    "Numeric string positions must contain 1 to 38 ASCII digits");
        }
        return new BigInteger(value).toString();
    }
}
