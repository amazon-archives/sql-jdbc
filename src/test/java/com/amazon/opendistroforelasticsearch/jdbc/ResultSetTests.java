/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.amazon.opendistroforelasticsearch.jdbc;

import com.amazon.opendistroforelasticsearch.jdbc.internal.exceptions.ObjectClosedException;
import com.amazon.opendistroforelasticsearch.jdbc.logging.NoOpLogger;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.QueryResponse;
import com.amazon.opendistroforelasticsearch.jdbc.types.ElasticsearchType;
import com.amazon.opendistroforelasticsearch.jdbc.test.PerTestWireMockServerExtension;
import com.amazon.opendistroforelasticsearch.jdbc.test.WireMockServerHelpers;
import com.amazon.opendistroforelasticsearch.jdbc.test.mocks.MockResultSet;
import com.amazon.opendistroforelasticsearch.jdbc.test.mocks.MockResultSetRows;
import com.amazon.opendistroforelasticsearch.jdbc.test.mocks.MockResultSetMetaData;
import com.amazon.opendistroforelasticsearch.jdbc.test.mocks.QueryMock;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

@ExtendWith(PerTestWireMockServerExtension.class)
public class ResultSetTests implements WireMockServerHelpers {

    @ParameterizedTest
    @MethodSource("queryMockProvider")
    void testQueryResultSet(QueryMock queryMock, WireMockServer mockServer) throws SQLException, IOException {
        queryMock.setupMockServerStub(mockServer);

        Connection con = new Driver().connect(getBaseURLForMockServer(mockServer), null);
        Statement st = con.createStatement();
        ResultSet rs = assertDoesNotThrow(() -> st.executeQuery(queryMock.getSql()));

        assertNotNull(rs);

        // prior to ResultSet iteration
        assertTrue(rs.isBeforeFirst(), "isBeforeFirst not True for non-empty ResultSet before the first next()");
        SQLException ex = assertThrows(SQLException.class, () -> rs.getObject(1));
        assertTrue(ex.getMessage().contains("Illegal operation before start of ResultSet"));

        // this will consume the resultSet
        queryMock.getMockResultSet().assertMatches(rs);

        // post ResultSet iteration
        assertTrue(rs.isAfterLast(), "isAfterLast not True after end of ResultSet.");
        assertFalse(rs.isBeforeFirst(), "isBeforeFirst True when isAfterLast is True.");
        ex = assertThrows(SQLException.class, () -> rs.getObject(1));
        assertTrue(ex.getMessage().contains("Illegal operation after end of ResultSet"));

        rs.close();

        // post ResultSet close
        assertTrue(rs.isClosed(), "ResultSet isClosed returns False after call to close it.");
        assertThrows(ObjectClosedException.class, rs::next);
        assertThrows(ObjectClosedException.class, rs::isAfterLast);
        assertThrows(ObjectClosedException.class, rs::isBeforeFirst);
        assertThrows(ObjectClosedException.class, () -> rs.getObject(1));

        st.close();
        con.close();
    }

    private static Stream<Arguments> queryMockProvider() {
        return Stream.of(
                Arguments.of(new QueryMock.NycTaxisQueryMock()),
                Arguments.of(new QueryMock.NycTaxisQueryWithAliasMock())
        );
    }

    @Test
    void testNullableFieldsQuery(WireMockServer mockServer) throws SQLException, IOException {
        QueryMock.NullableFieldsQueryMock queryMock = new QueryMock.NullableFieldsQueryMock();

        queryMock.setupMockServerStub(mockServer);

        Connection con = new Driver().connect(getBaseURLForMockServer(mockServer), null);
        Statement st = con.createStatement();
        ResultSet rs = assertDoesNotThrow(() -> st.executeQuery(queryMock.getSql()));

        assertNotNull(rs);

        MockResultSetMetaData mockResultSetMetaData = MockResultSetMetaData.builder()
                .column("testBoolean", ElasticsearchType.BOOLEAN)
                .column("docId", ElasticsearchType.TEXT)
                .column("testByte", ElasticsearchType.BYTE)
                .column("testFloat", ElasticsearchType.FLOAT)
                .column("testLong", ElasticsearchType.LONG)
                .column("testShort", ElasticsearchType.SHORT)
                .column("testHalfFloat", ElasticsearchType.HALF_FLOAT)
                .column("testTimeStamp", ElasticsearchType.DATE)
                .column("testScaledFloat", ElasticsearchType.SCALED_FLOAT)
                .column("testKeyword", ElasticsearchType.KEYWORD)
                .column("testText", ElasticsearchType.TEXT)
                .column("testDouble", ElasticsearchType.DOUBLE)
                .build();

        MockResultSetRows mockResultSetRows = MockResultSetRows.builder()
                .row()
                .column(false, true)
                .column("2", false)
                .column((byte) 0, true)
                .column((float) 22.145135459218345, false)
                .column((long) 0, true)
                .column((short) 0, true)
                .column((float) 24.324234543532153, false)
                .column(Timestamp.valueOf("2015-01-01 12:10:30"), false)
                .column((double) 24.324234543532153, false)
                .column("Test String", false)
                .column("document3", false)
                .column((double) 0, true)
                .row()
                .column(true, false)
                .column("1", false)
                .column((byte) 126, false)
                .column((float) 0, true)
                .column((long) 32000320003200030L, false)
                .column((short) 29000, false)
                .column((float) 0, true)
                .column(null, true)
                .column((double) 0, true)
                .column(null, true)
                .column(null, true)
                .column((double) 22.312423148903218, false)
                .build();

        MockResultSet mockResultSet = new MockResultSet(mockResultSetMetaData, mockResultSetRows);

        mockResultSet.assertMatches(rs);

        rs.close();
        con.close();
    }

    @Test
    void testResultSetWrapper() throws SQLException {
        ResultSetImpl rsImpl = new ResultSetImpl(mock(StatementImpl.class), mock(QueryResponse.class), NoOpLogger.INSTANCE);

        assertTrue(rsImpl.isWrapperFor(ResultSet.class),
                "ResultSet impl returns False for isWrapperFor(ResultSet.class)");

        ResultSet unwrapped = assertDoesNotThrow(() -> rsImpl.unwrap(ResultSet.class),
                "Unexpected exception when unwrapping ResultSet");

        assertNotNull(unwrapped, "Unwrapped ResultSet null");

        assertFalse(rsImpl.isWrapperFor(mock(ResultSet.class).getClass()),
                "ResultSet impl returns True for isWrapperFor(mockClass)");

        assertFalse(rsImpl.isWrapperFor(null),
                "ResultSet impl returns True for isWrapperFor(null)");

        SQLException ex = assertThrows(SQLException.class, () -> rsImpl.unwrap(mock(ResultSet.class).getClass()));
        assertTrue(ex.getMessage().contains("Unable to unwrap"));
    }
}
