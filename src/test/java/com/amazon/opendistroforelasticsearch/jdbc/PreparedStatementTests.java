package com.amazon.opendistroforelasticsearch.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import com.amazon.opendistroforelasticsearch.jdbc.config.ConnectionConfig;
import com.amazon.opendistroforelasticsearch.jdbc.logging.NoOpLogger;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.ConnectionResponse;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.JdbcQueryRequest;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.Protocol;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.ProtocolFactory;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.QueryRequest;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.QueryResponse;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.exceptions.ResponseException;
import com.amazon.opendistroforelasticsearch.jdbc.test.PerTestWireMockServerExtension;
import com.amazon.opendistroforelasticsearch.jdbc.transport.Transport;
import com.amazon.opendistroforelasticsearch.jdbc.transport.TransportFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;


/**
 * PreparedStatement tests
 *
 * @author echo
 * @since 12.03.20
 **/
@ExtendWith(PerTestWireMockServerExtension.class)
public class PreparedStatementTests {

    @Test
    void testPreparedStatementExecute() throws ResponseException, IOException, SQLException {

        final String sql = "select pickup_datetime, trip_type, passenger_count, " +
            "fare_amount, extra, vendor_id from nyc_taxis LIMIT 5";

        TransportFactory tf = mock(TransportFactory.class);
        ProtocolFactory pf = mock(ProtocolFactory.class);
        Protocol mockProtocol = mock(Protocol.class);

        when(mockProtocol.connect(anyInt())).thenReturn(mock(ConnectionResponse.class));

        when(tf.getTransport(any(), any(), any()))
            .thenReturn(mock(Transport.class));

        when(pf.getProtocol(any(ConnectionConfig.class), any(Transport.class)))
            .thenReturn(mockProtocol);

        when(mockProtocol.execute(any(QueryRequest.class)))
            .thenReturn(mock(QueryResponse.class));

        try (Connection con = new ConnectionImpl(ConnectionConfig.builder().build(), tf, pf, NoOpLogger.INSTANCE)) {
            try (PreparedStatement pstm = con.prepareStatement(sql)) {
                assertTrue(pstm.execute());
                ResultSet rs = assertDoesNotThrow(() -> pstm.getResultSet());
                rs.close();
            }
        }
    }

}
