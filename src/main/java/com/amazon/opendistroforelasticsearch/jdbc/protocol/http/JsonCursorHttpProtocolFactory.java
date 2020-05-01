package com.amazon.opendistroforelasticsearch.jdbc.protocol.http;

import com.amazon.opendistroforelasticsearch.jdbc.config.ConnectionConfig;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.ProtocolFactory;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.HttpTransport;

public class JsonCursorHttpProtocolFactory implements ProtocolFactory<JsonCursorHttpProtocol, HttpTransport> {

    public static JsonCursorHttpProtocolFactory INSTANCE = new JsonCursorHttpProtocolFactory();

    private JsonCursorHttpProtocolFactory() {

    }

    @Override
    public JsonCursorHttpProtocol getProtocol(ConnectionConfig connectionConfig, HttpTransport transport) {
        return new JsonCursorHttpProtocol(transport);
    }
}
