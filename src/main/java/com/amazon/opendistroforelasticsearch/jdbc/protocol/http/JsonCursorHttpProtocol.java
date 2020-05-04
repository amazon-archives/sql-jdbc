package com.amazon.opendistroforelasticsearch.jdbc.protocol.http;

import com.amazon.opendistroforelasticsearch.jdbc.protocol.QueryRequest;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.QueryResponse;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.exceptions.ResponseException;
import com.amazon.opendistroforelasticsearch.jdbc.transport.http.HttpTransport;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.IOException;
import java.io.InputStream;

public class JsonCursorHttpProtocol extends JsonHttpProtocol {

    public JsonCursorHttpProtocol(HttpTransport transport) {
        this(transport, DEFAULT_SQL_CONTEXT_PATH);
    }

    public JsonCursorHttpProtocol(HttpTransport transport, String sqlContextPath) {
        super(transport, sqlContextPath);
    }

    @Override
    public QueryResponse execute(QueryRequest request) throws ResponseException, IOException {
        try (CloseableHttpResponse response = getTransport().doPost(
                getSqlContextPath(),
                defaultJsonHeaders,
                defaultJdbcParams,
                buildQueryRequestBody(request), 0)) {

            return getJsonHttpResponseHandler().handleResponse(response, this::processQueryResponse);

        }
    }

    private String buildQueryRequestBody(QueryRequest queryRequest) throws IOException {
        JsonCursorQueryRequest jsonQueryRequest = new JsonCursorQueryRequest(queryRequest);
        String requestBody = mapper.writeValueAsString(jsonQueryRequest);
        return requestBody;
    }

    private JsonQueryResponse processQueryResponse(InputStream contentStream) throws IOException {
        return mapper.readValue(contentStream, JsonQueryResponse.class);
    }

}
