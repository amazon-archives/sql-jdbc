package com.amazon.opendistroforelasticsearch.jdbc.protocol.http;

import com.amazon.opendistroforelasticsearch.jdbc.protocol.JdbcQueryParam;;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.QueryRequest;


import java.util.List;
import java.util.Objects;

public class JdbcCursorQueryRequest implements QueryRequest {

    String cursor;
    List<JdbcQueryParam> parameters;

    public JdbcCursorQueryRequest(String cursor) {
        this.cursor = cursor;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JdbcCursorQueryRequest)) return false;
        JdbcCursorQueryRequest that = (JdbcCursorQueryRequest) o;
        return Objects.equals(cursor, that.cursor) &&
                Objects.equals(getParameters(), that.getParameters());
    }

    @Override
    public int hashCode() {
        return Objects.hash(cursor, getParameters());
    }

    @Override
    public String getQuery() {
        return cursor;
    }

    @Override
    public List<JdbcQueryParam> getParameters() {
        return parameters;
    }

    public void setParameters(List<JdbcQueryParam> parameters) {
        this.parameters = parameters;
    }

    @Override
    public int getFetchSize() {
        return 0;
    }

    @Override
    public String toString() {
        return "JdbcQueryRequest{" +
                "cursor='" + cursor + '\'' +
                ", parameters=" + parameters +
                '}';
    }
}
