package com.amazon.opendistroforelasticsearch.jdbc.protocol.http;

import com.amazon.opendistroforelasticsearch.jdbc.protocol.Parameter;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.QueryRequest;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class JsonCursorQueryRequest implements QueryRequest  {

    private final String cursor;

    public JsonCursorQueryRequest(QueryRequest queryRequest) {
        this.cursor = queryRequest.getQuery();
    }

    @JsonProperty("cursor")
    @Override
    public String getQuery() {
        return cursor;
    }

    @JsonIgnore
    @Override
    public List<? extends Parameter> getParameters() {
        return null;
    }

    @JsonIgnore
    @Override
    public int getFetchSize() {
        return 0;
    }
}
