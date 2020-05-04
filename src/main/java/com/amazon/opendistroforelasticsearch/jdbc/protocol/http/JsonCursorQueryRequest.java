package com.amazon.opendistroforelasticsearch.jdbc.protocol.http;

import com.amazon.opendistroforelasticsearch.jdbc.protocol.Parameter;
import com.amazon.opendistroforelasticsearch.jdbc.protocol.QueryRequest;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class JsonCursorQueryRequest implements QueryRequest  {

    private final String cursor;
    private List<? extends Parameter> parameters;

    public JsonCursorQueryRequest(QueryRequest queryRequest) {
        this.cursor = queryRequest.getQuery();
        this.parameters = queryRequest.getParameters();
    }


    @JsonProperty("cursor")
    @Override
    public String getQuery() {
        return cursor;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Override
    public List<? extends Parameter> getParameters() {
        return parameters;
    }

    @JsonIgnore
    @Override
    public int getFetchSize() {
        return 0;
    }
}
