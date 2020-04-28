package com.amazon.opendistroforelasticsearch.jdbc.config;

public class FetchSizeProperty extends IntConnectionProperty{

    public static final String KEY = "fetchSize";

    public FetchSizeProperty() {
        super(KEY);
    }

    public Integer getDefault() {
        return 0;
    }
}
