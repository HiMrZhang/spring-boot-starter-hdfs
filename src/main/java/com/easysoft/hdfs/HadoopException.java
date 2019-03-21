package com.easysoft.hdfs;

public class HadoopException extends RuntimeException {
    public HadoopException(String message, Throwable ex) {
        super(message, ex);
    }

    public HadoopException(String message) {
        super(message);
    }
}
