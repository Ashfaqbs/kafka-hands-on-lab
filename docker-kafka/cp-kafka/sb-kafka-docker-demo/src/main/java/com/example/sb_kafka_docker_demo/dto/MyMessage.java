package com.example.sb_kafka_docker_demo.dto;

public class MyMessage  {
    private String key;
    private String value;



    // IMP Create Constructors, getters, setters

    public MyMessage()
    {

    }

    public MyMessage(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
