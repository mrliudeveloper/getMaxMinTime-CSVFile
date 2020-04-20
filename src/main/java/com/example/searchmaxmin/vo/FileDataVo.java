package com.example.searchmaxmin.vo;

import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;
import java.util.Set;

@Getter
@Setter
public class FileDataVo {

    private  Double time;

    private  String from;

    private Map<String,Object> contents;
}
