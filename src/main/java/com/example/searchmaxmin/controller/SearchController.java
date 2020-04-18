package com.example.searchmaxmin.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.searchmaxmin.pojo.Info;
import com.example.searchmaxmin.service.SearchService;
import com.mongodb.BasicDBObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@RestController
public class SearchController {
    @Autowired
    private SearchService searchService;

    //查询所有的记录
    @RequestMapping("/findAll")
    public String findAll()
    {
        List<Info> allCards = searchService.getAllCards();
        return allCards.toString();
    }
    //查询用时最短、最长的时间
    @RequestMapping("/findMinMaxTime")
    public BasicDBObject getMinMaxTime()
    {
        BasicDBObject json = searchService.getMinMaxMTime();
        return json;
    }
    @RequestMapping("/downloadCSVFile")
    public void getCSVFile(String topicName, HttpServletResponse response)
    {
        List<Info> infos = searchService.getDataByTopicName(topicName);
        String result = searchService.distTopicName(topicName,infos);
        System.out.println(result);
        try {
            ServletOutputStream outputStream = response.getOutputStream();
            outputStream.print(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}