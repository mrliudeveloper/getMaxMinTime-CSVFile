package com.example.searchmaxmin.service;

import com.alibaba.fastjson.JSONObject;
import com.example.searchmaxmin.pojo.Info;


import com.example.searchmaxmin.vo.FileDataVo;
import com.mongodb.BasicDBObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class SearchService {

    @Autowired
    private MongoTemplate mongoTemplate;
    private  String collectionName="Infos";

    public List<Info> getAllCards()
    {
        List<Info> all = mongoTemplate.findAll(Info.class,collectionName);
        return all;
    }

    public JSONObject getMinMaxMTime()
    {
        //db.Infos.find().sort({time:1}).skip(0).limit(1);
        //db.Infos.aggregate([{$group : {_id : "$systemId", min : {$min : "$time"}}}])
        JSONObject jsonObject=null;
        Aggregation aggregation=Aggregation.newAggregation(
                Aggregation.group("$systemId")
                        .avg("$time").as("avgtime")
                        .min("$time").as("mintime")
                        .max("$time").as("maxtime"));

        AggregationResults<JSONObject> aggregate = mongoTemplate.aggregate(aggregation, collectionName, JSONObject.class);

        for (Iterator<JSONObject>iterator=aggregate.iterator();iterator.hasNext();)
        {
            JSONObject next = iterator.next();
            jsonObject=next;
        }
        return jsonObject;
    }

    public List<Info> getDataByTopicName(String topicName)
    {
        Query query=Query.query(Criteria.where("topicName").is(topicName));
        List<Info> infos = mongoTemplate.find(query, Info.class,collectionName);
        return infos;
    }
    //替换掉isStruct方法即可
    public boolean isStruct() {

        return true;
    }
    public List<FileDataVo> saveFileData(String topicName, List<Info> infos)
    {
        List<FileDataVo> fileDatas=new ArrayList<>();
        for (int i=0;i<infos.size();i++)
        {
            Info info = infos.get(i);
            FileDataVo data = new FileDataVo();
            //小数点后保留三位
            double time = (int) (info.getTime() * 1000) / 1000.0;
            data.setTime(time);
            data.setFrom(info.getFrom());
            String contentString = info.getContent();
            Map<String,Object>map=(Map)JSONObject.parseObject(contentString);
            //当为结构体是需要剥去外层，内层数据保存到map
            if (isStruct())
            {
                map=(Map<String, Object>) map.get(topicName);
            }
            data.setContents(map);
            fileDatas.add(data);
        }
        return fileDatas;
    }

    public String format(List<FileDataVo> fileDataVos)
    {
        StringBuffer buffer = new StringBuffer();
        //set->list,保证每次遍历的一致性
        List<String> list=new ArrayList<>();
        list.addAll(fileDataVos.get(0).getContents().keySet());
        //add 表头
        this.addHeader(buffer,list);
        //字符串拼接
        for (int i=0;i<fileDataVos.size();i++)
        {
            FileDataVo fileDataVo = fileDataVos.get(i);
            StringBuffer stringBuffer=new StringBuffer();
            Map<String, Object> contents = fileDataVo.getContents();
            Double time = fileDataVo.getTime();
            String from = fileDataVo.getFrom();
            for (String s:list)
            {
                stringBuffer.append(contents.get(s)).append(",");
            }
            String bufferString = stringBuffer.substring(0, stringBuffer.length()-1);
            buffer.append("\n").append(time).append(",").append(from).append(",").append(bufferString);
        }
        String result=buffer.toString();
        if (isStruct())
        {
        result = result.substring(1, fileDataVos.toString().length() - 1);
        }
        return result;
    }
    private void addHeader(StringBuffer str, List<String>list) {
        str.append(" time").append(",").append("from").append(",");
        for (String s:list)
        {
            str.append(s).append(",");
        }
    }

}
