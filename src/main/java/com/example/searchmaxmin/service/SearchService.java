package com.example.searchmaxmin.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.searchmaxmin.pojo.Info;

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
    private static ArrayList<String> fieldNames_002 =new ArrayList<String>(Arrays.asList(
            "altitude","latitude","x","y","z","longitude"
    ));
    private static   ArrayList<String>fieldNames_003=new ArrayList<>(Arrays.asList(
            "vx","vy","gamma","vz","psi","phi"
    ));

    public List<Info> getAllCards()
    {
        List<Info> all = mongoTemplate.findAll(Info.class,collectionName);
        return all;
    }

    public BasicDBObject getMinMaxMTime()
    {
        //db.Infos.find().sort({time:1}).skip(0).limit(1);
        //db.Infos.aggregate([{$group : {_id : "$systemId", min : {$min : "$time"}}}])
        BasicDBObject basicDBObject=null;
        Aggregation aggregation=Aggregation.newAggregation(
                Aggregation.group("$systemId").min("$time").as("mintime").max("$time").as("maxtime"));

        AggregationResults<BasicDBObject> aggregate = mongoTemplate.aggregate(aggregation, collectionName, BasicDBObject.class);

        for (Iterator<BasicDBObject>iterator=aggregate.iterator();iterator.hasNext();)
        {
            BasicDBObject next = iterator.next();
            basicDBObject=next;
        }
        return basicDBObject;
    }

    public List<Info> getDataByTopicName(String topicName)
    {
        Query query=Query.query(Criteria.where("topicName").is(topicName));
        List<Info> infos = mongoTemplate.find(query, Info.class,collectionName);
        return infos;
    }
    //区分topicName并生成csv文本
    public String distTopicName(String topicName,List<Info> infos) {
        //经常对字符串进行更新StringBuffer性能更好
        StringBuffer s=new StringBuffer();
        this.addHeader(s,topicName);
        for (int i=0;i<infos.size();i++)
        {
            Info info = infos.get(i);
            Double time = info.getTime();
            String from = info.getFrom();
            String content = info.getContent();
            JSONObject jsonObject= JSON.parseObject(content);
            Object obj = jsonObject.get(topicName);
            //time、from固定部分
            if (topicName.equals("topic_001"))
            {
                s.append(time).append(",")
                        .append(from).append(",")
                        .append(obj).append(",")
                        .append("\n");
            }else
            {
                ArrayList<String> fieldNames=null;
                if (topicName.equals("topic_002"))
                {
                    fieldNames=fieldNames_002;
                }else if (topicName.equals("topic_003"))
                {
                    fieldNames=fieldNames_003;
                }
                StringBuffer records=new StringBuffer();
                for (int j = 0; j< fieldNames.size(); j++)
                {
                    String s1 = fieldNames.get(j);
                    JSONObject obj1 = (JSONObject) obj;
                    Object o = obj1.get(s1);
                    records.append(o).append(",");
                }
                s.append(time).append(",")
                        .append(from).append(",")
                        .append(records)
                        .append("\n");
            }
        }
        //StringBuffer->String
        String result = s.toString();
        //去除最后一行的标点的最后一个标点
        if(result!=null&&result!="") {
            result=result.substring(0,result.length()-2);
        }
        return result;
    }
    //添加header
    private void addHeader(StringBuffer s,String topicName)
    {
        s.append("time,from,");
        if (topicName.equals("topic_001")){
            s.append("topic_001,");
        }else  if (topicName.equals("topic_002")) {
            for (int i=0;i<fieldNames_002.size();i++)
            {
                s.append(fieldNames_002.get(i)).append(",");
            }
        }else if (topicName.equals("topic_003")) {
            for (int i=0;i<fieldNames_003.size();i++)
            {
                s.append(fieldNames_003.get(i)).append(",");
            }
        }
        s.append("\n");
    }
}
