package com.elad.storm.reliablilty;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eladw on 12/21/17.
 */
public class ReliableWordReader extends BaseRichSpout {

    private static final Integer MAX_FAILURES = 3;
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private BufferedReader reader;
    private Map<Integer, String> allMessages;
    private List<Integer> toSend;
    private Map<Integer, Integer> msgFailureCount;

    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        try{
            this.fileReader = new FileReader(conf.get("fileToRead").toString());
            this.reader = new BufferedReader(fileReader);
            this.allMessages = new HashMap<Integer, String>();
            this.toSend = new ArrayList();
            int i=0;
            while(reader.readLine()!=null){
                allMessages.put(i++, reader.readLine());
                toSend.add(i);
            }
            this.msgFailureCount = new HashMap<Integer,Integer>();

        } catch (Exception e){
            throw new RuntimeException("Error read file [ " + conf.get("fileToRead") +  "]");
        }
    }

    public void nextTuple() {
        if(!toSend.isEmpty()){
            for(int msgId : toSend){
                String word = allMessages.get(msgId);
                collector.emit(new Values(word), msgId);
            }
            toSend.clear();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("Sending msg [" + msgId + "] success");
    }

    @Override
    public void fail(Object msgId) {
        Integer failedId = (Integer)msgId;
        Integer failures = 1;
        if(msgFailureCount.containsKey(failedId)){
            failures = msgFailureCount.get(failedId) + 1;
        }

        if(failures < MAX_FAILURES){
            msgFailureCount.put(failedId, failures);
            toSend.add(failedId);
            System.out.println("Re-sending message[" + failedId +"]");
        } else {
            System.out.println("sending message[" + failedId +"] failed !!!!!");
        }
    }
}