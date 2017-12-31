package com.elad.storm.reliablilty;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by eladw on 12/31/17.
 */
public class RandomFailureBolt extends BaseRichBolt{

    private static final Integer MAX_PERCENT_FAIL = 8;
    Random random = new Random();
    private OutputCollector collector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    public void execute(Tuple input) {
        Integer r = random.nextInt(10);
        if(r< MAX_PERCENT_FAIL){
            collector.emit(input, new Values(input.getString(0)));
            collector.ack(input);
        } else {
            collector.fail(input);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
    }
}
