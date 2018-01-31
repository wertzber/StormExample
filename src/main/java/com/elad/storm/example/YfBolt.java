package com.elad.storm.example;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.omg.SendingContext.RunTime;

import java.io.PrintWriter;
import java.util.Map;

/**
 * Created by eladw on 12/21/17.
 */
public class YfBolt extends BaseBasicBolt {

    private PrintWriter writer;
    private String filename;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
       this.filename = stormConf.get("fileToWrite").toString();;
    }

    @Override
    public void cleanup() {
        writer.close();
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        try{
            this.writer = new PrintWriter(filename, "UTF-8");
        } catch (Exception e){
            throw new RuntimeException("Error opening file [" + filename + "]");
        }

        String symbol = input.getValue(0).toString();
        String timestamp = input.getString(1);
        Double price = (Double) input.getValueByField("price");
        Double prevClose = input.getDoubleByField("prev_close");

        Boolean gain = true;

        if(price < prevClose){
            gain = false;
        }

        collector.emit(new Values(symbol, timestamp, price, gain));
        writer.append(symbol + "," + timestamp + "," + price + "," + gain + "\n");
        writer.flush();
        try{
            writer.close();
        } catch (Exception e){
            System.out.println("### Failed close file:"+ filename);
        }


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("company","timestamp", "price", "gain"));
    }
}
