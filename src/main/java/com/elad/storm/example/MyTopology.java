package com.elad.storm.example;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by eladw on 12/21/17.
 * Sample topology reads from yahoo finanace
 */
public class MyTopology {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Yahoo-Finance-Spout", new YfSpout());
        builder.setBolt("Yahoo-Finance-Bolt", new YfBolt()).shuffleGrouping("Yahoo-Finance-Spout");

        StormTopology topology = builder.createTopology();

        Config conf = new Config();
        conf.setDebug(true); //see every emit in log
        conf.put("fileToWrite", "/Users/eladw/git-dp/storm-example/src/main/resources/output/sample-topology.txt");

        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("stock-Tracker-Topology", conf, topology);
            Thread.sleep(10000);

        } finally {
            cluster.shutdown();
        }


    }


}
