package com.elad.storm.reliablilty;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Created by eladw on 12/31/17.
 */
public class ToplogyReliableMain {

    public static void main(String[] args) throws InterruptedException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("Reliable-Spout", new ReliableWordReader());
        builder.setBolt("Random-Failure-Bolt",
                new RandomFailureBolt()).shuffleGrouping("Reliable-Spout");


        Config config = new Config();
        config.setDebug(true);
        config.put("fileToRead", "/Users/eladw/git-dp/StormExample/src/main/resources/sample.txt");


        LocalCluster cluster = new LocalCluster();
        try{
            cluster.submitTopology("Random-File-Topology", config, builder.createTopology());
            Thread.sleep(10000000);
        } finally {
            cluster.shutdown();
        }

    }

}
