package com.storm.example;

import org.apache.storm.StormSubmitter;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

//import storm configuration packages
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

//Create main class LogAnalyserStorm submit topology.
public class LogAnalyserStorm {
    public static void main(String[] args) throws Exception{
        //Create Config instance for cluster configuration
        Config config = new Config();
        config.setDebug(true);

        //
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());

        builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt())
                .shuffleGrouping("call-log-reader-spout");

        builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt())
                .fieldsGrouping("call-log-creator-bolt", new Fields("call"));

        //System.setProperty("storm.jar", "/home/ec2-user/KafkaStorm.jar");
        StormSubmitter.submitTopology("KafkaStorm", config, builder.createTopology());
    }
}