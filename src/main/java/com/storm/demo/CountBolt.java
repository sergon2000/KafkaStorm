package com.storm.demo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CountBolt implements IRichBolt{
    Map<String, Integer> counters;
    private OutputCollector collector;
    private FileWriter fileWriter;
    private BufferedWriter bufferedWriter;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = collector;
        try {
            fileWriter = new FileWriter("/home/ec2-user/KafkaStormOutput.log", true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);

        if(!counters.containsKey(str)){
            counters.put(str, 1);
        }else {
            Integer c = counters.get(str) +1;
            counters.put(str, c);
        }

        try {
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(str + " : " + counters.get(str));
            bufferedWriter.newLine();
            bufferedWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        for(Map.Entry<String, Integer> entry:counters.entrySet()){
            System.out.println(entry.getKey()+" : " + entry.getValue());
        }

        collector.ack(input);
    }

    @Override
    public void cleanup() {
        try {
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}