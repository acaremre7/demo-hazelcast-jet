package com.ea.hazelcastdemo.service;

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.DAG;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class OperatorService implements CommandLineRunner {
    private JetInstance instance;

    @Override
    public void run(String... args) {
        System.out.println("Select \"0\" for batch operations, \"1\" for stream operations, \"2\" for DAG operations.");
        Scanner sc = new Scanner(System.in);
        switch (sc.next()) {
            case "0":
                batchOperate();
                break;
            case "1":
                streamOperate();
                break;
            case "2":
                dagOperate();
                break;
            default:
                run();
                break;
        }
    }

    private void batchOperate() {
        //Init jet instance
        instance = Jet.newJetInstance(new JetConfig().setHazelcastConfig(new Config()));
        try {
            fillMap(1_000_000);
            //Attaching jobs
            instance.newJob(PipelineFactory.getBatchPipeline1()).join();
            instance.newJob(PipelineFactory.getBatchPipeline2()).join();
            //Printing results
            System.out.println("---------------- Result 1 : " + (instance.getList("result1").isEmpty() ? 0 : (Long) instance.getList("result1").get(0)));
            System.out.println("---------------- Result 2 : " + (instance.getList("result2").isEmpty() ? 0 : (Long) instance.getList("result2").get(0)));
        } finally {
            instance.shutdown();
        }
    }

    private void streamOperate() {
        //Init jet instance
        JetConfig jetConfig = new JetConfig();
        Config config = new Config();
        EventJournalConfig eventJournalMapConfig = new EventJournalConfig()
                .setEnabled(true)
                .setCapacity(5000)
                .setTimeToLiveSeconds(20);
        Map<String, EventJournalConfig> eventJournalConfigMap = new HashMap<>();
        eventJournalConfigMap.put("integerIMap", eventJournalMapConfig);
        config.setMapEventJournalConfigs(eventJournalConfigMap);
        jetConfig.setHazelcastConfig(config);
        instance = Jet.newJetInstance(jetConfig);
        //Create threads
        Thread jobThread = new Thread(() -> {
            instance.newJob(PipelineFactory.getStreamPipeline()).join();
        });
        Thread printThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("Total element count in input map --->" + instance.getMap("integerIMap").size() + "  Current processed element count in output map --->" + instance.getMap("result3").size());
            }
        });
        //Run them
        jobThread.start();
        printThread.start();
        fillMap(1_000_000);
    }

    private void dagOperate(){
        instance = Jet.newJetInstance();
        fillMap(100);
        try {
            instance.newJob(DAGFactory.getDAG()).join();
            Map<Integer,Integer> resultMap = instance.getMap("integerIMap");
            for(int i : resultMap.values()){
                System.out.println(i);
            }
        }finally {
            instance.shutdown();
        }
    }

    public void fillMap(int size) {
        //Creating integer map with random values
        Map<Integer, Integer> integerMap = instance.getMap("integerIMap");
        Random rand = new Random();
        for (int i = 0; i < size; i++) {
            integerMap.put(i, rand.nextInt(100));
        }
    }
}
