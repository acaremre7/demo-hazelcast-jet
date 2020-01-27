package com.ea.hazelcastdemo.service;

import com.ea.hazelcastdemo.service.DAG.ExampleProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;

public class DAGFactory {

    private DAGFactory(){}

    public static DAG getDAG(){
        DAG dag = new DAG();
        PipelineImpl pipeline = new PipelineImpl();
        Vertex v1 = dag.newVertex("source", SourceProcessors.readMapP("integerIMap"));
        Vertex v2 = dag.newVertex("process", ExampleProcessor::new);
        Vertex v3 = dag.newVertex("printer", SinkProcessors.writeMapP("integerIMap"));
        dag.edge(Edge.between(v1,v2)).edge(Edge.between(v2,v3));
        return dag;
    }
}
