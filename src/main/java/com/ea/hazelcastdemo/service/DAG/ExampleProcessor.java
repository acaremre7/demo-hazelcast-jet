package com.ea.hazelcastdemo.service.DAG;

import com.hazelcast.jet.core.AbstractProcessor;

import java.util.Map;

public class ExampleProcessor extends AbstractProcessor {

    @Override
    protected boolean tryProcess0(Object item) throws Exception {
        ((Map.Entry)item).setValue(((Integer)((Map.Entry)item).getValue()) + 1000);
        return tryEmit(item);
    }
}
