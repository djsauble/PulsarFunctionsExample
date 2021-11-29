package com.datastax;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class PredictLabelsFunction implements Function<String, String> {
        @Override
        public String process(String input, Context ctx) throws Exception {
                return String.format("%s!", input);
        }
}
