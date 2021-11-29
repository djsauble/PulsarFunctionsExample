package com.datastax;

/*import java.io.ByteArrayInputStream;
import java.util.HashMap;*/

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
/*import org.dmg.pmml.FieldName;*/
/*import org.dmg.pmml.regression.RegressionModel;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.regression.RegressionModelEvaluator;
import org.jpmml.model.PMMLUtil;*/

/* Logger for debugging purposes */
/*import org.slf4j.Logger;*/

public class PredictLabelsFunction implements Function<String, String> {

        /*private static final String MODEL_KEY = "ML-Model";

        private byte[] mlModel = null;
        private ModelEvaluator<RegressionModel> evaluator;*/
	/*Logger LOG;*/

        @Override
        public String process(String input, Context ctx) throws Exception {
                /*LOG = ctx.getLogger();*/

                /*LOG.info("[MODEL] Processing message...");*/

                /*ctx.publish("persistent://public/functions/test-out", input);*/

                /*if (initalized()) {
                  mlModel = ctx.getState(MODEL_KEY).array(); //
                  evaluator = new RegressionModelEvaluator(
                        PMMLUtil.unmarshal(new ByteArrayInputStream(mlModel)));  //
                }*/

                /*LOG.info(input);*/

                /* Long travel = (Long)evaluator.evaluate(featureVector)
                                .get(FieldName.create("travel_time"));

                order.setEstimatedArrival(System.currentTimeMillis() + travel);  */
                return String.format("%s!", input);
        }

        /*private final boolean initalized() {
                return (mlModel != null);
        }*/
}
