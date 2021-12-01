import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.nio.ByteBuffer;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.dmg.pmml.DataType;
import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.ModelEvaluatorBuilder;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorBuilder;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.InputField;
import org.jpmml.evaluator.LoadingModelEvaluatorBuilder;
import org.jpmml.evaluator.TargetField;
import org.jpmml.model.PMMLUtil;

public class RegressionFunction implements Function<String, String> {

        private static final String MODEL_KEY = "ML-Model";

        @Override
        public String process(String input, Context ctx) throws Exception {

                ByteBuffer state = ctx.getState(MODEL_KEY);

                if (state == null) {
                        return "No model has been uploaded";
                }
                else {
                        byte[] mlModel = state.array();
                        ByteArrayInputStream stream = new ByteArrayInputStream(mlModel);
                        try {
                                // Build and verify the model
                                EvaluatorBuilder builder = new ModelEvaluatorBuilder(PMMLUtil.unmarshal(stream));
                                Evaluator evaluator = builder.build();
                                evaluator.verify();

                                // Get a list of features and target variables from the model
                                List<? extends InputField> inputFields = evaluator.getInputFields();
                                List<? extends TargetField> targetFields = evaluator.getTargetFields();

                                // Construct the input to the model
                                Map<FieldName, FieldValue> featureVector = new LinkedHashMap<>();
                                InputField inputField = inputFields.get(0);
                                featureVector.put(inputField.getName(), inputField.prepare(input));

                                // Evaluate the model on the inputs
                                Map<FieldName, ?> results = evaluator.evaluate(featureVector);

                                // Extract the prediction
                                FieldName targetField = targetFields.get(0).getName();
                                return results.get(targetField).toString();

                        }
                        catch (Exception exception) {
                                return exception.getMessage();
                        }
                }
        }

        public static void main(String[] args) {
                try {
                        Evaluator evaluator = new LoadingModelEvaluatorBuilder()
                                .load(new File("LogisticRegression.pmml"))
                                .build();
                        
                        evaluator.verify();

                        // Get a list of features and target variables from the model
                        List<? extends InputField> inputFields = evaluator.getInputFields();
                        List<? extends TargetField> targetFields = evaluator.getTargetFields();

                        // Construct the input to the model
                        Map<FieldName, FieldValue> featureVector = new LinkedHashMap<>();
                        InputField inputField = inputFields.get(0);
                        featureVector.put(inputField.getName(), inputField.prepare("fox woke chicken"));

                        // Evaluate the model on the inputs
                        Map<FieldName, ?> results = evaluator.evaluate(featureVector);

                        // Extract the prediction
                        FieldName targetField = targetFields.get(0).getName();
                        System.out.println(results.get(targetField).toString());
                }
                catch (Exception e) {
                        System.out.println(e.getMessage());
                }
        }
}