//package com.datastax;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import org.dmg.pmml.FieldName;
import org.dmg.pmml.Model;
import org.dmg.pmml.regression.RegressionModel;
import org.dmg.pmml.regression.RegressionTable;
import org.dmg.pmml.regression.NumericPredictor;
import org.jpmml.evaluator.ModelEvaluator;
import org.jpmml.evaluator.ModelEvaluatorBuilder;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.EvaluatorBuilder;
import org.jpmml.evaluator.FieldValue;
import org.jpmml.evaluator.InputField;
import org.jpmml.evaluator.TargetField;
import org.jpmml.evaluator.regression.RegressionModelEvaluator;
import org.jpmml.model.PMMLUtil;

//import org.dmg.pmml.mining.MiningModel;
//import org.jpmml.evaluator.mining.MiningModelEvaluator;

public class PredictLabelsFunction implements Function<String, String> {

        private static final String MODEL_KEY = "ML-Model";

        @Override
        public String process(String input, Context ctx) throws Exception {

                ByteBuffer state = ctx.getState(MODEL_KEY);

                if (state == null) {
                        return "state is null";
                }
                else {
                        byte[] mlModel = state.array();
                        ByteArrayInputStream stream = new ByteArrayInputStream(mlModel);
                        try {
                                EvaluatorBuilder builder = new ModelEvaluatorBuilder(PMMLUtil.unmarshal(stream));
                                Evaluator evaluator = builder.build();

                                List<? extends InputField> inputFields = evaluator.getInputFields();
                                List<? extends TargetField> targetFields = evaluator.getTargetFields();

                                // Create the feature vector from our input
                                Map<FieldName, FieldValue> featureVector = new LinkedHashMap<>();
                                for (InputField inputField : inputFields) {
                                        FieldName fieldName = inputField.getName();
                                        FieldValue fieldValue = inputField.prepare(Double.valueOf(input));
                                        featureVector.put(fieldName, fieldValue);
                                }

                                // Verify the model
                                evaluator.verify();

                                // Evaluate the model against the feature vector
                                Map<FieldName, ?> results = evaluator.evaluate(featureVector);

                                // Extract the target variable
                                for (TargetField targetField : targetFields) {
                                        FieldName targetName = targetField.getName();

                                        Object value = results.get(targetName);

                                        return Double.toString((Double)value);
                                }

                        }
                        catch (Exception exception) {
                                return exception.getMessage();
                        }

                        return "Something went wrong";
                }
        }

        public static void main(String[] args) {
                byte[] mlModel = {60, 63, 120, 109, 108, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 48, 34, 32, 101, 110, 99, 111, 100, 105, 110, 103, 61, 34, 85, 84, 70, 45, 56, 34, 32, 115, 116, 97, 110, 100, 97, 108, 111, 110, 101, 61, 34, 121, 101, 115, 34, 63, 62, 10, 60, 80, 77, 77, 76, 32, 120, 109, 108, 110, 115, 61, 34, 104, 116, 116, 112, 58, 47, 47, 119, 119, 119, 46, 100, 109, 103, 46, 111, 114, 103, 47, 80, 77, 77, 76, 45, 52, 95, 52, 34, 32, 120, 109, 108, 110, 115, 58, 100, 97, 116, 97, 61, 34, 104, 116, 116, 112, 58, 47, 47, 106, 112, 109, 109, 108, 46, 111, 114, 103, 47, 106, 112, 109, 109, 108, 45, 109, 111, 100, 101, 108, 47, 73, 110, 108, 105, 110, 101, 84, 97, 98, 108, 101, 34, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 52, 46, 52, 34, 62, 10, 9, 60, 72, 101, 97, 100, 101, 114, 62, 10, 9, 9, 60, 65, 112, 112, 108, 105, 99, 97, 116, 105, 111, 110, 32, 110, 97, 109, 101, 61, 34, 74, 80, 77, 77, 76, 45, 83, 107, 76, 101, 97, 114, 110, 34, 32, 118, 101, 114, 115, 105, 111, 110, 61, 34, 49, 46, 54, 46, 51, 51, 34, 47, 62, 10, 9, 9, 60, 84, 105, 109, 101, 115, 116, 97, 109, 112, 62, 50, 48, 50, 49, 45, 49, 49, 45, 51, 48, 84, 48, 49, 58, 53, 51, 58, 48, 56, 90, 60, 47, 84, 105, 109, 101, 115, 116, 97, 109, 112, 62, 10, 9, 60, 47, 72, 101, 97, 100, 101, 114, 62, 10, 9, 60, 77, 105, 110, 105, 110, 103, 66, 117, 105, 108, 100, 84, 97, 115, 107, 62, 10, 9, 9, 60, 69, 120, 116, 101, 110, 115, 105, 111, 110, 32, 110, 97, 109, 101, 61, 34, 114, 101, 112, 114, 34, 62, 80, 77, 77, 76, 80, 105, 112, 101, 108, 105, 110, 101, 40, 115, 116, 101, 112, 115, 61, 91, 40, 39, 114, 101, 103, 114, 101, 115, 115, 105, 111, 110, 39, 44, 32, 76, 105, 110, 101, 97, 114, 82, 101, 103, 114, 101, 115, 115, 105, 111, 110, 40, 41, 41, 93, 41, 60, 47, 69, 120, 116, 101, 110, 115, 105, 111, 110, 62, 10, 9, 60, 47, 77, 105, 110, 105, 110, 103, 66, 117, 105, 108, 100, 84, 97, 115, 107, 62, 10, 9, 60, 68, 97, 116, 97, 68, 105, 99, 116, 105, 111, 110, 97, 114, 121, 62, 10, 9, 9, 60, 68, 97, 116, 97, 70, 105, 101, 108, 100, 32, 110, 97, 109, 101, 61, 34, 121, 34, 32, 111, 112, 116, 121, 112, 101, 61, 34, 99, 111, 110, 116, 105, 110, 117, 111, 117, 115, 34, 32, 100, 97, 116, 97, 84, 121, 112, 101, 61, 34, 100, 111, 117, 98, 108, 101, 34, 47, 62, 10, 9, 9, 60, 68, 97, 116, 97, 70, 105, 101, 108, 100, 32, 110, 97, 109, 101, 61, 34, 120, 49, 34, 32, 111, 112, 116, 121, 112, 101, 61, 34, 99, 111, 110, 116, 105, 110, 117, 111, 117, 115, 34, 32, 100, 97, 116, 97, 84, 121, 112, 101, 61, 34, 100, 111, 117, 98, 108, 101, 34, 47, 62, 10, 9, 60, 47, 68, 97, 116, 97, 68, 105, 99, 116, 105, 111, 110, 97, 114, 121, 62, 10, 9, 60, 82, 101, 103, 114, 101, 115, 115, 105, 111, 110, 77, 111, 100, 101, 108, 32, 102, 117, 110, 99, 116, 105, 111, 110, 78, 97, 109, 101, 61, 34, 114, 101, 103, 114, 101, 115, 115, 105, 111, 110, 34, 32, 97, 108, 103, 111, 114, 105, 116, 104, 109, 78, 97, 109, 101, 61, 34, 115, 107, 108, 101, 97, 114, 110, 46, 108, 105, 110, 101, 97, 114, 95, 109, 111, 100, 101, 108, 46, 95, 98, 97, 115, 101, 46, 76, 105, 110, 101, 97, 114, 82, 101, 103, 114, 101, 115, 115, 105, 111, 110, 34, 62, 10, 9, 9, 60, 77, 105, 110, 105, 110, 103, 83, 99, 104, 101, 109, 97, 62, 10, 9, 9, 9, 60, 77, 105, 110, 105, 110, 103, 70, 105, 101, 108, 100, 32, 110, 97, 109, 101, 61, 34, 121, 34, 32, 117, 115, 97, 103, 101, 84, 121, 112, 101, 61, 34, 116, 97, 114, 103, 101, 116, 34, 47, 62, 10, 9, 9, 9, 60, 77, 105, 110, 105, 110, 103, 70, 105, 101, 108, 100, 32, 110, 97, 109, 101, 61, 34, 120, 49, 34, 47, 62, 10, 9, 9, 60, 47, 77, 105, 110, 105, 110, 103, 83, 99, 104, 101, 109, 97, 62, 10, 9, 9, 60, 82, 101, 103, 114, 101, 115, 115, 105, 111, 110, 84, 97, 98, 108, 101, 32, 105, 110, 116, 101, 114, 99, 101, 112, 116, 61, 34, 49, 53, 50, 46, 57, 49, 56, 56, 54, 49, 56, 50, 54, 49, 54, 49, 54, 55, 34, 62, 10, 9, 9, 9, 60, 78, 117, 109, 101, 114, 105, 99, 80, 114, 101, 100, 105, 99, 116, 111, 114, 32, 110, 97, 109, 101, 61, 34, 120, 49, 34, 32, 99, 111, 101, 102, 102, 105, 99, 105, 101, 110, 116, 61, 34, 57, 51, 56, 46, 50, 51, 55, 56, 54, 49, 50, 53, 49, 50, 54, 51, 55, 34, 47, 62, 10, 9, 9, 60, 47, 82, 101, 103, 114, 101, 115, 115, 105, 111, 110, 84, 97, 98, 108, 101, 62, 10, 9, 60, 47, 82, 101, 103, 114, 101, 115, 115, 105, 111, 110, 77, 111, 100, 101, 108, 62, 10, 60, 47, 80, 77, 77, 76, 62, 10};

                ByteArrayInputStream stream = new ByteArrayInputStream(mlModel);

                try {
                        // Use with LinearRegression.pmml
                        EvaluatorBuilder builder = new ModelEvaluatorBuilder(PMMLUtil.unmarshal(stream));
                        Evaluator evaluator = builder.build();

                        //ModelEvaluator<RegressionModel> evaluator = new RegressionModelEvaluator(PMMLUtil.unmarshal(stream));

                        List<? extends InputField> inputFields = evaluator.getInputFields();
                        List<? extends TargetField> targetFields = evaluator.getTargetFields();

                        // Create the feature vector from our input
                        Map<FieldName, FieldValue> featureVector = new LinkedHashMap<>();
                        for (InputField inputField : inputFields) {
                                FieldName fieldName = inputField.getName();
                                FieldValue fieldValue = inputField.prepare(Double.valueOf("-0.03961813"));
                                featureVector.put(fieldName, fieldValue);
                        }

                        // Verify the model
                        evaluator.verify();

                        // Evaluate the model against the feature vector
                        Map<FieldName, ?> results = evaluator.evaluate(featureVector);

                        // Extract the target variable
                        for (TargetField targetField : targetFields) {
                                FieldName targetName = targetField.getName();

                                Object value = results.get(targetName);

                                System.out.println((String)value);
                        }
                }
                catch (Exception e) {
                        System.out.println(e.getMessage());
                }
        }
}