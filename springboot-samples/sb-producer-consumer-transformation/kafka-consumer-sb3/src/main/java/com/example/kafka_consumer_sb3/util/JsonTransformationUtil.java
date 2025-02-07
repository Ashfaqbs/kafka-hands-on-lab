package com.example.kafka_consumer_sb3.util;


import org.json.JSONObject;
import org.codehaus.janino.SimpleCompiler;
import java.lang.reflect.Method;
import java.util.logging.Logger;

public class JsonTransformationUtil {
    private static final Logger logger = Logger.getLogger(JsonTransformationUtil.class.getName());

    static final String transformationLogic = ""
            + "import org.json.JSONObject;"
            + "public class DynamicTransformer {"
            + "   public static JSONObject transform(JSONObject json) {"
            + "       json.put(\"name\", json.getString(\"name\").toUpperCase());"
            + "       json.put(\"age\", json.getInt(\"age\") + 1);"
            + "       return json;"
            + "   }"
            + "}";



    public static JSONObject transformJson(JSONObject jsonObject, String transformationLogic) {
        try {
            // Compile the provided logic using Janino
            SimpleCompiler compiler = new SimpleCompiler();
            compiler.cook(transformationLogic);

            // Load the dynamically compiled class
            Class<?> transformerClass = compiler.getClassLoader().loadClass("DynamicTransformer");
            Method transformMethod = transformerClass.getMethod("transform", JSONObject.class);

            // Invoke the transformation logic
            return (JSONObject) transformMethod.invoke(null, jsonObject);

        } catch (Exception e) {
            logger.severe("Transformation error: " + e.getMessage());
            return jsonObject; // Return the original JSON if transformation fails
        }
    }
}
