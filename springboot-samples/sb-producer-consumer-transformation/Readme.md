# Dynamic JSON Transformation with Kafka Consumer

## Overview
This project demonstrates how to dynamically transform JSON messages consumed from Kafka before persisting them in a database. The transformation logic is compiled and executed at runtime using **Janino's SimpleCompiler**.

## Project Structure
We have two main components:
1. **Kafka Producer & Consumer Service** (Spring Boot-based application)
2. **Dynamic JSON Transformation Utility**

---

## 1. Kafka Consumer Service (Spring Boot)

### **Kafka Consumer Implementation**
- The consumer listens to the `employee-topic` and receives messages as `EmployeeDTO`.
- The received JSON is transformed dynamically before saving it to the database.
- The transformation logic is compiled at runtime using Janino.

### **Kafka Listener Code**
```java
@KafkaListener(topics = "employee-topic", groupId = "employee-group")
public void consume(EmployeeDTO employeeDTO) {
    System.out.println("Consumer received: " + employeeDTO);

    // Convert EmployeeDTO to JSON
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("name", employeeDTO.getName());
    jsonObject.put("department", new JSONObject().put("name", employeeDTO.getDepartment().getName()));

    // Dynamic transformation logic
    String transformationLogic = ""
            + "import org.json.JSONObject;"
            + "public class DynamicTransformer {"
            + "   public static JSONObject transform(JSONObject json) {"
            + "       json.put(\"name\", json.getString(\"name\").toUpperCase());"
            + "       json.getJSONObject(\"department\").put(\"name\", json.getJSONObject(\"department\").getString(\"name\").toUpperCase());"
            + "       return json;"
            + "   }"
            + "}";

    // Apply transformation
    JSONObject transformedJson = JsonTransformationUtil.transformJson(jsonObject, transformationLogic);

    // Extract transformed values
    String transformedName = transformedJson.getString("name");
    String transformedDeptName = transformedJson.getJSONObject("department").getString("name");

    // Save Department
    Department department = new Department();
    department.setName(transformedDeptName);
    department = departmentRepository.save(department);

    // Save Employee
    Employee employee = new Employee();
    employee.setName(transformedName);
    employee.setDepartment(department);
    employeeRepository.save(employee);

    System.out.println("Employee saved with transformed data: " + transformedName);
}
```

---

## 2. Dynamic JSON Transformation Utility

### **JsonTransformationUtil.java**
This utility dynamically compiles and executes JSON transformation logic.

```java
import org.json.JSONObject;
import org.codehaus.janino.SimpleCompiler;
import java.lang.reflect.Method;
import java.util.logging.Logger;

public class JsonTransformationUtil {
    private static final Logger logger = Logger.getLogger(JsonTransformationUtil.class.getName());

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
```

---

## Flow Diagram

1. **Kafka Producer** sends an `EmployeeDTO` message to the topic `employee-topic`.
2. **Kafka Consumer** listens to the topic and receives the message.
3. The **consumer converts the message to JSON**.
4. **Dynamic transformation logic** is applied to modify the JSON.
5. **Transformed data is extracted** from JSON.
6. **Department and Employee entities are created** and saved in the database.
7. Logging is used to track the flow of execution.

---

## Advantages of This Approach
✅ **Dynamic Logic Updates**: Modify transformations at runtime without changing consumer code.  
✅ **Flexible Transformations**: Convert fields, modify structures, or add computed values.  
✅ **Integration Ready**: The transformation logic can be stored in a database, fetched via API, or configured dynamically.  

---


---

## Conclusion
This project provides a **Kafka-based real-time data processing system** where transformation logic is **dynamically compiled** and applied at runtime. This allows for flexibility in modifying business logic without modifying the core application.


Kafka Setup Link:
https://medium.com/@erkndmrl/kafka-cluster-with-docker-compose-5864d50f677e

Maven Links :
https://mvnrepository.com/artifact/org.json/json/20240303
https://mvnrepository.com/artifact/org.codehaus.janino/janino/3.1.12

```


<!-- https://mvnrepository.com/artifact/org.json/json -->
<dependency>
    <groupId>org.json</groupId>
    <artifactId>json</artifactId>
    <version>20240303</version>
</dependency>


<!-- https://mvnrepository.com/artifact/org.codehaus.janino/janino -->
<dependency>
    <groupId>org.codehaus.janino</groupId>
    <artifactId>janino</artifactId>
    <version>3.1.12</version>
</dependency>

```

