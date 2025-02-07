package com.example.kafka_consumer_sb3.consumer;

import com.example.kafka_consumer_sb3.entity.Department;
import com.example.kafka_consumer_sb3.entity.Employee;
import com.example.kafka_consumer_sb3.repository.DepartmentRepository;
import com.example.kafka_consumer_sb3.repository.EmployeeRepository;
import com.example.kafka_consumer_sb3.util.JsonTransformationUtil;
import com.example.kafka_producer_sb3.dto.DepartmentDTO;
import com.example.kafka_producer_sb3.dto.EmployeeDTO;
import org.json.JSONObject;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class EmployeeConsumer {
    private final EmployeeRepository employeeRepository;
    private final DepartmentRepository departmentRepository;

    public EmployeeConsumer(EmployeeRepository employeeRepository, DepartmentRepository departmentRepository) {
        this.employeeRepository = employeeRepository;
        this.departmentRepository = departmentRepository;
    }


    @KafkaListener(topics = "employee-topic", groupId = "employee-group")
    public void consume(EmployeeDTO employeeDTO) {
        System.out.println("Consumer received: " + employeeDTO);

        // Convert EmployeeDTO to JSON
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", employeeDTO.getName());
        jsonObject.put("department", new JSONObject().put("name", employeeDTO.getDepartment().getName()));

        // Dynamic transformation logic (Can be loaded from DB or API)
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

        // Extract transformed data
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

//        DepartmentDTO department1 = employeeDTO.getDepartment();
//        Department departmentTemp=
//                new Department();
//        departmentTemp .setName(department1.getName().toUpperCase());
//                Department department = departmentRepository.save(departmentTemp);
//
//        Employee employee = new Employee();
//        employee.setName(employeeDTO.getName());
//        employee.setDepartment(department);
//
//        employeeRepository.save(employee);
//        System.out.println("Employee saved: " + employeeDTO.getName());

















    }
}
