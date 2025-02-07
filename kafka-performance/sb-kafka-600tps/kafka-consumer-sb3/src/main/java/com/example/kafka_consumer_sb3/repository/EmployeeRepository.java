package com.example.kafka_consumer_sb3.repository;

import com.example.kafka_consumer_sb3.entity.Employee;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EmployeeRepository extends JpaRepository<Employee, Long> {
}
