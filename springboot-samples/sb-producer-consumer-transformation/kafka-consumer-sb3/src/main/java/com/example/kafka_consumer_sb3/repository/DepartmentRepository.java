package com.example.kafka_consumer_sb3.repository;

import com.example.kafka_consumer_sb3.entity.Department;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DepartmentRepository extends JpaRepository<Department, Long> {
}
