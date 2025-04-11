package com.example.controller;

import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.streams.KafkaStreamsInteractiveQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/machines")
public class MachineStatusController {

    private final KafkaStreamsInteractiveQueryService queryService;

    public MachineStatusController(KafkaStreamsInteractiveQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/{machineId}/status")
    public ResponseEntity<String> getStatus(@PathVariable String machineId) {
        ReadOnlyKeyValueStore<String, String> store =
                queryService.retrieveQueryableStore("machine-status-store", QueryableStoreTypes.keyValueStore());

        String status = store.get(machineId);
        return (status != null) ? ResponseEntity.ok(status) : ResponseEntity.notFound().build();
    }

//    http://localhost:8080/machines/MCH-1381/status
//    MAINTENANCE
}
