package com.example.dto;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class MachineEvent {
    private String machineId;
    private String industry;
    private String location;
    private double temperature;
    private double vibration;
    private int intensity;
    private String status;
    private String timestamp;

}
