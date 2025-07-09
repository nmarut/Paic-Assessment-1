package com.paic.assessment.marut.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;


@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "cdr_logs")
public class CdrLogsEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID")
    private Integer id;

    @Column(name = "FileName", nullable = false, length = 255)
    private String fileName;

    @Column(name = "UploadStartTime", nullable = false)
    private LocalDateTime uploadStartTime;

    @Column(name = "UploadEndTime")
    private LocalDateTime uploadEndTime;

    @Column(name = "SuccessfulRecords", nullable = false)
    private Integer successfulRecords = 0;

    @Column(name = "FailedRecords", nullable = false)
    private Integer failedRecords = 0;

}
