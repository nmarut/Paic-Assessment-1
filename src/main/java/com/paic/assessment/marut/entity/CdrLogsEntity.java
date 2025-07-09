package com.paic.assessment.marut.entity;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
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

    // Getters and Setters

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public LocalDateTime getUploadStartTime() {
        return uploadStartTime;
    }

    public void setUploadStartTime(LocalDateTime uploadStartTime) {
        this.uploadStartTime = uploadStartTime;
    }

    public LocalDateTime getUploadEndTime() {
        return uploadEndTime;
    }

    public void setUploadEndTime(LocalDateTime uploadEndTime) {
        this.uploadEndTime = uploadEndTime;
    }

    public Integer getSuccessfulRecords() {
        return successfulRecords;
    }

    public void setSuccessfulRecords(Integer successfulRecords) {
        this.successfulRecords = successfulRecords;
    }

    public Integer getFailedRecords() {
        return failedRecords;
    }

    public void setFailedRecords(Integer failedRecords) {
        this.failedRecords = failedRecords;
    }
}
