package com.paic.assessment.marut.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.paic.assessment.marut.entity.CdrLogsEntity;

public interface CdrLogsRepository extends JpaRepository<CdrLogsEntity, Integer> {

}
