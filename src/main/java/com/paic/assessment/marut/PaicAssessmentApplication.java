package com.paic.assessment.marut;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class PaicAssessmentApplication {

	public static void main(String[] args) {
		SpringApplication.run(PaicAssessmentApplication.class, args);
	}

}
