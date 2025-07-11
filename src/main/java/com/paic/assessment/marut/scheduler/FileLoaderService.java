package com.paic.assessment.marut.scheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.paic.assessment.marut.entity.CdrLogsEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.paic.assessment.marut.entity.CallDetailsRecordEntity;
import com.paic.assessment.marut.repository.CallDetailsRecordsRepository;
import com.paic.assessment.marut.repository.CdrLogsRepository;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class FileLoaderService {

    private final CdrLogsRepository cdrLogsRepository;
    
    private final ExecutorService executor = Executors.newFixedThreadPool(4);
	
	private String directoryPath = "C:\\files\\input";
	
	    @Autowired
	    private CallDetailsRecordsRepository callDetailsRepository;
	 
	    private static final String INPUT_DIR = "C:\\files\\input";
	    private static final String PROCESSED_DIR = "C:\\files\\processed";
	    private static final String ERROR_DIR = "CC:\\files\\error";

    FileLoaderService(CdrLogsRepository cdrLogsRepository) {
        this.cdrLogsRepository = cdrLogsRepository;
    }
	
//	@Scheduled(fixedRate  = 10000)
	public File fetchSingleCsvFile(String directoryPath) {
	    File dir = new File(directoryPath);
	 
	    if (dir.exists() && dir.isDirectory()) {
	    	File[] files = dir.listFiles(File::isFile);
	 
	        if (files != null && files.length > 0) {
	            Arrays.sort(files);
	            return files[0];
	        }
	    }
	    return null;
	}
	
	 
	   
	 	@Scheduled(fixedRate  = 60000)
	    public void processSingleCsvFile() {
	        File csvFile = fetchSingleCsvFile(INPUT_DIR);
	 		log.info("Inside process single Csv file");
	        if (csvFile == null) {
	            System.out.println("No CSV file found to process.");
	            return;
	        }
			CdrLogsEntity logEntry = new CdrLogsEntity();
			logEntry.setFileName(csvFile.getName());
			logEntry.setUploadStartTime(LocalDateTime.now());
			logEntry.setSuccessfulRecords(0);
			logEntry.setFailedRecords(0);
			logEntry = cdrLogsRepository.save(logEntry);

			int successfulCount = 0;
			int failedCount = 0;

	        try {

				successfulCount= processFile(csvFile);
	        	
	            moveFile(csvFile, PROCESSED_DIR);
	            System.out.println("Processed and moved file: " + csvFile.getName());
	        } catch (Exception e) {
				failedCount++;
	            e.printStackTrace();
	            moveFile(csvFile, ERROR_DIR);
	            System.err.println("Failed to process file. Moved to error: " + csvFile.getName());
	        }

			finally {

				logEntry.setUploadEndTime(LocalDateTime.now());
				logEntry.setSuccessfulRecords(successfulCount);
				logEntry.setFailedRecords(failedCount);
				cdrLogsRepository.save(logEntry);
			}
	    }
	 

	 
	    private void moveFile(File file, String targetDirectory) {

			log.info("Inside Move File Function");
	        try {
	            Files.createDirectories(Paths.get(targetDirectory));
	            Path targetPath = Paths.get(targetDirectory, file.getName());
	            Files.move(file.toPath(), targetPath, StandardCopyOption.REPLACE_EXISTING);
	        } catch (IOException e) {
	            System.err.println("Error moving file: " + e.getMessage());
	        }
	    }


	private int processFile(File file) {
		final int CHUNK_SIZE = 500;
		log.info("Inside process File Method");
		int totalRecords = 0;
		List<CallDetailsRecordEntity> chunk = new ArrayList<>(CHUNK_SIZE);
		List<Future<?>> futures = new ArrayList<>();

		try (BufferedReader br = new BufferedReader(new FileReader(file))) {
			String line;

			while ((line = br.readLine()) != null) {
				String[] values = line.split("\\|", -1);
				if (values.length < 31) {
					log.warn("Skipping line, not enough Values: {}", line);
					continue;
				}

				try {
					CallDetailsRecordEntity record = mapValuesToEntity(values);
					chunk.add(record);

					if (chunk.size() >= CHUNK_SIZE) {
						List<CallDetailsRecordEntity> batch = new ArrayList<>(chunk);
						futures.add(executor.submit(() -> callDetailsRepository.saveAll(batch)));
						totalRecords += batch.size();
						chunk.clear();
					}
				} catch (Exception e) {
					log.warn("Skipping bad line: {} → {}", line, e.getMessage());
				}
			}

			if (!chunk.isEmpty()) {
				List<CallDetailsRecordEntity> batch = new ArrayList<>(chunk);
				futures.add(executor.submit(() -> callDetailsRepository.saveAll(batch)));
				totalRecords += batch.size();
			}

			for (Future<?> f : futures) {
				f.get();
			}

		} catch (Exception e) {
			log.error("Failed to process file {}", file.getName(), e);
			throw new RuntimeException(e);
		}
		return totalRecords;
	}


	private CallDetailsRecordEntity mapValuesToEntity(String[] values) {
	        int idx = 0;
			log.info("Inside mapValuesToEntity Function");
	        CallDetailsRecordEntity record = new CallDetailsRecordEntity();
	        record.setRecordDate(parseDateTime(values[idx++]));
	        record.setLSpc(parseInt(values[idx++]));
	        record.setLSsn(parseInt(values[idx++]));
	        record.setLRi(parseInt(values[idx++]));
	        record.setLGtI(parseInt(values[idx++]));

	        record.setLGtDigits(values[idx++].trim());
	        record.setRSpc(parseInt(values[idx++]));
	        record.setRSsn(parseInt(values[idx++]));
	        record.setRRi(parseInt(values[idx++]));
	        record.setRGtI(parseInt(values[idx++]));
	        record.setRGtDigits(values[idx++].trim());
	        record.setServiceCode(values[idx++].trim());
	        record.setOrNature(parseInt(values[idx++]));
	        record.setOrPlan(parseInt(values[idx++]));
	        record.setOrDigits(values[idx++].trim());
	        record.setDeNature(parseInt(values[idx++]));
	        record.setDePlan(parseInt(values[idx++]));
	        record.setDeDigits(values[idx++].trim());
	        record.setIsdnNature(parseInt(values[idx++]));
	        record.setIsdnPlan(parseInt(values[idx++]));
	        record.setMsisdn(values[idx++].trim());
	        record.setVlrNature(parseInt(values[idx++]));
	        record.setVlrPlan(parseInt(values[idx++]));
			record.setImsi(values[idx++].trim());
	        record.setVlrDigits(values[idx++].trim());
	        record.setStatus(values[idx++].trim());
	        record.setType(values[idx++].trim());
	        record.setTstamp(parseDateTime(values[idx++]));
	        record.setLocalDialogId(parseLong(values[idx++]));
	        record.setRemoteDialogId(parseLong(values[idx++]));
	        record.setDialogDuration(parseLong(values[idx++]));
	        record.setUssdString(values[idx++].trim());
	        return record;
	    }




	    private Integer parseInt(String val) {
	        try { return (val == null || val.trim().isEmpty()) ? 0 : Integer.parseInt(val.trim()); }
	        catch (NumberFormatException e) { return 0; }
	    }

	    private Long parseLong(String val) {
	        try { return (val == null || val.trim().isEmpty()) ? 0L : Long.parseLong(val.trim()); }
	        catch (NumberFormatException e) { return 0L; }
	    }


	    private final DateTimeFormatter formatterComma = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");
	    private final DateTimeFormatter formatterDot = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

	    private LocalDateTime parseDateTime(String val) {
	        if (val == null || val.trim().isEmpty()) {
	            throw new IllegalArgumentException("Missing DateTime value!");
	        }
	        String trimmed = val.trim();
	        try {
	            return trimmed.contains(",")
	                ? LocalDateTime.parse(trimmed, formatterComma)
	                : LocalDateTime.parse(trimmed, formatterDot);
	        } catch (DateTimeParseException e) {
	            throw new IllegalArgumentException("Invalid DateTime format: " + trimmed);
	        }
	    }




	}
	
	
	

