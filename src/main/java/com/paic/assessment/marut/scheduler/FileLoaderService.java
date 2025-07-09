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
	 
	        if (csvFile == null) {
	            System.out.println("No CSV file found to process.");
	            return;
	        }
	 
	        try {

	        	processPipeSeparatedFile(csvFile);
	        	
	            moveFile(csvFile, PROCESSED_DIR);
	            System.out.println("Processed and moved file: " + csvFile.getName());
	        } catch (Exception e) {
	            e.printStackTrace();
	            moveFile(csvFile, ERROR_DIR);
	            System.err.println("Failed to process file. Moved to error: " + csvFile.getName());
	        }
	    }
	 

	 
	    private void moveFile(File file, String targetDirectory) {
	        try {
	            Files.createDirectories(Paths.get(targetDirectory));
	            Path targetPath = Paths.get(targetDirectory, file.getName());
	            Files.move(file.toPath(), targetPath, StandardCopyOption.REPLACE_EXISTING);
	        } catch (IOException e) {
	            System.err.println("Error moving file: " + e.getMessage());
	        }
	    }
	    
	    
	    private void processPipeSeparatedFile(File file) {
	        final int CHUNK_SIZE = 1000;
	        List<CallDetailsRecordEntity> chunk = new ArrayList<>(CHUNK_SIZE);
	        List<Future<?>> futures = new ArrayList<>();

	        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
	            String line;

	            while ((line = br.readLine()) != null) {
	                String[] tokens = line.split("\\|", -1);
	                if (tokens.length < 31) {
	                    log.warn("Skipping line, not enough tokens: {}", line);
	                    continue;
	                }

	                try {
	                    CallDetailsRecordEntity record = mapTokensToEntity(tokens);
	                    chunk.add(record);

	                    if (chunk.size() >= CHUNK_SIZE) {
	                        List<CallDetailsRecordEntity> batch = new ArrayList<>(chunk);
	                        futures.add(executor.submit(() -> callDetailsRepository.saveAll(batch)));
	                        chunk.clear();
	                    }
	                } catch (Exception e) {
	                    log.warn("Skipping bad line: {} → {}", line, e.getMessage());
	                }
	            }

	            if (!chunk.isEmpty()) {
	                List<CallDetailsRecordEntity> batch = new ArrayList<>(chunk);
	                futures.add(executor.submit(() -> callDetailsRepository.saveAll(batch)));
	            }

	            for (Future<?> f : futures) {
	                f.get();
	            }

	        } catch (Exception e) {
	            log.error("Failed to process file {}", file.getName(), e);
	            throw new RuntimeException(e);
	        }
	    }
	    
	    private CallDetailsRecordEntity mapTokensToEntity(String[] tokens) {
	        int idx = 0;
	        CallDetailsRecordEntity record = new CallDetailsRecordEntity();
	        record.setRecordDate(parseDateTime(tokens[idx++]));
	        record.setLSpc(parseInt(tokens[idx++]));
	        record.setLSsn(parseInt(tokens[idx++]));
	        record.setLRi(parseInt(tokens[idx++]));
	        record.setLGtI(parseInt(tokens[idx++]));
	        record.setLGtDigits(tokens[idx++].trim());
	        record.setRSpc(parseInt(tokens[idx++]));
	        record.setRSsn(parseInt(tokens[idx++]));
	        record.setRRi(parseInt(tokens[idx++]));
	        record.setRGtI(parseInt(tokens[idx++]));
	        record.setRGtDigits(tokens[idx++].trim());
	        record.setServiceCode(tokens[idx++].trim());
	        record.setOrNature(parseInt(tokens[idx++]));
	        record.setOrPlan(parseInt(tokens[idx++]));
	        record.setOrDigits(tokens[idx++].trim());
	        record.setDeNature(parseInt(tokens[idx++]));
	        record.setDePlan(parseInt(tokens[idx++]));
	        record.setDeDigits(tokens[idx++].trim());
	        record.setIsdnNature(parseInt(tokens[idx++]));
	        record.setIsdnPlan(parseInt(tokens[idx++]));
	        record.setMsisdn(tokens[idx++].trim());
	        record.setVlrNature(parseInt(tokens[idx++]));
	        record.setVlrPlan(parseInt(tokens[idx++]));
	        record.setVlrDigits(tokens[idx++].trim());
	        record.setImsi(tokens[idx++].trim());
	        record.setStatus(tokens[idx++].trim());
	        record.setType(tokens[idx++].trim());
	        record.setTstamp(parseDateTime(tokens[idx++]));
	        record.setLocalDialogId(parseLong(tokens[idx++]));
	        record.setRemoteDialogId(parseLong(tokens[idx++]));
	        record.setDialogDuration(parseLong(tokens[idx++]));
	        record.setUssdString(tokens[idx++].trim());
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
	
	
	

