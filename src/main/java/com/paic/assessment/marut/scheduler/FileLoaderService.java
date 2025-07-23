package com.paic.assessment.marut.scheduler;

import com.paic.assessment.marut.entity.CallDetailsRecordEntity;
import com.paic.assessment.marut.entity.CdrLogsEntity;
import com.paic.assessment.marut.repository.CallDetailsRecordsRepository;
import com.paic.assessment.marut.repository.CdrLogsRepository;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.*;

@Service
@Slf4j
public class FileLoaderService {

    private final CdrLogsRepository cdrLogsRepository;
    private final CallDetailsRecordsRepository callDetailsRepository;
    private final ExecutorService executor = Executors.newFixedThreadPool(4);

    @Value("${app.input-dir}")
    private String inputDir;

    @Value("${app.processed-dir}")
    private String processedDir;

    @Value("${app.error-dir}")
    private String errorDir;

    @Value("${app.date-format-comma}")
    private String dateFormatComma;

    @Value("${app.date-format-dot}")
    private String dateFormatDot;

    private DateTimeFormatter formatterComma;
    private DateTimeFormatter formatterDot;

    public FileLoaderService(CdrLogsRepository cdrLogsRepository,
                             CallDetailsRecordsRepository callDetailsRepository) {
        this.cdrLogsRepository = cdrLogsRepository;
        this.callDetailsRepository = callDetailsRepository;
    }

    @PostConstruct
    public void initFormatters() {
        formatterComma = DateTimeFormatter.ofPattern(dateFormatComma);
        formatterDot = DateTimeFormatter.ofPattern(dateFormatDot);
    }

    @Scheduled(fixedRate = 60000)
    public void processSingleCsvFile() {
        File file = fetchSingleCsvFile(inputDir);
        log.info("Inside processSingleCsvFile");

        if (file == null) {
            log.info("No CSV file found to process.");
            return;
        }

        processAndLogFile(file);
    }

    private void processAndLogFile(File file) {
        CdrLogsEntity logEntry = new CdrLogsEntity();
        logEntry.setFileName(file.getName());
        logEntry.setUploadStartTime(LocalDateTime.now());
        logEntry.setSuccessfulRecords(0);
        logEntry.setFailedRecords(0);
        logEntry = cdrLogsRepository.save(logEntry);

        int successfulCount = 0;
        int failedCount = 0;

        try {
            successfulCount = processFile(file);
            moveFile(file, processedDir);
            log.info("Processed and moved file: {}", file.getName());
        } catch (Exception e) {
            failedCount++;
            log.error("Failed to process file: {}", file.getName(), e);
            moveFile(file, errorDir);
        } finally {
            logEntry.setUploadEndTime(LocalDateTime.now());
            logEntry.setSuccessfulRecords(successfulCount);
            logEntry.setFailedRecords(failedCount);
            cdrLogsRepository.save(logEntry);
        }
    }

    private File fetchSingleCsvFile(String directoryPath) {
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

    private void moveFile(File file, String targetDirectory) {
        try {
            Files.createDirectories(Paths.get(targetDirectory));
            Path targetPath = Paths.get(targetDirectory, file.getName());
            Files.move(file.toPath(), targetPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            log.error("Error moving file: {}", e.getMessage(), e);
        }
    }

    private int processFile(File file) {
        final int CHUNK_SIZE = 500;
        int totalRecords = 0;
        List<CallDetailsRecordEntity> chunk = new ArrayList<>(CHUNK_SIZE);
        List<Future<?>> futures = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;

            while ((line = br.readLine()) != null) {
                String[] values = line.split("\\|", -1);
                if (values.length < 31) {
                    log.warn("Skipping line, not enough values: {}", line);
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

    private Integer parseInt(String val) {
        try {
            return (val == null || val.trim().isEmpty()) ? 0 : Integer.parseInt(val.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    private Long parseLong(String val) {
        try {
            return (val == null || val.trim().isEmpty()) ? 0L : Long.parseLong(val.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
