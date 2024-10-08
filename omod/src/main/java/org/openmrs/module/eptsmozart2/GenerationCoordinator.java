package org.openmrs.module.eptsmozart2;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.openmrs.User;
import org.openmrs.api.context.Context;
import org.openmrs.module.eptsmozart2.api.EPTSMozART2GenerationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 8/17/22.
 */
@Component("eptsmozart2.generationCoordinator")
public class GenerationCoordinator implements Observer {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private Mozart2Generation generationRecord = null;
    private Exception generationException;

    @Autowired
    private EPTSMozART2GenerationService moz2GenService;

    public static final GeneratorTask GENERATOR_TASK = new GeneratorTask();
    public static final Map<String, StatusInfo> INITIAL_STATUSES = new LinkedHashMap<>();
    private static ExecutorService SINGLE_THREAD_EXECUTOR = Executors.newSingleThreadExecutor();

    static {
        INITIAL_STATUSES.put("patient", new StatusInfo("patient", 0, 0));
        INITIAL_STATUSES.put("location", new StatusInfo("location", 0, 0));
        INITIAL_STATUSES.put("patient_state", new StatusInfo("patient_state", 0, 0));
        INITIAL_STATUSES.put("form", new StatusInfo("form", 0, 0));
        INITIAL_STATUSES.put("identifier", new StatusInfo("identifier", 0, 0));
        INITIAL_STATUSES.put("key_vulnerable_pop", new StatusInfo("key_vulnerable_pop", 0, 0));
        INITIAL_STATUSES.put("observation", new StatusInfo("observation", 0, 0));
        INITIAL_STATUSES.put("dsd_supportgroup", new StatusInfo("dsd_supportgroup", 0, 0));
        INITIAL_STATUSES.put("medication", new StatusInfo("medication", 0, 0));
        INITIAL_STATUSES.put("laboratory", new StatusInfo("laboratory", 0, 0));
        INITIAL_STATUSES.put("tb_data", new StatusInfo("tb_data", 0, 0));
        INITIAL_STATUSES.put("clinical_consultation", new StatusInfo("clinical_consultation", 0, 0));
        INITIAL_STATUSES.put("sti", new StatusInfo("sti", 0, 0));
        INITIAL_STATUSES.put("family_planning", new StatusInfo("family_planning", 0, 0));
        INITIAL_STATUSES.put("counseling", new StatusInfo("counseling", 0, 0));
        INITIAL_STATUSES.put("prophylaxis", new StatusInfo("prophylaxis", 0, 0));
        INITIAL_STATUSES.put("dah", new StatusInfo("dah", 0, 0));
        INITIAL_STATUSES.put("prep", new StatusInfo("prep", 0, 0));
        INITIAL_STATUSES.put("ccu", new StatusInfo("ccu", 0, 0));
        INITIAL_STATUSES.put("ccr", new StatusInfo("ccr", 0, 0));
        INITIAL_STATUSES.put("home_visit", new StatusInfo("home_visit", 0, 0));
    }

    public GenerationCoordinator() {
        GENERATOR_TASK.addObserver(this);
    }

    public Map<String, StatusInfo> runGeneration() {
        if(!GENERATOR_TASK.isExecuting()) {
            log.debug("Submitting Mozart2 Generation Task to Executor thread.");
            generationException = null;
            if(SINGLE_THREAD_EXECUTOR.isShutdown() || SINGLE_THREAD_EXECUTOR.isTerminated()) {
                SINGLE_THREAD_EXECUTOR = Executors.newSingleThreadExecutor();
            }
            generationRecord = new Mozart2Generation();
            generationRecord.setExecutor(Context.getAuthenticatedUser());
            generationRecord.setBatchSize(Mozart2Properties.getInstance().getBatchSize());
            generationRecord.setDatabaseName(Mozart2Properties.getInstance().getNewDatabaseName());
            generationRecord.setDateStarted(LocalDateTime.now());
            generationRecord.setEndDateUsed(Mozart2Properties.getInstance().getEndDate());
            moz2GenService.saveMozartGeneration(generationRecord);
            SINGLE_THREAD_EXECUTOR.submit(GENERATOR_TASK);
        } else {
            log.debug("Generator Task already running");
        }
        return generateStatusInfo();
    }

    public Map<String, StatusInfo> generateStatusInfo() {
        if(!GENERATOR_TASK.GENERATORS.isEmpty()) {
            Map<String, StatusInfo> progressStatuses = new LinkedHashMap<>(INITIAL_STATUSES);
            GENERATOR_TASK.GENERATORS.stream().forEach(generator -> {
                progressStatuses.replace(generator.getTable(),
                        new StatusInfo(generator.getTable(), generator.getCurrentlyGenerated(),
                                            generator.getToBeGenerated(), generator.getHasRecords()));
            });

            // clear the task
            if(!GENERATOR_TASK.isExecuting()) {
                GENERATOR_TASK.initializeVariables();
            }
            return progressStatuses;
        }
        return INITIAL_STATUSES;
    }

    public void cancelGeneration() {
        cancelGeneration(true);
    }

    private synchronized void cancelGeneration(Boolean updateGenerationRecord) {
        if (GENERATOR_TASK.isExecuting()) {
            log.debug("Stopping Mozart2 generation");
            GENERATOR_TASK.shutdown();
            SINGLE_THREAD_EXECUTOR.shutdownNow();

        } else {
            log.debug("Mozart2 generation already finished, can't stop it");
        }

        if(updateGenerationRecord && generationRecord != null) {
            generationRecord.setStatus(Mozart2Generation.Status.CANCELLED);
            generationRecord.setDateEnded(LocalDateTime.now());
            moz2GenService.saveMozartGeneration(generationRecord);
        }
    }

    @Override
    public synchronized void update(Observable o, Object arg) {
        Map<String, Object> params = (Map) arg;
        String name = (String) params.get("name");
        switch(name) {
            case "generatorTask":
                if(generationRecord != null) {
                    switch((String) params.get("status")) {
                        case "done":
                            generationRecord.setDateEnded(LocalDateTime.now());
                            generationRecord.setStatus(Mozart2Generation.Status.COMPLETED);
                            moz2GenService.saveMozartGeneration(generationRecord);

                            // Audit info into mozart2 database
                            try {
                                String createTableSql = Utils.readFileToString("audit_info.sql");
                                User user = generationRecord.getExecutor();
                                String executor = new StringBuilder(user.getUserId()).append("(").append(user.getUsername()).append(")").toString();
                                String insertSql = new StringBuilder("INSERT INTO `audit_info`(`date_started`, `date_ended`, `batch_size`, `executor`, ")
                                        .append("`locations`, `status`, `end_date_used`) VALUES('").append(generationRecord.getDateStarted()).append("', '")
                                        .append(generationRecord.getDateEnded()).append("', ").append(generationRecord.getBatchSize()).append(", '")
                                        .append(executor).append("', '").append(Mozart2Properties.getInstance().getLocationsIdsString()).append("', '")
                                        .append(generationRecord.getStatus()).append("', '").append(generationRecord.getEndDateUsed()).append("')")
                                        .toString();

                                String[] sqls = new String[] {
                                        createTableSql,
                                        insertSql
                                };
                                DbUtils.runSqlStatements(sqls, Mozart2Properties.getInstance().getNewDatabaseName());

                            } catch (SQLException e) {
                                e.printStackTrace();
                            } catch (IOException ioe) {
                                ioe.printStackTrace();
                            }
                            break;
                        case "dumpFileDone":
                            generationRecord.setSqlDumpPath((String) params.get("filename"));
                            moz2GenService.saveMozartGeneration(generationRecord);
                            break;
                        case "dumpFileError":
                            generationRecord.setErrorMessage((String) params.get("errorMessage"));
                            moz2GenService.saveMozartGeneration(generationRecord);
                            break;
                    }
                }
                break;
            case "exception":
                Exception e = (Exception) params.get("status");
                if(generationRecord != null) {
                    generationRecord.setDateEnded(LocalDateTime.now());
                    generationRecord.setStatus(Mozart2Generation.Status.ERROR);
                    if(e != null) {
                        generationRecord.setErrorMessage(e.getMessage());
                        generationRecord.setStackTrace(ExceptionUtils.getStackTrace(e));
                    }
                    moz2GenService.saveMozartGeneration(generationRecord);
                }
                this.setGenerationException(e);
                this.cancelGeneration(false);
                break;
        }
    }

    public Exception getGenerationException() {
        return this.generationException;
    }

    public void setGenerationException(Exception generationException) {
        this.generationException = generationException;
    }
}
