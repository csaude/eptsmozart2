package org.openmrs.module.eptsmozart2;

import org.openmrs.api.context.Context;
import org.openmrs.module.eptsmozart2.api.EPTSMozART2GenerationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Date;
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

    @Autowired
    private EPTSMozART2GenerationService moz2GenService;

    public static final GeneratorTask GENERATOR_TASK = new GeneratorTask();
    public static final Map<String, StatusInfo> INITIAL_STATUSES = new LinkedHashMap<>();
    private static ExecutorService SINGLE_THREAD_EXECUTOR = Executors.newSingleThreadExecutor();

    static {
        INITIAL_STATUSES.put("patient", new StatusInfo("patient", 0, 0));
        INITIAL_STATUSES.put("location", new StatusInfo("location", 0, 0));
        INITIAL_STATUSES.put("patient_state", new StatusInfo("patient_state", 0, 0));
        INITIAL_STATUSES.put("program", new StatusInfo("program", 0, 0));
        INITIAL_STATUSES.put("form", new StatusInfo("form", 0, 0));
        INITIAL_STATUSES.put("identifier", new StatusInfo("identifier", 0, 0));
        INITIAL_STATUSES.put("observation", new StatusInfo("observation", 0, 0));
        INITIAL_STATUSES.put("medications", new StatusInfo("medications", 0, 0));
        INITIAL_STATUSES.put("laboratory", new StatusInfo("laboratory", 0, 0));
        INITIAL_STATUSES.put("clinical_consultation", new StatusInfo("clinical_consultation", 0, 0));
    }

    public GenerationCoordinator() {
        GENERATOR_TASK.addObserver(this);
    }

    public Map<String, StatusInfo> runGeneration() {
        if(!GENERATOR_TASK.isExecuting()) {
            log.debug("Submitting Mozart2 Generation Task to Executor thread.");
            if(SINGLE_THREAD_EXECUTOR.isShutdown() || SINGLE_THREAD_EXECUTOR.isTerminated()) {
                SINGLE_THREAD_EXECUTOR = Executors.newSingleThreadExecutor();
            }
            generationRecord = new Mozart2Generation();
            generationRecord.setExecutor(Context.getAuthenticatedUser());
            generationRecord.setBatchSize(Mozart2Properties.getInstance().getBatchSize());
            generationRecord.setDatabaseName(Mozart2Properties.getInstance().getNewDatabaseName());
            generationRecord.setDateStarted(LocalDateTime.now());
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
                        new StatusInfo(generator.getTable(), generator.getCurrentlyGenerated(), generator.getToBeGenerated()));
            });

            // clear the task
            if(!GENERATOR_TASK.isExecuting()) {
                GENERATOR_TASK.initializeVariables();
            }
            return progressStatuses;
        }
        return INITIAL_STATUSES;
    }

    public synchronized void cancelGeneration() {
        if (GENERATOR_TASK.isExecuting()) {
            log.debug("Stopping Mozart2 generation");
            GENERATOR_TASK.shutdown();
            SINGLE_THREAD_EXECUTOR.shutdownNow();

        } else {
            log.debug("Mozart2 generation already finished, can't stop it");
        }

        if(generationRecord != null) {
            generationRecord.setStatus(Mozart2Generation.Status.CANCELLED);
            generationRecord.setDateEnded(LocalDateTime.now());
            moz2GenService.saveMozartGeneration(generationRecord);
        }
    }

    @Override
    public synchronized void update(Observable o, Object arg) {
        Map<String, String> params = (Map) arg;
        String name = params.get("name");
        switch(name) {
            case "generatorTask":
                if(generationRecord != null) {
                    if("done".equalsIgnoreCase(params.get("status"))) {
                        generationRecord.setDateEnded(LocalDateTime.now());
                        generationRecord.setStatus(Mozart2Generation.Status.COMPLETED);
                        moz2GenService.saveMozartGeneration(generationRecord);
                    }
                    if("dumpFileDone".equalsIgnoreCase(params.get("status"))) {
                        generationRecord.setSqlDumpPath(params.get("filename"));
                        moz2GenService.saveMozartGeneration(generationRecord);
                    }
                }
        }
    }
}
