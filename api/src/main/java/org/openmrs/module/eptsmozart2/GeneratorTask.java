package org.openmrs.module.eptsmozart2;

import org.openmrs.api.context.Context;
import org.openmrs.module.eptsmozart2.etl.*;
import org.openmrs.scheduler.Task;
import org.openmrs.scheduler.TaskDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GeneratorTask extends Observable implements Observer, Task, Callable<Void> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeneratorTask.class);
    private static final Integer NUMBER_OF_THREADS = 16;
    private TaskDefinition taskDefinition;
    private static ExecutorService service = new ContextAwareExecutorService(Executors.newFixedThreadPool(NUMBER_OF_THREADS));
    private static AtomicBoolean taskIsRunning = new AtomicBoolean(false);
    public static final List<Generator> GENERATORS = new ArrayList<>(NUMBER_OF_THREADS);

	@Override
	public void execute() {
		if (taskIsRunning.get()) {
			LOGGER.debug("Task is already running");
			return;
		}
		taskIsRunning.set(true);

		try {
            Mozart2Properties.reInitializeMozart2Properties();
            initializeVariables();
			DbUtils.createNewDatabase();

			// Create the lookup table
			String[] sqls = Utils.readFileToString("type_id_lookup.sql").split(";");
			DbUtils.runSqlStatements(sqls, Mozart2Properties.getInstance().getNewDatabaseName());

            // Create the encounter_obs table.
            sqls = Utils.readFileToString("encounter_obs.sql").split(";");
            DbUtils.runSqlStatements(sqls, Mozart2Properties.getInstance().getDatabaseName());


			List<Generator> toBeInvoked = new ArrayList<>(NUMBER_OF_THREADS);
			ObservableGenerator generator = new PatientTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new LocationTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new ObservationLookupTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

            GENERATORS.addAll(toBeInvoked);
            service.invokeAll(toBeInvoked);
            toBeInvoked.clear();

			generator = new ObservationTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

            generator = new DSDSupportGroupTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new PatientStateTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new FormTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new IdentifierTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new KeyVulnerablePopTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new MedicationTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new LaboratoryTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new ClinicalConsultationTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new TBDataTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

            generator = new STITableGenerator();
            generator.addObserver(this);
            toBeInvoked.add(generator);

			generator = new FamilyPlanningTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new CounselingTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new ProphylaxisTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new DAHTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new PrepTableGenerator();
            generator.addObserver(this);
            toBeInvoked.add(generator);

            generator = new CCUTableGenerator();
            generator.addObserver(this);
            toBeInvoked.add(generator);

            generator = new CCRTableGenerator();
            generator.addObserver(this);
            toBeInvoked.add(generator);

            generator = new HomeVisitTableGenerator();
            generator.addObserver(this);
            toBeInvoked.add(generator);

            GENERATORS.addAll(toBeInvoked);
            service.invokeAll(toBeInvoked);

            notifyTaskCompletion("done");
            sqls = Utils.readFileToString("drop_encounter_obs_table.sql").split(";");
            DbUtils.runSqlStatements(sqls, Mozart2Properties.getInstance().getDatabaseName());
            createDumpFileAndNotify();
        } catch (SQLException | IOException | InterruptedException e) {
            LOGGER.error("An error occurred during generation", e);
            if(e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            e.printStackTrace();
        } finally {
            shutdownService();
            taskIsRunning.set(false);
        }
    }

    private void notifyTaskCompletion(String status) {
        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("name", "generatorTask");
        parameters.put("status", status);
        this.setChanged();
        try {
            Context.openSession();
            this.notifyObservers(parameters);
        } finally {
            Context.closeSession();
        }
    }

    private void createDumpFileAndNotify() {
        Map<String, String> parameters = new LinkedHashMap<>();
        parameters.put("name", "generatorTask");
        try {
            File dumpFile = Utils.createMozart2SqlDump();
            parameters.put("status", "dumpFileDone");
            parameters.put("filename", dumpFile.getCanonicalPath());
        } catch (Exception e) {
            LOGGER.error("Error while creating the sql dump file: {}", e.getMessage());
            e.printStackTrace();
            parameters.put("status", "dumpFileError");
            parameters.put("errorMessage", e.getMessage());
        } finally {
            this.setChanged();
            try {
                Context.openSession();
                this.notifyObservers(parameters);
            } finally {
                Context.closeSession();
            }
        }
    }

    private void shutdownService() {
        try {
            if (!service.isShutdown()) {
                service.shutdown();
                if (!service.awaitTermination(5, TimeUnit.MINUTES)) {
                    service.shutdownNow();
                }
            }
        } catch (InterruptedException e) {
            LOGGER.error("Service shutdown interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void initialize(TaskDefinition taskDefinition) {
        this.taskDefinition = taskDefinition;
    }

    @Override
    public TaskDefinition getTaskDefinition() {
        return taskDefinition;
    }

    @Override
    public boolean isExecuting() {
        return taskIsRunning.get();
    }

    @Override
    public void shutdown() {
        for (Generator generator : GENERATORS) {
            try {
                generator.cancel();
            } catch (SQLException e) {
                LOGGER.error("Error canceling generator for table: {}", generator.getTable(), e);
            }
        }
        shutdownService();
        taskIsRunning.set(false);
    }

    @Override
    public Void call() throws Exception {
        execute();
        return Void.TYPE.newInstance();
    }

    public static void initializeVariables() {
        GENERATORS.clear();
        service = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
    }

    @Override
    public void update(Observable o, Object arg) {
        LOGGER.debug("Notifying observers about a message from {}", o);
        this.setChanged();
        this.notifyObservers(arg);
    }
}
