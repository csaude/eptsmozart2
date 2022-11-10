package org.openmrs.module.eptsmozart2;


import org.openmrs.api.context.Context;
import org.openmrs.module.eptsmozart2.etl.ClinicalConsultationTableGenerator;
import org.openmrs.module.eptsmozart2.etl.DSDTableGenerator;
import org.openmrs.module.eptsmozart2.etl.FormTableGenerator;
import org.openmrs.module.eptsmozart2.etl.FormTypeTableGenerator;
import org.openmrs.module.eptsmozart2.etl.Generator;
import org.openmrs.module.eptsmozart2.etl.IdentifierTableGenerator;
import org.openmrs.module.eptsmozart2.etl.KeyPopVulnerableTableGenerator;
import org.openmrs.module.eptsmozart2.etl.LaboratoryGenerator;
import org.openmrs.module.eptsmozart2.etl.LocationTableGenerator;
import org.openmrs.module.eptsmozart2.etl.MedicationTableGenerator;
import org.openmrs.module.eptsmozart2.etl.ObservableGenerator;
import org.openmrs.module.eptsmozart2.etl.ObservationLookupTableGenerator;
import org.openmrs.module.eptsmozart2.etl.ObservationTableGenerator;
import org.openmrs.module.eptsmozart2.etl.PatientStateTableGenerator;
import org.openmrs.module.eptsmozart2.etl.PatientTableGenerator;
import org.openmrs.module.eptsmozart2.etl.ProgramTableGenerator;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/10/22.
 */
public class GeneratorTask extends Observable implements Observer, Task, Callable<Void> {
	private static final Logger LOGGER = LoggerFactory.getLogger(GeneratorTask.class);
	private static final Integer NUMBER_OF_THREADS = 10;
	private TaskDefinition taskDefinition;
	private static ExecutorService service = Executors.newFixedThreadPool(NUMBER_OF_THREADS);

	private static AtomicBoolean taskIsRunning = new AtomicBoolean(false);

	public static final List<Generator> GENERATORS = new ArrayList<>(10);

	@Override
	public void execute() {
		if (taskIsRunning.get()) {
			LOGGER.debug("Task is already running");
			return;
		}
		taskIsRunning.set(true);

		try {
			DbUtils.createNewDatabase();

			// Create the lookup table
			String[] sqls = Utils.readFileToString("type_id_lookup.sql").split(";");
			DbUtils.runSqlStatements(sqls, Mozart2Properties.getInstance().getNewDatabaseName());

			initializeVariables();

			List<Generator> toBeInvoked = new ArrayList<>(10);
			ObservableGenerator generator = new PatientTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new LocationTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new FormTypeTableGenerator();
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

			generator = new DSDTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new PatientStateTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new ProgramTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new FormTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new IdentifierTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new KeyPopVulnerableTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new MedicationTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new LaboratoryGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			generator = new ClinicalConsultationTableGenerator();
			generator.addObserver(this);
			toBeInvoked.add(generator);

			GENERATORS.addAll(toBeInvoked);

			service.invokeAll(toBeInvoked);

			service.shutdownNow();

			taskIsRunning.set(false);

			Map<String, String> parameters = new LinkedHashMap<>();
			parameters.put("name", "generatorTask");
			parameters.put("status", "done");
			this.setChanged();
			try {
				Context.openSession();
				this.notifyObservers(parameters);
			} finally {
				Context.closeSession();
			}
			// Create the dumpfile
			try {
				File dumpFile  = Utils.createMozart2SqlDump();
				parameters.replace("status",  "dumpFileDone");
				parameters.put("filename", dumpFile.getCanonicalPath());
				this.setChanged();
				Context.openSession();
				this.notifyObservers(parameters);
			} catch (IOException e) {
				parameters.replace("status", "dumpFileError");
				parameters.put("errorMessage", e.getMessage());
				this.setChanged();
				Context.openSession();
				this.notifyObservers(parameters);
			} finally {
				Context.closeSession();
			}
		}
		catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			service.shutdownNow();
			e.printStackTrace();
		} finally {
			if(service.isTerminated() || service.isShutdown()) {
				taskIsRunning.set(false);
			}
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
		for(Generator generator: GENERATORS) {
			try {
				generator.cancel();
			} catch (SQLException e) {
				LOGGER.error("An error occurred while canceling generator for table: {}", generator.getTable(), e);
			}
		}
		service.shutdownNow();
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
		LOGGER.debug("Notifying observers about a messsage from {}", o);
		this.setChanged();
		this.notifyObservers(arg);
	}
}
