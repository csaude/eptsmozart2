package org.openmrs.module.eptsmozart2;


import org.openmrs.module.eptsmozart2.etl.ClinicalConsultationTableGenerator;
import org.openmrs.module.eptsmozart2.etl.FormTableGenerator;
import org.openmrs.module.eptsmozart2.etl.Generator;
import org.openmrs.module.eptsmozart2.etl.IdentifierTableGenerator;
import org.openmrs.module.eptsmozart2.etl.LaboratoryGenerator;
import org.openmrs.module.eptsmozart2.etl.MedicationsTableGenerator;
import org.openmrs.module.eptsmozart2.etl.ObservationTableGenerator;
import org.openmrs.module.eptsmozart2.etl.PatientStateTableGenerator;
import org.openmrs.module.eptsmozart2.etl.PatientTableGenerator;
import org.openmrs.module.eptsmozart2.etl.ProgramTableGenerator;
import org.openmrs.scheduler.Task;
import org.openmrs.scheduler.TaskDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/10/22.
 */
public class GeneratorTask implements Task, Callable<Void> {
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

			initializeVariables();

			Generator generator = new PatientTableGenerator();
			GENERATORS.add(generator);
			generator.call();
			
			List<Generator> toBeInvoked = new ArrayList<>(10);
			generator = new ObservationTableGenerator();
			toBeInvoked.add(generator);

			generator = new PatientStateTableGenerator();
			toBeInvoked.add(generator);

			generator = new ProgramTableGenerator();
			toBeInvoked.add(generator);

			generator = new FormTableGenerator();
			toBeInvoked.add(generator);

			generator = new IdentifierTableGenerator();
			toBeInvoked.add(generator);

			generator = new MedicationsTableGenerator();
			toBeInvoked.add(generator);

			generator = new LaboratoryGenerator();
			toBeInvoked.add(generator);

			generator = new ClinicalConsultationTableGenerator();
			toBeInvoked.add(generator);

			GENERATORS.addAll(toBeInvoked);

			service.invokeAll(toBeInvoked);

			service.shutdownNow();

			taskIsRunning.set(false);
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		} 
		catch (Exception e) {
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
		service.shutdownNow();
		taskIsRunning.set(false);
	}
	
	@Override
	public Void call() throws Exception {
		execute();
		return Void.TYPE.newInstance();
	}

	private static void initializeVariables() {
        GENERATORS.clear();
        service = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
    }
}
