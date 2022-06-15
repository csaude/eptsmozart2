package org.openmrs.module.eptsmozart2;

import org.openmrs.module.eptsmozart2.etl.PatientTableGenerator;
import org.openmrs.scheduler.Task;
import org.openmrs.scheduler.TaskDefinition;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/10/22.
 */
public class GeneratorTask implements Task, Callable<Void> {
	
	private TaskDefinition taskDefinition;
	
	private static AtomicBoolean taskIsRunning = new AtomicBoolean(false);
	
	@Override
	public void execute() {
		if (taskIsRunning.get()) {
			return;
		}
		taskIsRunning.set(true);
		
		try {
			DbUtils.createNewDatabase();
			new PatientTableGenerator().call();
		}
		catch (SQLException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			taskIsRunning.set(false);
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
		
	}
	
	@Override
	public Void call() throws Exception {
		execute();
		return Void.TYPE.newInstance();
	}
}
