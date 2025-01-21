package org.openmrs.module.eptsmozart2;

import org.openmrs.api.context.Context;
import org.openmrs.api.context.UserContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 1/21/25.
 */
public class ContextAwareExecutorService implements ExecutorService {
	
	private final ExecutorService delegate;
	
	public ContextAwareExecutorService(ExecutorService delegate) {
		this.delegate = delegate;
	}
	
	@Override
    public void execute(Runnable command) {
        // Capture the context of the current thread
        UserContext userContext = Context.getUserContext();
        // Wrap the task to propagate the captured context
        Runnable wrappedCommand = () -> {
            try {
                Context.setUserContext(userContext);
                command.run(); // Execute the original task
            } finally {
                Context.clearUserContext();
            }
        };
        // Submit the wrapped task to the executor
        delegate.execute(wrappedCommand);
    }
	
	// Delegate all other methods to the underlying executor
	@Override
	public void shutdown() {
		delegate.shutdown();
	}
	
	@Override
	public boolean isShutdown() {
		return delegate.isShutdown();
	}
	
	@Override
	public boolean isTerminated() {
		return delegate.isTerminated();
	}
	
	@Override
	public List<Runnable> shutdownNow() {
		return delegate.shutdownNow();
	}
	
	@Override
	public boolean awaitTermination(long timeout, java.util.concurrent.TimeUnit unit) throws InterruptedException {
		return delegate.awaitTermination(timeout, unit);
	}
	
	@Override
    public <T> java.util.concurrent.Future<T> submit(java.util.concurrent.Callable<T> task) {
        UserContext userContext = Context.getUserContext();
        return delegate.submit(() -> {
            Context.setUserContext(userContext);
            try {
                return task.call();
            } finally {
                Context.clearUserContext();
            }
        });
    }
	
	@Override
    public <T> java.util.concurrent.Future<T> submit(Runnable task, T result) {
        UserContext userContext = Context.getUserContext();
        return delegate.submit(() -> {
            Context.setUserContext(userContext);
            try {
                task.run();
            } finally {
                Context.clearUserContext();
            }
        }, result);
    }
	
	@Override
    public java.util.concurrent.Future<?> submit(Runnable task) {
        UserContext userContext = Context.getUserContext();;
        return delegate.submit(() -> {
            Context.setUserContext(userContext);
            try {
                task.run();
            } finally {
                Context.clearUserContext();
            }
        });
    }
	
	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		List<Callable<T>> wrappedTasks = wrapTasksWithUserContext(tasks);
		return delegate.invokeAll(wrappedTasks);
	}
	
	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
	        throws InterruptedException {
		List<Callable<T>> wrappedTasks = wrapTasksWithUserContext(tasks);
		// Submit wrapped tasks to the executor and return the futures
		return delegate.invokeAll(wrappedTasks, timeout, unit);
	}
	
	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
		List<Callable<T>> wrappedTasks = wrapTasksWithUserContext(tasks);
		// Submit tasks to the executor and return the result of the first successful task
		return delegate.invokeAny(wrappedTasks);
	}
	
	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
	        throws InterruptedException, ExecutionException, TimeoutException {
		List<Callable<T>> wrappedTasks = wrapTasksWithUserContext(tasks);
		// Submit wrapped tasks to the executor and return the futures
		return delegate.invokeAny(wrappedTasks, timeout, unit);
	}
	
	protected <T> List<Callable<T>> wrapTasksWithUserContext(Collection<? extends Callable<T>> tasks) {
        List<Callable<T>> wrappedTasks = new ArrayList<>();
        UserContext userContext = Context.getUserContext();

        // Wrap each task to propagate user context
        for (Callable<T> task : tasks) {
            wrappedTasks.add(() -> {
                try {
                    Context.setUserContext(userContext);  // Set user context for the task
                    return task.call();
                } finally {
                    Context.clearUserContext();  // Ensure user context is cleared after task execution
                }
            });
        }
        return wrappedTasks;
    }
}
