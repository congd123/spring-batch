package com.spring.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan
public class Application {
	public static void main(String[] args) {
		AnnotationConfigApplicationContext ctx = null;
		
		try {
			ctx = new AnnotationConfigApplicationContext(BatchConfiguration.class);

			JobLauncher jobLauncher = ctx.getBean("jobLauncher", JobLauncher.class);
			Job job = ctx.getBean("job", Job.class);

			JobExecution execution = jobLauncher.run(job, new JobParameters());
			
			System.out.println("Execution: " + execution.getStatus());
		} catch (Exception e) {
			System.out.println("Error!");
		} finally {
			if (ctx != null) {
				ctx.close();
			}
		}
	}
}
