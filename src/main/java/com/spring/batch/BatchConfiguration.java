package com.spring.batch;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.SqlPagingQueryProviderFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableBatchProcessing
@ComponentScan({
	"com.spring.batch"
})
public class BatchConfiguration extends DefaultBatchConfigurer {
	@Autowired private JobBuilderFactory jobs;
	@Autowired private StepBuilderFactory steps;
	
	@Bean
	public Job job(Step step) throws Exception {
		return jobs.get("job")
				.start(step)
				.build();
	}
	
	@Bean
	public Step step(
			@Qualifier("pagingItemReader") JdbcPagingItemReader<User> reader,
			UserItemProcessor processor,
			JdbcBatchItemWriter<User> writer){
		return steps.get("step")
				.<User,User>chunk(2)
				.reader(reader)
				.processor(processor)
				.writer(writer)
				.build();
	}
	
	@Bean
	@StepScope
	public JdbcPagingItemReader<User> pagingItemReader(@Qualifier("dataSourceFrom") DataSource dataSource,
			PagingQueryProvider queryProvider, UserRowMapper userRowMapper) {
		
		JdbcPagingItemReader<User> itemReader = new JdbcPagingItemReader<User>();
		itemReader.setDataSource(dataSource);
		itemReader.setQueryProvider(queryProvider);
		itemReader.setPageSize(10);
		itemReader.setRowMapper(userRowMapper);
		
		return itemReader;
	}
	
	@Bean
	protected PagingQueryProvider queryProvider(@Qualifier("dataSourceFrom") DataSource dataSource) throws Exception {
		SqlPagingQueryProviderFactoryBean fb = new SqlPagingQueryProviderFactoryBean();
		fb.setDataSource(dataSource);
		fb.setSelectClause("id, name, email");
		fb.setFromClause("users");
		fb.setSortKey("id");

		return fb.getObject();
	}
	
	@Bean
	protected JdbcBatchItemWriter<User> writer(@Qualifier("dataSourceTo") DataSource dataSource) throws Exception {
		JdbcBatchItemWriter<User> itemWriter = new JdbcBatchItemWriter<User>();
		itemWriter.setDataSource(dataSource);
		itemWriter.setSql("INSERT INTO users VALUES (:id, :name, :email)");
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<User>());
		
		return itemWriter;
	}
	
	@Qualifier("dataSourceFrom")
	@Bean
	public DataSource dataSourceFrom() {
		EmbeddedDatabaseBuilder embeddedDatabaseBuilder = new EmbeddedDatabaseBuilder();
		return embeddedDatabaseBuilder
				.setName("dataSourceFrom")
				.addScript("classpath:schema-hsql-from.sql")
				.setType(EmbeddedDatabaseType.HSQL)
				.build();
	}

	@Qualifier("dataSourceTo")
	@Bean
	public DataSource dataSourceTo() {
		EmbeddedDatabaseBuilder embeddedDatabaseBuilder = new EmbeddedDatabaseBuilder();
		return embeddedDatabaseBuilder
				.setName("dataSourceTo")
				.addScript("classpath:schema-hsql-to.sql")
				.setType(EmbeddedDatabaseType.HSQL)
				.build();
	}
	
	@Bean
	public JobRepository jobRepository(PlatformTransactionManager transactionManager) throws Exception {
		MapJobRepositoryFactoryBean jobRep = new MapJobRepositoryFactoryBean();
		jobRep.setTransactionManager(transactionManager);

		return jobRep.getObject();
	}

	@Bean
	public JobLauncher jobLauncher(JobRepository jobRepository) throws Exception {
		SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
		jobLauncher.setJobRepository(jobRepository);

		return jobLauncher;
	}

    @Override
    @Autowired
    public void setDataSource(@Qualifier("dataSourceTo") DataSource dataSource) {
    	super.setDataSource(dataSource);
    }
}
