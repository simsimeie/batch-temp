package com.example.springbatchexample.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.support.builder.CompositeItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import javax.persistence.EntityManagerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class TaskConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EntityManagerFactory entityManagerFactory;
    private Map<String, Person> localRepo = new ConcurrentHashMap<>();

    @Bean
    public Job taskJob() throws Exception {
        return this.jobBuilderFactory.get("taskJob")
                .incrementer(new RunIdIncrementer())
                .start(this.taskStep(null, null))
                .build();
    }

    @Bean
    @JobScope
    public Step taskStep(@Value("#{jobParameters[chunkSize]}") Integer chunkSize, @Value("#{jobParameters[allowDuplicate]}") Boolean allowDuplicate) throws Exception {
        return this.stepBuilderFactory.get("taskStep")
                .<Person, Person>chunk(chunkSize)
                .reader(csvItemReader())
                .processor(duplicateCheckProcessor(allowDuplicate))
                .writer(compositeItemWriter())
                .build();
    }

    @Bean
    public ItemWriter<Person> compositeItemWriter() throws Exception {
        List<ItemWriter<? super Person>> writers = Stream.of(
                jpaItemWriter(),
                logItemWriter()
        ).collect(Collectors.toList());

        CompositeItemWriter<Person> itemWriter = new CompositeItemWriterBuilder<Person>()
                .delegates(writers)
                .build();

        itemWriter.afterPropertiesSet();

        return itemWriter;
    }


    @Bean
    @StepScope
    public ItemWriter<Person> logItemWriter(){
        return persons -> log.info("person.size : {}", persons.size());
    }

    @Bean
    @StepScope
    public ItemWriter<Person> jpaItemWriter() throws Exception {
        JpaItemWriter<Person> itemWriter = new JpaItemWriterBuilder<Person>()
                .entityManagerFactory(entityManagerFactory)
                .build();

        itemWriter.afterPropertiesSet();
        return itemWriter;
    }

    private ItemProcessor<Person, Person> duplicateCheckProcessor(boolean allowDuplicate) {
        return person -> {
          if(allowDuplicate == true) {
              return person;
          }
          else {
              if(localRepo.get(person.getName()) != null) {
                  return null;
              }
              else {
                  localRepo.put(person.getName(), person);
                  return person;
              }
          }
        };
    }

    private ItemReader<Person> csvItemReader() throws Exception {
        DefaultLineMapper<Person> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "name", "age", "address");
        lineMapper.setLineTokenizer(tokenizer);

        lineMapper.setFieldSetMapper(fieldSet -> {
            int id = fieldSet.readInt("id");
            String name = fieldSet.readString("name");
            String age = fieldSet.readString("age");
            String address = fieldSet.readString("address");

            return new Person(id, name, age, address);
        });

        FlatFileItemReader<Person> itemReader = new FlatFileItemReaderBuilder<Person>()
                .name("csvFileItemReader")
                .encoding("UTF-8")
                .resource(new ClassPathResource("task-input.csv"))
                //첫번째 라인은 필드명이므로 1줄은 스킵해라
                .linesToSkip(1)
                .maxItemCount(100)
                .lineMapper(lineMapper)
                .build();
        // itemReader에서 필수 항목이 정상적으로 설정되었는지 확인하는 메서드
        itemReader.afterPropertiesSet();

        return itemReader;
    }
}
