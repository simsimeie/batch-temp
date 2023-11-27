package com.example.springbatchexample.part3;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.util.ArrayList;
import java.util.List;

@Configuration
@Slf4j
@RequiredArgsConstructor
public class TaskInputConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job taskInputJob() throws Exception {
        return this.jobBuilderFactory.get("taskInputJob")
                .incrementer(new RunIdIncrementer())
                .start(this.taskInputStep(null))
                .build();
    }

    @Bean
    @JobScope
    public Step taskInputStep(@Value("#{jobParameters[chunkSize]}")Integer chunkSize) throws Exception {
        return this.stepBuilderFactory.get("taskInputStep")
                .<Person, Person>chunk(chunkSize)
                .reader(inputReader())
                .writer(inputWriter())
                .build();
    }

    private ItemWriter<Person> inputWriter() throws Exception {
        BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[] {"id", "name", "age", "address"});
        DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<>();
        lineAggregator.setDelimiter(",");
        lineAggregator.setFieldExtractor(fieldExtractor);

        FlatFileItemWriter<Person> itemWriter = new FlatFileItemWriterBuilder<Person>()
                .name("csvFileItemWriter")
                .encoding("UTF-8")
                .resource(new FileSystemResource("output/task-input.csv"))
                .lineAggregator(lineAggregator)
                .headerCallback(writer -> writer.write("id,이름,나이,거주지"))
                .footerCallback(writer -> writer.write("---------------\n"))
                .append(true)
                .build();

        itemWriter.afterPropertiesSet();

        return itemWriter;
    }

    private ItemReader<Person> inputReader() {
        return new CustomItemReader<Person>(getItems());
    }

    private List<Person> getItems() {
        List<Person> personList = new ArrayList<>();
        String[] names = {"멤버1", "멤버2", "멤버3", "멤버4", "멤버5", "멤버6", "멤버7", "멤버8", "멤버9", "멤버10"};

        for (int i = 0; i < 100; i++) {
            personList.add(new Person(i+1, names[i% names.length], "test age", "test address"));
        }

        return personList;
    }
}
