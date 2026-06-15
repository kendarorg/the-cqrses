package org.kendar.pfm.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * Supplies the mandatory {@code kf-datasource} bean the kf-spring starter consumes (it is
 * {@code @Qualifier}-injected into {@code DefaultDb}). The framework's tables and the app's
 * {@code pfm_*} read tables share this single H2 ({@code MODE=MySQL}) database; the same bean backs
 * the read-model {@link JdbcTemplate}.
 */
@Configuration
public class DataSourceConfig {

    @Bean("kf-datasource")
    public DataSource kfDataSource(@Value("${pfm.datasource.url}") String url) {
        return DataSourceBuilder.create().url(url).build();
    }

    @Bean
    public JdbcTemplate jdbcTemplate(@Qualifier("kf-datasource") DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
