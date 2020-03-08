package com.lsd.etl.itag.config;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by lsd
 * 2020-03-07 14:56
 */
@Configuration
public class GsonConfig {

    @Bean
    public Gson gson() {
        return new GsonBuilder().setPrettyPrinting().create();
    }

}
