package org.openmrs.module.eptsmozart2.web;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.openmrs.User;
import org.openmrs.module.eptsmozart2.UserSerializer;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;

import java.util.Arrays;
import java.util.List;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 8/23/22.
 */
@Configuration
@EnableWebMvc
public class WebConfiguration extends WebMvcConfigurerAdapter {
	
	@Override
	public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
		SimpleModule m = new SimpleModule();
		m.addSerializer(User.class, new UserSerializer());
		Jackson2ObjectMapperBuilder builder = new Jackson2ObjectMapperBuilder().modules(Arrays.asList(m));
		converters.add(new MappingJackson2HttpMessageConverter(builder.build()));
	}
}
