package org.openmrs.module.eptsmozart2;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.openmrs.User;

import java.io.IOException;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 8/23/22.
 */
public class UserSerializer extends JsonSerializer<User> {
	
	@Override
	public void serialize(User value, JsonGenerator jgen, SerializerProvider provider) throws IOException,
	        JsonProcessingException {
		jgen.writeStartObject();
		jgen.writeStringField("uuid", value.getUuid());
		if (value.getPersonName() != null) {
			jgen.writeStringField("fullname", value.getPersonName().getFullName());
		}
		jgen.writeStringField("username", value.getUsername());
		jgen.writeEndObject();
	}
}
