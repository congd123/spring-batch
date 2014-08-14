package com.spring.batch;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class UserItemProcessor implements ItemProcessor<User, User> {

	@Override
	public User process(final User user) throws Exception {
		String upperName = user.getName().toUpperCase();
		
		return new User(user.getId(), upperName, user.getEmail());
	}

}
