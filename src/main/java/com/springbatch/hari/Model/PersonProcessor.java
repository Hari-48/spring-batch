package com.springbatch.hari.Model;


import com.springbatch.hari.Entity.Person;
import org.springframework.batch.item.ItemProcessor;

public class PersonProcessor implements ItemProcessor<Person, Person> {

	@Override
	public Person process(Person person) throws Exception {

		 person.setFirstName(person.getFirstName().toUpperCase());
		 person.setLastName(person.getLastName().toUpperCase());

		return person;
	}
}