package com.springbatch.hari.Repo;


import com.springbatch.hari.Entity.Person;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PersonRepository extends JpaRepository<Person, Long> {
}
