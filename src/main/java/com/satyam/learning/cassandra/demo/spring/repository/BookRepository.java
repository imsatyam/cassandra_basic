package com.satyam.learning.cassandra.demo.spring.repository;

import com.satyam.learning.cassandra.demo.spring.entity.Book;
import org.springframework.data.cassandra.repository.CassandraRepository;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.stereotype.Repository;

/**
 * Created by Satyam on 3/13/2016.
 */
@Repository
public interface BookRepository extends CassandraRepository<Book> {

    @Query("select * from book where title = ?0 and publisher=?1")
    Iterable<Book> findByTitleAndPublisher(String title, String publisher);

}