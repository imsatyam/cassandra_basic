package com.satyam.learning.cassandra.demo.spring.client;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableSet;
import com.satyam.learning.cassandra.demo.spring.entity.Book;
import com.satyam.learning.cassandra.demo.spring.repository.BookRepository;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.ClassPathResource;

/**
 * Created by Satyam on 3/13/2016.
 * Spring Client does not work with Cassandra 3.x. It still supports cassandra 2 dataStax drivers
 */
public class BookRepositoryClient {

    public static void main(String[] args)
    {
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(new ClassPathResource("spring-config.xml").getPath());
        BookRepository bookRepository = context.getBean(BookRepository.class);

        System.out.println("1. Saving a book...");
        Book book1 = new Book(UUIDs.timeBased(), "Head First Java", "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        bookRepository.save(ImmutableSet.of(book1));

        System.out.println("2. Fetching all the books...");
        printBooks(bookRepository.findAll());

        System.out.println("3. Saving a list of books...");
        Book b1 = new Book(UUIDs.timeBased(), "Head Design Patterns", "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        Book b2 = new Book(UUIDs.timeBased(), "Head First - Cassandra", "O'Reilly Media", ImmutableSet.of("Computer", "Software"));
        bookRepository.save(ImmutableSet.of(b1, b2));

        System.out.println("4. Fetching all the books...");
        printBooks(bookRepository.findAll());

        System.out.println("5. Updating title of Java Book...");
        book1.setTitle("Head First Java Second Edition");
        bookRepository.save(ImmutableSet.of(book1));

        System.out.println("6. Searching Java book with title and publisher...");
        printBooks(bookRepository.findByTitleAndPublisher("Head First Java Second Edition", "O'Reilly Media"));

        System.out.println("7. Deleting Design patterns book...");
        bookRepository.delete(b1);

        System.out.println("8. Fetching all the books...");
        printBooks(bookRepository.findAll());

        context.close();
    }

    private static void printBooks(Iterable<Book> books) {
        for(Book book : books){
            System.out.println("ID: " + book.getId() + " Title: " + book.getTitle());
        }
    }

}
