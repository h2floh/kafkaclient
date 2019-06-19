package com.microsoft.cse;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World! I am a Kafka consumer!" );

        Consumer myconsumer = new Consumer();

        myconsumer.StartConsuming();

        System.out.println( "Bye World! I was a Kafka consumer!" );
    }
}