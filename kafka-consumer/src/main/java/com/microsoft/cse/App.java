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

        try
        {
            Consumer myconsumer = new Consumer();

            myconsumer.StartConsuming();
        }
        catch (Exception e)
        {
            System.out.printf("Oops, something went wrong %s\n", e.getMessage());
            e.printStackTrace();
        }

        System.out.println( "Bye World! I was a Kafka consumer!" );
    }
}