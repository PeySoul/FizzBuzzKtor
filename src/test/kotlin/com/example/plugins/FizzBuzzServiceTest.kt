package com.example.plugins

import org.junit.Test
import org.junit.jupiter.api.Assertions.*

class FizzBuzzServiceTest {
    @Test
    fun fizzBuzz() {
        // Given
        val fizzBuzz = FizzBuzz(3,5,10,"Fizz","Buzz")
        // When
        val result = fizzBuzz.getFizzBuzzResult().toString()
        //Then
        assertEquals("[FizzBuzz, 1, 2, Fizz, 4, Buzz, Fizz, 7, 8, Fizz, Buzz]", result)
    }



}