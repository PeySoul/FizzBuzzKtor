package com.example.plugins

import kotlinx.coroutines.*
import kotlinx.coroutines.selects.select
import kotlinx.serialization.Serializable
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.Statement

@Serializable
data class FizzBuzz(
    val int1: Int,
    val int2: Int,
    val limit: Int,
    val str1: String,
    val str2: String,
    val counter: Int = 0
)

class FizzBuzzService(private val connection: Connection) {
    companion object {
        private const val CREATE_TABLE_FIZZBUZZ =
            "CREATE TABLE FIZZBUZZS (ID SERIAL PRIMARY KEY, INT1 INT, INT2 INT, CEILING INT, STR1 VARCHAR(255), STR2 VARCHAR(255), COUNTER INT)"
        private const val INSERT_FIZZBUZZ =
            "INSERT INTO fizzbuzzs (int1, int2, ceiling, str1, str2, counter) VALUES (?, ?, ?, ?, ?, ?)"
        private const val SELECT_FIIZBUZZ =
            "SELECT id, int1, int2, ceiling, str1, str2, counter FROM fizzbuzzs WHERE int1 = ? AND int2 = ? AND ceiling = ? AND str1 LIKE ? AND str2 LIKE ? "
        private const val UPDATE_FIIZBUZZ = "UPDATE fizzbuzzs SET counter = ? WHERE id = ?"
        private const val SELECT_MOST_POPULAR =
            "SELECT id, int1, int2, ceiling, str1, str2, counter FROM fizzbuzzs order by counter limit 1"
    }

    init {
        val statement = connection.createStatement()
        statement.executeUpdate(CREATE_TABLE_FIZZBUZZ)
    }

    /**
     * Create a fizzbuzz row if it is the first encountered with those params or update the counter if
     * this fizzbuzz appeared before
     */
    suspend fun create(fizzBuzz: FizzBuzz): List<String> = withContext(Dispatchers.IO) {
        val resultSet = getSelectStatement(fizzBuzz).executeQuery()
        if (resultSet.next()) {
            val counter = resultSet.getInt("counter")
            val id = resultSet.getInt("id")
            getUpdateStatement(counter, id).executeUpdate()

            return@withContext getFizzBuzzReturn(fizzBuzz)
        } else {
            //We insert as the same fizzbuzz has not been seen yet
            val insertStatement = getInsertStatement(fizzBuzz)
            insertStatement.executeUpdate()

            val generatedKeys = insertStatement.generatedKeys
            if (generatedKeys.next()) {
                return@withContext getFizzBuzzReturn(fizzBuzz)
            } else {
                throw Exception("Unable to retrieve the id of the newly inserted fizzbuzz")
            }

        }

    }

    suspend fun getMostPopular(): FizzBuzz = withContext(Dispatchers.IO) {
        val selectStatement = connection.prepareStatement(SELECT_MOST_POPULAR)
        val resultSet = selectStatement.executeQuery()
        if (resultSet.next()) {
            val int1 = resultSet.getInt("int1")
            val int2 = resultSet.getInt("int2")
            val limit = resultSet.getInt("ceiling")
            val str1 = resultSet.getString("str1")
            val str2 = resultSet.getString("str2")
            val counter = resultSet.getInt("counter")

            return@withContext FizzBuzz(int1, int2, limit, str1, str2, counter)
        } else {
            throw Exception("Record not found")
        }
    }

    private fun getSelectStatement( fizzBuzz: FizzBuzz): PreparedStatement{
        val selectStatement = connection.prepareStatement(SELECT_FIIZBUZZ)
        selectStatement.setInt(1, fizzBuzz.int1)
        selectStatement.setInt(2, fizzBuzz.int2)
        selectStatement.setInt(3, fizzBuzz.limit)
        selectStatement.setString(4, fizzBuzz.str1)
        selectStatement.setString(5, fizzBuzz.str2)
        return selectStatement
    }

    private fun getUpdateStatement(counter: Int, id: Int): PreparedStatement{
        val updateStatement = connection.prepareStatement(UPDATE_FIIZBUZZ)
        updateStatement.setInt(1, counter + 1)
        updateStatement.setInt(2, id)
        return updateStatement
    }

    private fun getInsertStatement(fizzBuzz: FizzBuzz):PreparedStatement{
        val insertStatement = connection.prepareStatement(INSERT_FIZZBUZZ, Statement.RETURN_GENERATED_KEYS)
        insertStatement.setInt(1, fizzBuzz.int1)
        insertStatement.setInt(2, fizzBuzz.int2)
        insertStatement.setInt(3, fizzBuzz.limit)
        insertStatement.setString(4, fizzBuzz.str1)
        insertStatement.setString(5, fizzBuzz.str2)
        insertStatement.setInt(6, 1)
        return insertStatement
    }



    private fun getFizzBuzzReturn(fizzBuzz: FizzBuzz): List<String> {
        val output = mutableListOf<String>()
        for (i in 0..fizzBuzz.limit) {
            if (i % (fizzBuzz.int1 * fizzBuzz.int2) == 0) {
                output.add("${fizzBuzz.str1}${fizzBuzz.str2}")
            } else if (i % fizzBuzz.int1 == 0) {
                output.add(fizzBuzz.str1)
            } else if (i % fizzBuzz.int2 == 0) {
                output.add(fizzBuzz.str2)
            } else {
                output.add(i.toString())
            }
        }
        return output
    }

}


