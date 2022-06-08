/*
  Quantexa Copyright Statement
 */

package com.quantexa.assignments.transactions

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Try

/***
  * This Object holds the functions required for the Quantexa coding exercise and an entry point to execute them.
  * Once executed each question is executed in turn printing results to console
  */
  
object TransactionAssignment extends App {

  /***
    * A case class to represent a transaction
    * @param transactionId The transaction Identification String
    * @param accountId The account Identification String
    * @param transactionDay The day of the transaction
    * @param category The category of the transaction
    * @param transactionAmount The transaction value
    */
  case class Transaction(
                          transactionId: String,
                          accountId: String,
                          transactionDay: Int,
                          category: String,
                          transactionAmount: Double)

  case class Question1Result (transactionDay : Int,
                              transactionTotal: Double
                             )

  case class Question2Result( accountId: String,
                              categoryAvgValueMap: Map[String,Double]
                            )

  case class Question3Result ( transactionDay: Int,
                               accountId: String,
                               max: Double,
                               avg: Double,
                               aaTotal: Double,
                               ccTotal: Double,
                               ffTotal:Double
                             )

  //The full path to the file to import
  val fileName = getClass.getResource("/transactions.csv").getPath

  //The lines of the CSV file (dropping the first to remove the header)
  //  Source.fromInputStream(getClass.getResourceAsStream("/transactions.csv")).getLines().drop(1)
  val transactionLines: Iterator[String] = Source.fromFile(fileName).getLines().drop(1)

  //Here we split each line up by commas and construct Transactions
  val transactions: List[Transaction] = transactionLines.map { line =>
    val split = line.split(',')
    Transaction(split(0), split(1), split(2).toInt, split(3), split(4).toDouble)
  }.toList


  //Question 1
  val dailyTransactions = transactions.groupBy(a => a.transactionDay)
  //calculating the sum for each day
  val transactionValue = dailyTransactions.map(a => (a._1, a._2.map(a => a.transactionAmount).sum))
  //getting the day and corresponding sum for that day
  val question1answer = transactionValue.map(a => {
    val transactionDay = a._1
    val transactionTotal = a._2
    Question1Result(transactionDay, transactionTotal)
  })

  val question1ResultValue = question1answer.toSeq //Final output for question1 Seq[Question1Result]

  println(question1ResultValue)

  //Question 2
//group by account
  val transactionType = transactions.groupBy(a => a.category)
  val numberOfTransactions = transactionType.map(a => (a._2.length)).toSeq
  val sumOfTransactions = transactionType.map(a => a._2.map(a => a.transactionAmount).sum).toSeq
  //val average = sumOfTransactions / numberOfTransactions

  val average = sumOfTransactions.zip(numberOfTransactions).map{
    case (a, b) => a/b.toDouble
  }

  val categoryAvgValueMap = transactionType.map(a => a._1).zip(average).toMap
  val accountId = transactionType.map(a => a._2.map(a => {
    val accountId = a.accountId
    Question2Result(accountId, categoryAvgValueMap)
  }))

  val Question2ResultValue = accountId.flatten.toSeq



 println(Question2ResultValue)

  //Question 3


  val groupedAccountTransactions = transactions.groupBy(a => a.accountId).toSeq


  //println(groupedAccountTransactions)

  val grouped = groupedAccountTransactions.map(a => a._2)


  val statistics = grouped.flatten


  var days = 29
  def stats(list: Seq[Transaction], Day: Int): Seq[Question3Result] = {
    val minimumFive = Day - 5
    val filtered = list.filter(transac => transac.transactionDay >= minimumFive && transac.transactionDay < Day)

    val map = filtered.groupBy(a=> a.accountId) // gives the previous five days accounts

   val fields = map.values.map(b => b.map(a => {
      val accountId = a.accountId //required field
      val transactionId = a.transactionId //required field
      val day = a.transactionDay //required field
      val avgNumTransactions = b.length
      val avgTotal = b.map(a => a.transactionAmount).sum
      val average = avgTotal / avgNumTransactions //required field
      val maximum = b.map(a => a.transactionAmount).max // required field

      //total  by category AA CC FF
      val AA = b.filter(cat => cat.category.contains("AA"))
      val AAtotals = AA.map(cat => cat.transactionAmount).sum
      val CC = b.filter(cat => cat.category.contains("CC"))
      val CCtotals = CC.map(cat => cat.transactionAmount).sum
      val FF = b.filter(cat => cat.category.contains("FF"))
      val FFtotals = FF.map(cat => cat.transactionAmount).sum



     //println("day: " + day + " accountId: " + accountId + " maximum " + maximum )
      Question3Result(day,accountId, maximum, average, AAtotals, CCtotals,FFtotals)
    }))

val finalSeq = fields.flatten.toSeq.sortBy(a => a.transactionDay)
    println(finalSeq)
    //println(days)
    finalSeq //final output!

  }

  stats(statistics, days) //
  //stats(statistics, 10)
























 

}