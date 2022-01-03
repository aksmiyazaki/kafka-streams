package com.github.aksmiyazaki.moneytransactions.aggregator

import com.github.aksmiyazaki.moneytransactions.{AccountStatus, Transaction}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant

class TransactionsAggregatorTest extends AnyFlatSpec with Matchers {

  it should "correctly add a single transaction to status" in {
    val currentStatus = AccountStatus("0.0", 0)
    val ts = Instant.ofEpochSecond(123123)
    val firstTransaction = Transaction("10.123", "deposit", ts)

    val res = TransactionsAggregator.treatTransaction("test", firstTransaction, currentStatus)

    res.balance shouldEqual "10.123"
    res.number_of_deposits shouldEqual 1
    res.last_transaction_timestamp shouldEqual ts
  }

  it should "correctly add a transaction with small value" in {
    val transactionTs = Instant.ofEpochSecond(123123123)
    val currentStatus = AccountStatus("24.0", 1, Instant.ofEpochSecond(123))
    val firstTransaction = Transaction("0.00001", "deposit", transactionTs)

    val res = TransactionsAggregator.treatTransaction("test", firstTransaction, currentStatus)

    res.balance shouldEqual "24.00001"
    res.number_of_deposits shouldEqual 2
    res.last_transaction_timestamp shouldEqual transactionTs
  }
}
