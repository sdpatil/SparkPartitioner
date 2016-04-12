package com.spnotes.spark

import java.util.Comparator

import org.apache.spark._
import org.apache.spark.rdd.RDD

/**
  * Created by sunilpatil on 4/11/16.
  */
case class Player(playerId: Long, name: String, country: String, club: String) extends Ordered[Player]{

  override def toString: String = {
    s"{playerId -> $playerId,  name -> $name, country ->$country, club ->$club }"
  }

  override def hashCode(): Int = {
    playerId.hashCode()
  }


  override def compare(that: Player): Int = {
    if(this.playerId < that.playerId)
      -1
    else if(this.playerId > that.playerId)
      1
    else
      0
  }
}

class PlayerCountryOrdering extends Ordering[Player]{
  override def compare(first: Player,second: Player): Int = {
    if(first.country < second.country)
      -1
    else if(first.country > second.country)
      1
    else
      0
  }
}

class MyPlayerPartitioner(numParts:Int) extends Partitioner{
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    var returnValue:Int = 0
    if(key.isInstanceOf[Player])
      returnValue= key.asInstanceOf[Player].club.hashCode % numPartitions
    else
      returnValue=  key.hashCode() %numPartitions
    if(returnValue < 0)
      returnValue =0
   // println(s"$key -> $returnValue")
    returnValue
  }
}

object SparkPartitioner extends Logging {

  def main(argv: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SparkPartitioner").setMaster("local")
    val sparkContext = new SparkContext(sparkConf)

    val playersRuns = List(
      (Player(1, "Sachin Tendulkar", "India", "Mumbai Indians"), List(100, 100, 100)),
      (Player(2, "Virat Kohli", "India", "Royal Challengers Banglore"), List(100, 0, 100)),
      (Player(3, "Mahendra Singh Dhoni", "India", "Rising Pune Supergiants"), List(100, 100, 0)),
      (Player(4, "Shikhar Dhawan", "India", "Sunrisers Hyderabad"), List(100, 0, 100)),
      (Player(5, "Ishant Sharma", "India", "Rising Pune Supergiants"), List(100, 0, 100)),
      (Player(6, "Ashwin", "India", "Rising Pune Supergiants"), List(100, 0, 100)),
      (Player(7, "AB de Villiers", "South Africa", "Royal Challengers Banglore"), List(0, 0, 100)),
      (Player(8, "chris gayle", "West Indies", "Royal Challengers Banglore"), List(100, 0, 0))
    )

    val players = sparkContext.parallelize(playersRuns)
    printPartitionedRDD(players)

    println("****Hash Partitioner****")
    val partitionedPlayers = players.partitionBy(new HashPartitioner(2)).persist()
    printPartitionedRDD(partitionedPlayers)

    println("****Range Partitioner****")
    val rangePartitionRDD = sparkContext.parallelize(List((Player(1,null,null,null),Player(4,null,null,null)),(Player(5,null,null,null),Player(8,null,null,null))))
    val rangePartitionedPlayers = players.partitionBy(new RangePartitioner(2,rangePartitionRDD)).persist()
    printPartitionedRDD(rangePartitionedPlayers)

    println("****Custom Partitioner****")
    val clubPartitionedPlayers = players.partitionBy(new MyPlayerPartitioner(3)).persist()
    printPartitionedRDD(clubPartitionedPlayers)
  }

  def printPartitionedRDD(players: RDD[(Player, List[Int])]): Unit = {
    players.mapPartitions { playerIt =>
      println("******Start Partition***********")
      playerIt.foreach { case (player, runs) =>
        println(s" $player")
        (player, runs)
      }
      println("******End Partition***********")
      playerIt
    }.count()
  }

}
