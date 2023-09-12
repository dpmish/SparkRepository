
package bank

import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace

object DebitCreditTranaction extends App {
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  
  sparkConf.set("spark.app.name"," Employee_Record")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
 
   val readDF= spark.read.format("csv")
               .option("header","true").option("inferschema","true")
               .option("path","G:/Mydata/bank/personal_transactions.csv").load()
               
           readDF.printSchema()
    
           readDF.show()
           
          import spark.implicits._
           
    val df2 = readDF.withColumn("amount_chk", when(col("Transaction Type") === ("debit"), ($"Amount")*(-1)).otherwise (col("Amount")) )     
          
    df2.show()
    
    val df3 = df2.groupBy("Customer_No").agg(sum("amount_chk").alias("Total_Bal"))
    
    df3.show()
     spark.stop()      
  
  
  
}