// Databricks notebook source
var mylist = List.range(1, 45)
println(mylist(0))
println("List is: " + mylist)
println(mylist.sum)
println((mylist.sum)/4)
var divi4 = mylist.filter(_ % 4 == 0)
println(divi4)
println(divi4.sum)

// COMMAND ----------

var divi3 = mylist.filter(each => each % 3 == 0 && each < 20)

def sqr(x: Int) = x * x
println(divi3)
var divi3sq = divi3.map(sqr)
println(divi3sq)
println(divi3sq.sum)

// COMMAND ----------

def max(a: Int, b: Int, c: Int) = {
   def get_max(x: Int, y: Int) = if (x > y) x else y
   get_max(a, get_max(b, c))
  }
println(max(7,4,5)
)
