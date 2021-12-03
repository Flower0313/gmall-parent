package com.atguigu.test

/**
 * @ClassName gmall-parent-Student 
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月03日18:27 - 周五
 * @Describe
 */
case class Student(var name: String = null,
                   var age: Int = 0
                  ) {
  def this(name: String) {
    this()
    mergeName(name)
  }
  println("hah")

  def show(): Unit = {
    println("name:" + name + ",age:" + age)
  }

  def mergeName(name: String): Unit = {
    this.name = name
  }
}

object CaseDemo {
  def main(args: Array[String]): Unit = {
    val flower: Student = Student("flower")
    flower.show()
  }
}