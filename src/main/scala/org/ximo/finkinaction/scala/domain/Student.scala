package org.ximo.finkinaction.scala.domain

/**
  *
  *
  * @author xikl
  * @date 2019/9/8
  */
case class Student(name: String, age: Int) {

  var id: Int = _

  def this(id: Int, name: String, age: Int) {
    this(name, age)
    this.id = id
  }

}


