package SparkCore_04
/*注:样例类和隐式函数必须在一个类中,隐式函数必须在object类中声明和实现*/
object MyOrdering{
  implicit val Teacherording: Ordering[Teacher] = new Ordering[Teacher] {
    override def compare(x: Teacher, y: Teacher): Int = {
      if (x.face != y.face) {
        x.face - y.face
      } else {
        y.age - x.age
      }
    }
  }
}
case class Teacher(face:Int,age:Int)
