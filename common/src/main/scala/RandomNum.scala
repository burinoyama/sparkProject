import scala.util.Random

object RandomNum {

  def apply(fromNum:Int, toNum:Int) ={
    fromNum + new Random().nextInt(toNum - fromNum + 1)
  }


  def create(from:Int, toNum:Int, amount:Int, delimiter:String, repeat:Boolean): Unit = {
    println(apply(1, 10))
  }


  def main(args: Array[String]): Unit = {
//    create(1, 10, 3, ",", Boolean)

  }












//  RandomOptions()

}
