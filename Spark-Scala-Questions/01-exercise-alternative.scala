
class Salaries(val sal:Seq[Double]) {

  def doubleThoseSalaries():Seq[Double] = {
    sal.map(salario => salario*2)
  }

}

object Ejercicio {
  def main(args:Array[String]) {

    val seqSalaries = Seq(1000.00, 1500.90)
    val output = new Salaries(seqSalaries).doubleThoseSalaries()
    output.foreach(println)
  }
}
