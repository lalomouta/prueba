
object DoubleSalaries {

  def increaseSalaries(salaries:Seq[Double]):Seq[Double] = {
    salaries.map(salario => salario *2)

  }

  def main(args: Array[String]):Unit ={
    
    val salaries = Seq(1001.00, 2756.33)

    val output = increaseSalaries(salaries)

    output.foreach(println) 
  
  }
}


