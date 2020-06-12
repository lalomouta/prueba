
object DoubleSalaries {

  def increaseSalaries(salaries:Seq[Double]):Seq[Double] = {
    salaries.map(salario => salario *2)

  }

  def main(args: Array[String]):Unit ={
   //suponemos que el input es una estructura de datos de tipo Seq 
    val salaries = Seq(1001.00, 2756.33)

    val output = increaseSalaries(salaries)

    output.foreach(println) 
  
  }
}


