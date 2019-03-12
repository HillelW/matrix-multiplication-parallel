/**
 *
 * @author Hillel Wolin
 *
 * Program to multiply an 8 x 8 matrix by an 8 x 8 matrix. Using Akka actors, we
 * create a virtual ring of four processors such that each processor gets a two row
 * block from each matrix. The parallel algorithm is provided in Casanova p. 111.
 *
 * The program works by first having a supervisor broadcast an entire
 * matrix block from each of the two matrices to be multiplied to each processor.
 * Each processor keeps the first matrix block throughout the remainder of the program.
 *
 * Once a given process has hit an iteration number of 3, it relays its
 * final sum result back to the supervisor. The supervisor accumulates the sums in its
 * own state variables and prints the result matrix once all of the processes have finished.
 *
 * Given our initial values, the result should be a an 8 x 8 matrix, each of whose rows are:
 * (0, 28, 56, 84, 112, 140, 168, 196).
 */

package multithreading.project

import akka.actor._
import Array._

//case classes for different message types
//for use before the result has been computed
case class MatrixMultiply(matrix1: Array[Array[Int]], matrix2: Array[Array[Int]])
case class BroadcastBlocks(matrix1: Array[Array[Int]], matrix2: Array[Array[Int]])
case class UpdateMatrix2Block(matrix: Array[Array[Int]])
//for use after the result has been computed
case class Process0ToSupervisor(matrix: Array[Array[Int]])
case class Process1ToSupervisor(matrix: Array[Array[Int]])
case class Process2ToSupervisor(matrix: Array[Array[Int]])
case class Process3ToSupervisor(matrix: Array[Array[Int]])
case class PrintAnswer()

class Processor0 extends Actor {
  //state variables
  val processorNumber = 0
  val numberOfProcessors = 4
  val numberOfRows = 2
  var partialSum: Array[Array[Int]] = Array(
    Array(0, 0, 0, 0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0, 0, 0, 0))
  var matrix1Block: Array[Array[Int]] = _
  var matrix2Block: Array[Array[Int]] = _
  //supervisor
  val supervisor: ActorSelection = context.actorSelection("..")
  //next actor in the ring
  var nextActor: ActorSelection = context.actorSelection("../processor1")
  var iterationNumber = 0

  //there are two possible cases: either a processor receives two matrix blocks from
  //the supervisor, or just a single matrix block from another processor in the ring
  def receive = {

    //upon receiving two matrix blocks from the supervisor
    case matrixAndMatrix: BroadcastBlocks => {
      //initialize state variables
      matrix1Block = matrixAndMatrix.matrix1
      matrix2Block = matrixAndMatrix.matrix2

      //for testing: println(s"iteration $iterationNumber in processor0:")

      // to get the positive modulus and avoid an IndexOutOfBounds error, add the divisor
      //to the negative modulus in the case that (processNumber - iterationNumber) is negative
      for (l <- 0 until 4) {
        for (i <- 0 until 2) {
          for (j <- 0 until 2) {
            for (k <- 0 until 2) {

              if ((processorNumber - iterationNumber) < 0)
                partialSum(i)(l * 2 + j) += matrix1Block(i)(((((processorNumber - iterationNumber) % numberOfProcessors) + numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)
              else
                partialSum(i)(l * 2 + j) += matrix1Block(i)((((processorNumber - iterationNumber) % numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)

            }
          }
        }
      }

      iterationNumber += 1

      //after partial sum is computed, send current vector block to
      //the next processor
      nextActor ! UpdateMatrix2Block(matrix2Block)

    }
    //upon receiving one matrix block
    case matrixWrapper: UpdateMatrix2Block => {
      //for testing: println(s"iteration $iterationNumber in processor0 matrix only:")

      //update matrix block
      matrix2Block = matrixWrapper.matrix

      //for testing: println(s"iteration $iterationNumber in processor0:")

      //compute new partial sums based on iteration number
      for (l <- 0 until 4) {
        for (i <- 0 until 2) {
          for (j <- 0 until 2) {
            for (k <- 0 until 2) {

              if ((processorNumber - iterationNumber) < 0)
                partialSum(i)(l * 2 + j) += matrix1Block(i)(((((processorNumber - iterationNumber) % numberOfProcessors) + numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)
              else
                partialSum(i)(l * 2 + j) += matrix1Block(i)((((processorNumber - iterationNumber) % numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)

            }
          }
        }
      }

      iterationNumber += 1

      //if the algorithm has not completed, send to next actor in the ring
      if (iterationNumber < 4)
        nextActor ! UpdateMatrix2Block(matrix2Block)

      //otherwise, send result to supervisor
      else
        supervisor ! Process0ToSupervisor(partialSum)

    }
  }
}

class Processor1 extends Actor {
  //state variables
  val processorNumber = 0
  val numberOfProcessors = 4
  val numberOfRows = 2
  var partialSum: Array[Array[Int]] = Array(
    Array(0, 0, 0, 0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0, 0, 0, 0))
  var matrix1Block: Array[Array[Int]] = _
  var matrix2Block: Array[Array[Int]] = _
  //supervisor
  val supervisor: ActorSelection = context.actorSelection("..")
  //next actor in the ring
  var nextActor: ActorSelection = context.actorSelection("../processor2")
  var iterationNumber = 0

  //there are two possible cases: either a processor receives two matrix blocks from
  //the supervisor, or just a single matrix block from another processor in the ring
  def receive = {

    //upon receiving two matrix blocks from the supervisor
    case matrixAndMatrix: BroadcastBlocks => {
      //initialize state variables
      matrix1Block = matrixAndMatrix.matrix1
      matrix2Block = matrixAndMatrix.matrix2

      //for testing: println(s"iteration $iterationNumber in processor1:")

      // to get the positive modulus and avoid an IndexOutOfBounds error, add the divisor
      //to the negative modulus in the case that (processNumber - iterationNumber) is negative
      for (l <- 0 until 4) {
        for (i <- 0 until 2) {
          for (j <- 0 until 2) {
            for (k <- 0 until 2) {

              if ((processorNumber - iterationNumber) < 0)
                partialSum(i)(l * 2 + j) += matrix1Block(i)(((((processorNumber - iterationNumber) % numberOfProcessors) + numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)
              else
                partialSum(i)(l * 2 + j) += matrix1Block(i)((((processorNumber - iterationNumber) % numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)

            }
          }
        }
      }

      iterationNumber += 1

      //after partial sum is computed, send current vector block to
      //the next processor
      nextActor ! UpdateMatrix2Block(matrix2Block)

    }
    //upon receiving one matrix block
    case matrixWrapper: UpdateMatrix2Block => {
      //for testing: println(s"iteration $iterationNumber in processor0 matrix only:")

      //update matrix block
      matrix2Block = matrixWrapper.matrix

      //for testing: println(s"iteration $iterationNumber in processor1:")

      //compute new partial sums based on iteration number
      for (l <- 0 until 4) {
        for (i <- 0 until 2) {
          for (j <- 0 until 2) {
            for (k <- 0 until 2) {

              if ((processorNumber - iterationNumber) < 0)
                partialSum(i)(l * 2 + j) += matrix1Block(i)(((((processorNumber - iterationNumber) % numberOfProcessors) + numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)
              else
                partialSum(i)(l * 2 + j) += matrix1Block(i)((((processorNumber - iterationNumber) % numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)

            }
          }
        }
      }

      iterationNumber += 1

      //if the algorithm has not completed, send to next actor in the ring
      if (iterationNumber < 4)
        nextActor ! UpdateMatrix2Block(matrix2Block)

      //otherwise, send result to supervisor
      else
        supervisor ! Process1ToSupervisor(partialSum)

    }
  }
}

class Processor2 extends Actor {
  //state variables
  val processorNumber = 0
  val numberOfProcessors = 4
  val numberOfRows = 2
  var partialSum: Array[Array[Int]] = Array(
    Array(0, 0, 0, 0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0, 0, 0, 0))
  var matrix1Block: Array[Array[Int]] = _
  var matrix2Block: Array[Array[Int]] = _
  //supervisor
  val supervisor: ActorSelection = context.actorSelection("..")
  //next actor in the ring
  var nextActor: ActorSelection = context.actorSelection("../processor3")
  var iterationNumber = 0

  //there are two possible cases: either a processor receives two matrix blocks from
  //the supervisor, or just a single matrix block from another processor in the ring
  def receive = {

    //upon receiving two matrix blocks from the supervisor
    case matrixAndMatrix: BroadcastBlocks => {
      //initialize state variables
      matrix1Block = matrixAndMatrix.matrix1
      matrix2Block = matrixAndMatrix.matrix2

      //for testing:  println(s"iteration $iterationNumber in processor2:")

      // to get the positive modulus and avoid an IndexOutOfBounds error, add the divisor
      //to the negative modulus in the case that (processNumber - iterationNumber) is negative
      for (l <- 0 until 4) {
        for (i <- 0 until 2) {
          for (j <- 0 until 2) {
            for (k <- 0 until 2) {

              if ((processorNumber - iterationNumber) < 0)
                partialSum(i)(l * 2 + j) += matrix1Block(i)(((((processorNumber - iterationNumber) % numberOfProcessors) + numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)
              else
                partialSum(i)(l * 2 + j) += matrix1Block(i)((((processorNumber - iterationNumber) % numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)

            }
          }
        }
      }

      iterationNumber += 1

      //after partial sum is computed, send current vector block to
      //the next processor
      nextActor ! UpdateMatrix2Block(matrix2Block)

    }
    //upon receiving one matrix block
    case matrixWrapper: UpdateMatrix2Block => {
      //for testing: println(s"iteration $iterationNumber in processor2 matrix only:")
      //update matrix block
      matrix2Block = matrixWrapper.matrix

      //compute new partial sums based on iteration number
      for (l <- 0 until 4) {
        for (i <- 0 until 2) {
          for (j <- 0 until 2) {
            for (k <- 0 until 2) {

              if ((processorNumber - iterationNumber) < 0)
                partialSum(i)(l * 2 + j) += matrix1Block(i)(((((processorNumber - iterationNumber) % numberOfProcessors) + numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)
              else
                partialSum(i)(l * 2 + j) += matrix1Block(i)((((processorNumber - iterationNumber) % numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)
              //println("Column index is: " + ( ( (processorNumber - iterationNumber) % numberOfProcessors) * numberOfRows)+j)
            }
          }
        }
      }

      iterationNumber += 1

      //if the algorithm has not completed, send to next actor in the ring
      if (iterationNumber < 4)
        nextActor ! UpdateMatrix2Block(matrix2Block)

      //otherwise, send result to supervisor
      else
        supervisor ! Process2ToSupervisor(partialSum)

    }
  }
}

class Processor3 extends Actor {
  //state variables
  val processorNumber = 0
  val numberOfProcessors = 4
  val numberOfRows = 2
  var partialSum: Array[Array[Int]] = Array(
    Array(0, 0, 0, 0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0, 0, 0, 0))
  var matrix1Block: Array[Array[Int]] = _
  var matrix2Block: Array[Array[Int]] = _
  //supervisor
  val supervisor: ActorSelection = context.actorSelection("..")
  //next actor in the ring
  var nextActor: ActorSelection = context.actorSelection("../processor0")
  var iterationNumber = 0

  //there are two possible cases: either a processor receives two matrix blocks from
  //the supervisor, or just a single matrix block from another processor in the ring
  def receive = {

    //upon receiving two matrix blocks from the supervisor
    case matrixAndMatrix: BroadcastBlocks => {
      //initialize state variables
      matrix1Block = matrixAndMatrix.matrix1
      matrix2Block = matrixAndMatrix.matrix2

      //for testing: println(s"iteration $iterationNumber in processor3:")

      // to get the positive modulus and avoid an IndexOutOfBounds error, add the divisor
      //to the negative modulus in the case that (processNumber - iterationNumber) is negative
      for (l <- 0 until 4) {
        for (i <- 0 until 2) {
          for (j <- 0 until 2) {
            for (k <- 0 until 2) {

              if ((processorNumber - iterationNumber) < 0)
                partialSum(i)(l * 2 + j) += matrix1Block(i)(((((processorNumber - iterationNumber) % numberOfProcessors) + numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)
              else
                partialSum(i)(l * 2 + j) += matrix1Block(i)((((processorNumber - iterationNumber) % numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)

            }
          }
        }
      }

      iterationNumber += 1

      //after partial sum is computed, send current vector block to
      //the next processor
      nextActor ! UpdateMatrix2Block(matrix2Block)

    }
    //upon receiving one matrix block
    case matrixWrapper: UpdateMatrix2Block => {
      //for testing: println(s"iteration $iterationNumber in processor3 matrix only:")
      //update matrix block
      matrix2Block = matrixWrapper.matrix

      //compute new partial sums based on iteration number
      for (l <- 0 until 4) {
        for (i <- 0 until 2) {
          for (j <- 0 until 2) {
            for (k <- 0 until 2) {

              if ((processorNumber - iterationNumber) < 0)
                partialSum(i)(l * 2 + j) += matrix1Block(i)(((((processorNumber - iterationNumber) % numberOfProcessors) + numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)
              else
                partialSum(i)(l * 2 + j) += matrix1Block(i)((((processorNumber - iterationNumber) % numberOfProcessors) * numberOfRows) + k) * matrix2Block(k)(l * 2 + j)
              //println("Column index is: " + ( ( (processorNumber - iterationNumber) % numberOfProcessors) * numberOfRows)+j)
            }
          }
        }
      }

      iterationNumber += 1

      //if the algorihm has not completed, send to next actor in the ring
      if (iterationNumber < 4)
        nextActor ! UpdateMatrix2Block(matrix2Block)

      //otherwise, send result to supervisor
      else
        supervisor ! Process3ToSupervisor(partialSum)

    }
  }
}

class Supervisor extends Actor {
  //final results
  var resultFromProcessor0 = Array(
    Array(0, 0, 0, 0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0, 0, 0, 0))
  var resultFromProcessor1 = Array(
    Array(0, 0, 0, 0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0, 0, 0, 0))
  var resultFromProcessor2 = Array(
    Array(0, 0, 0, 0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0, 0, 0, 0))
  var resultFromProcessor3 = Array(
    Array(0, 0, 0, 0, 0, 0, 0, 0),
    Array(0, 0, 0, 0, 0, 0, 0, 0))

  //start four children that comprise the ring. The names supplied are
  //used in the path
  val processor0 = context.actorOf(Props[Processor0], name = "processor0")
  val processor1 = context.actorOf(Props[Processor1], name = "processor1")
  val processor2 = context.actorOf(Props[Processor2], name = "processor2")
  val processor3 = context.actorOf(Props[Processor3], name = "processor3")

  def receive = {

    //if the client tells the supervisor to multiply two matrices, broadcast the
    //appropriate matrix blocks to the processors
    case matrixAndMatrix: MatrixMultiply => {

      //for testing: println("in supervisor:")

      //obtain the two matrices
      val matrix1 = matrixAndMatrix.matrix1
      val matrix2 = matrixAndMatrix.matrix2

      //obtain the blocks of the first matrix
      val matrix1Block1 = matrix1.slice(0, 2)
      val matrix1Block2 = matrix1.slice(2, 4)
      val matrix1Block3 = matrix1.slice(4, 6)
      val matrix1Block4 = matrix1.slice(6, 8)

      //obtain the blocks of the second matrix
      val matrix2Block1 = matrix2.slice(0, 2)
      val matrix2Block2 = matrix2.slice(2, 4)
      val matrix2Block3 = matrix2.slice(4, 6)
      val matrix2Block4 = matrix2.slice(6, 8)

      //broadcast matrix blocks to each processor
      processor0 ! BroadcastBlocks(matrix1Block1, matrix2Block1)
      processor1 ! BroadcastBlocks(matrix1Block2, matrix2Block2)
      processor2 ! BroadcastBlocks(matrix1Block3, matrix2Block3)
      processor3 ! BroadcastBlocks(matrix1Block4, matrix2Block4)
    }
    //if response from a processor, save to state variable
    case response: Process0ToSupervisor => {

      resultFromProcessor0 = response.matrix

    }
    case response: Process1ToSupervisor => {

      resultFromProcessor1 = response.matrix

    }
    case response: Process2ToSupervisor => {

      resultFromProcessor2 = response.matrix

    }
    case response: Process3ToSupervisor => {

      resultFromProcessor3 = response.matrix

    }

    //if requested to print answer
    case request: PrintAnswer => {
      //aggregate all of the partial results into a single matrix
      val result = Array(resultFromProcessor0(0), resultFromProcessor0(1),
        resultFromProcessor1(0), resultFromProcessor1(1),
        resultFromProcessor2(0), resultFromProcessor0(1),
        resultFromProcessor3(0), resultFromProcessor3(1))

      println
      println("The result of the multiplication is the following matrix:\n")
      for (i <- 0 until 8) {
        for (j <- 0 until 8) {
          print(" " + result(i)(j));
        }
        println
      }
    }
  }
}

//driver program
object RingTest3 extends App {
  //both matrices will be 8 x 8
  val rows = 8
  val columns = 8

  //declare two 8 x 8 matrices
  var myMatrix1 = ofDim[Int](rows, columns)
  var myMatrix2 = ofDim[Int](rows, columns)

  //initialize both matrices with the same numbers
  for (i <- 0 until rows) {
    for (j <- 0 until columns) {
      myMatrix1(i)(j) = j;
      myMatrix2(i)(j) = j;
    }
  }

  //Print test-matrices
  println("First test matrix:")
  println
  for (i <- 0 until rows) {
    for (j <- 0 until columns) {
      print(" " + myMatrix1(i)(j));
    }
    println
  }

  println
  println("Second test matrix:")
  println
  for (i <- 0 until rows) {
    for (j <- 0 until columns) {
      print(" " + myMatrix1(i)(j));
    }
    println
  }
  println

  //create actor system
  val system = ActorSystem("MatrixRingTest")

  //create a parent of the ring
  val supervisor = system.actorOf(Props[Supervisor], name = "supervisor") //name used in path

  //tell the supervisor to multiply the two matrices
  supervisor ! MatrixMultiply(myMatrix1, myMatrix2)

  //wait for computation to complete
  Thread.sleep(1000)

  //tell supervisor to print the answer
  supervisor ! PrintAnswer()

  //shutdown the system
  system.shutdown()
}

  
 
