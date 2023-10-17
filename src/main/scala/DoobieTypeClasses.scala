import DoobieDemo.Movie
import cats.effect.{ExitCode, IO, IOApp}
import doobie.{ConnectionIO, Query0}
import doobie.implicits.toSqlInterpolator
import doobie.util.{Get, Put, Read, Write, fragment}
import doobie.util.transactor.Transactor
import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits.catsSyntaxApplicativeId
import doobie.{ConnectionIO, HC, HPS}
import doobie.util.transactor.Transactor
import doobie.implicits._
import doobie.HC._
import doobie.util.update.Update

import java.util.UUID
object DoobieTypeClasses extends IOApp {




  /*
  TODO
       So far, we have seen many examples of usages
       of the sql interpolator, which magically can
       convert Scala types into JDBC types
       when reading input parameters,
       and vice versa when concerning
       mapping values extracted from the database.
TODO
       As we can imagine, there is no magic whatsoever.
       As skilled Scala developers,
       we should have known that
       there are some type classes
       behind it whenever someone talks about magic
       In fact, Doobie basically uses four type classes
        for the conversion between Scala and JDBC types: Get[A], Put[A], Read[A] and Write[A].
   */

  type ActorID= Int
  type MovieID= String
  type DirectorId= String
  type Name= String
  type Title= String
  type Id= Int


  class ActorName(val value: String){
    override def toString: String = value
  }
  implicit class IODecorator[A](io: IO[A]) {
    def debug: IO[A] = for {
      a <- io
      t = Thread.currentThread().getName
      _ = println(s"[$t] $a")
    } yield a
  }

  val xa:Transactor[IO]= Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    "jdbc:postgresql://localhost:5432/myimdb",
    "docker",
    "docker"
  )




  object ActorName{
    implicit val actorNameGet:Get[ActorName]= Get[String].map(string=> new ActorName(string))
    implicit val actorNamePut:Put[ActorName]= Put[String].contramap(actorName=> actorName.value)
  }
    def findALlActorNames:IO[List[ActorName]] ={

      val query: doobie.Query0[ActorName] = sql"select name from actors".query[ActorName]

      //Lift the DSL to FREE Monad
      val action: ConnectionIO[List[ActorName]] =query.to[List]
      //this is not working because Doobie not able to identify the custom class objects
      action.transact(xa)

    }

  case class DirectorID(id:Int)
  case class DirectorName(name:String)
  case class DirectorLastName(lastName:String)
  case class Director(id:DirectorID , name:DirectorName,lastName: DirectorLastName)

  //"value types "

  object Director{
    implicit val directorRead:Read[Director]= Read[(Int,String,String)].map{
      case(id,name,lastName) => Director(DirectorID(id),DirectorName(name),DirectorLastName(lastName))
    }

    implicit val directorWrite:Write[Director]= Write[(Int,String,String)].contramap{
      case Director(DirectorID(id),DirectorName(name),DirectorLastName(lastName)) => (id,name,lastName)
    }
  }

  def findAllDirectorsProgram(): IO[List[Director]] = {
    val findAllDirectors: fs2.Stream[ConnectionIO, Director] =
         sql"select id, name, last_name from directors"
        .query[Director]   // converting SQL to DSL Model
        .stream            // this will Lift the DSL to Stream of FREE Monad

    findAllDirectors.compile.toList.transact(xa)
  }
  import doobie.postgres._
  import doobie.postgres.implicits._
  import doobie.util.transactor.Transactor._

  /**
   *
   * Writing Large Complex queries in Doobie:->
   * Let’s say we want to find a movie by its name.
   * We want to retrieve also the director’s information
   * and the list of actors that played in the film.
   * Using the ER model
   * we have to join three tables: the movies table,
   * the directors table, and the actors table.
   * Here is how we can implement it in Doobie:
   */
  def findMovieByTitle(title: Title): IO[Option[Movie]] = {
    val query = sql"""
                     |SELECT m.id,
                     |       m.title,
                     |       m.year_of_production,
                     |       array_agg(a.name) as actors,
                     |       d.name
                     |FROM movies m
                     |JOIN movies_actors ma ON m.id = ma.movie_id
                     |JOIN actors a ON ma.actor_id = a.id
                     |JOIN directors d ON m.director_id = d.id
                     |WHERE m.title = $title
                     |GROUP BY (m.id,
                     |          m.title,
                     |          m.year_of_production,
                     |          d.name,
                     |          d.last_name)
                     |""".stripMargin
      .query[Movie]
      .option

    query.transact(xa)
  }


  def findMovieByTitlev_2(title: String): IO[Option[Movie]] = {

    def findMovieByTitle: ConnectionIO[Option[(UUID, String, ActorID, ActorID)]] =
      sql"""
           | select id, title, year_of_production, director_id
           | from movies
           | where title = $title""".stripMargin
        .query[(UUID, String, Int, Int)].option


    def findDirectorById(directorId: Int):ConnectionIO[Option[(String, String)]] =
          sql"select name, last_name from directors where id = $directorId"
        .query[(String, String)].option



    def findActorsByMovieId(movieId: UUID) =
      sql"""
           | select a.name
           | from actors a
           | join movies_actors ma on a.id = ma.actor_id
           | where ma.movie_id = $movieId
           |""".stripMargin
        .query[String]
        .to[List]



    val query = for {
      maybeMovie <- findMovieByTitle
      directors <- maybeMovie match {

        case Some((_, _, _, directorId)) => findDirectorById(directorId)
        case None => Option.empty[(String, String)].pure[ConnectionIO]

      }

      actors <- maybeMovie match {
        case Some((movieId, _, _, _)) => findActorsByMovieId(movieId)
        case None                     => List.empty[String].pure[ConnectionIO]
      }

    } yield {
      maybeMovie.map {
        case (id, title, year, _) =>
        val directorName = directors.head._1
        val directorLastName = directors.head._2
        Movie(id.toString, title, year, actors, s"$directorName $directorLastName")
      }
    }

    query.transact(xa)
  }

  override def run(args: List[String]): IO[ExitCode] =
    findMovieByTitlev_2("Zack Snyder's Justice League")
      .debug
      .as(ExitCode.Success)

}
