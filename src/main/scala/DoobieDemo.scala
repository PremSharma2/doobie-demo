import cats.effect.{ExitCode, IO, IOApp}
import doobie.{ConnectionIO, HC, HPS}
import doobie.util.transactor.Transactor
import doobie.implicits._
import doobie.HC._
import doobie.util.fragment
import doobie.util.update.Update
object DoobieDemo extends IOApp{

  /**
  In Scala, we will use the following classes to define the domain objects:


   */
  type ActorID= Int
  type MovieID= String
  type Name= String
  type Letter= String
  type Director= String
  type Title= String
  type Year= Int
  case class Actor(id: ActorID, name: Name)
  case class Movie(id: MovieID, title: Title, year: Year, actors: List[String], director: Director)



  implicit class IODecorator[A](io: IO[A]) {
    def debug: IO[A] = for {
      a <- io
      t = Thread.currentThread().getName
      _ = println(s"[$t] $a")
    } yield a
  }


/*

 object Transactor {

    def apply[M[_], A0](
      kernel0: A0,
      connect0: A0 => Resource[M, Connection],
      interpret0: Interpreter[M],
      strategy0: Strategy
    ): Transactor.Aux[M, A0] = new Transactor[M] {
      type A = A0
      val kernel = kernel0
      val connect = connect0
      val interpret = interpret0
      val strategy = strategy0
    }

def apply[M[_]: Async](
        driver: String,
        url:    String,
        user:   String,
        pass:   String
      ): Transactor.Aux[M, Unit] =
        create(driver, () => DriverManager.getConnection(url, user, pass), Strategy.default)
 @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
      private def create[M[_]](
        driver: String,
        conn: () => Connection,
        strategy: Strategy
      )(implicit ev: Async[M]): Transactor.Aux[M, Unit] =
        Transactor(
          (),
          _ => {
            val acquire = ev.blocking{ Class.forName(driver); conn() }
            def release(c: Connection) = ev.blocking{ c.close() }
            Resource.make(acquire)(release)(ev)
          },
          KleisliInterpreter[M].ConnectionInterpreter,
          strategy
        )

 */


  val xa:Transactor[IO]= Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    "jdbc:postgresql://localhost:5432/myimdb",
    "docker",
    "docker"
  )


  def findAllActorNames:IO[List[String]] = {
    val query: doobie.Query0[String] = sql"select name from actors".query[String]
    val action: ConnectionIO[List[String]] = query.to[List]

    action.transact(xa)
  }



  def findActorById(id: ActorID): IO[Actor] = {
         val findActorById: ConnectionIO[Actor] =
         sql"select id, name from actors where id = $id"
        .query[Actor]
        .unique

    findActorById.transact(xa)
  }


  def findActorByIdSafe(id: ActorID): IO[Option[Actor]] = {
         val actor: ConnectionIO[Option[Actor]] =
         sql"select id, name from actors where id = $id"
        .query[Actor]
        .option

    actor.transact(xa)
  }


  val actorsNamesStream: fs2.Stream[ConnectionIO, String] =
       sql"select name from actors"
      .query[String]
      .stream



  val fetchAllActors: IO[List[String]] =
       sql"select name from actors".query[String]
      .stream
      .compile
      .toList
      .transact(xa)



  def findActorByName(name:Name):IO[Option[Actor]] ={
    val query="select id,name from actors where name = ?"

   val actorStream: fs2.Stream[ConnectionIO, Actor] =
     HC.stream[Actor](
      query,
      HPS.set(name),
      100
    )
          actorStream
          .compile
          .toList
          .map(_.headOption)
          .transact(xa)


  }


  /**
     Work with Fragments API using the fr String interpolator
   */

  def findActorsByNameInitialLetter(initialLetter: Letter): IO[List[Actor]]={
    val select: fragment.Fragment = fr"select id , name"
    val from=  fr"from actors"
    val where= fr"where Left(name,1)= $initialLetter"
    val statements: fragment.Fragment = select ++ from ++ where

      statements
      .query[Actor]
      .stream
      .compile
      .toList
      .transact(xa)

  }

  /**
      Update DB Operation it returns no of Rows affected
   */

  /*
  TODO
        As we can see, we continue to use the sql interpolator
        and its capabilities of dealing with input parameters.
         However, the update method returns
        an instance of the Update0 class. which is an DBOps
        It corresponds to the Query0 class whcih is also DbOps
        in the case of DMLs.
        We need to call one available method
       to get a ConnectionIO from an Update0.
       i.e Lift the Dbops to FreeMonad
       The easiest way to do this is to call the run method,
       which returns the number of updated rows
       inside the ConnectionIO the Free Monad.
   */

  def saveActor(actorID: ActorID,name: Name):IO[Int]={

    val query: fragment.Fragment = sql"insert into actors (id,name) values ($actorID , $name)"
        val persist: ConnectionIO[ActorID] = query.update.run
         persist.transact(xa)
  }


  def saveActor_v2(id: ActorID,name: Name):IO[Int]={

     val query = "insert into actors (id,name) values (?, ?)"
     val update: ConnectionIO[ActorID] = Update[Actor](query).run(Actor(id,name))
      update.transact(xa)
  }

  /**
   *
   * persisting the Actor with auto generated Keys
   */

  def saveActorAutoGenerated(name: String): IO[Int] = {

    val saveActor: ConnectionIO[Int] =

         sql"insert into actors (name) values ($name)"
        .update.withUniqueGeneratedKeys[Int]("id")

    saveActor.transact(xa)
  }

  /**
  Insert Update in a Batch
   */

  def saveMultipleActors(actorNames:List[String]): IO[List[Actor]] ={
    val insertStatement="insert into actors (name) values (?)"
   val updateAction: fs2.Stream[ConnectionIO, Actor] = Update[String](insertStatement)
     .updateManyWithGeneratedKeys[Actor]("id","name")(actorNames)

    updateAction.compile.toList.transact(xa)
  }

  override def run(args: List[String]): IO[ExitCode] =
    saveMultipleActors(List("Jai","Ram","Nizam","Rudresh"))
      .debug
      .as(ExitCode.Success)




}



