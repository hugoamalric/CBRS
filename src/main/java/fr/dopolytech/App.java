package fr.dopolytech;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Query;

import static org.neo4j.driver.Values.parameters;

public class App implements AutoCloseable {
    private final Driver driver;

    public App(String uri) {
        driver = GraphDatabase.driver(uri, AuthTokens.none());
    }

    @Override
    public void close() throws RuntimeException {
        driver.close();
    }

    public void cleanDatabase() {
        try (org.neo4j.driver.Session session = driver.session()) {
            session.executeWriteWithoutResult(tx -> {
                System.out.println("Cleaning database...");
                tx.run("MATCH (a)-[r]->() DELETE a, r").consume();
            });
        }
    }

    /**
     * Create an import function that imports movies.csv data to form such Graph patterns:
     * - two nodes (movie:Movie {movieId, title}) and (g:Genre {genre})
     * - edge between : (Movie)-[:HAS]->(Genre)
     */
    public void importMovies() {
        try (var session = driver.session()) {
            session.executeWriteWithoutResult(tx -> {
                System.out.println("Create movie constraint...");
                var queryConstraintString = "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Movie) REQUIRE m.movieId IS UNIQUE";
                tx.run(queryConstraintString).consume();
            });

            session.executeWriteWithoutResult(tx -> {
                System.out.println("Importing movies.csv...");
                var queryLoadCSVString = "LOAD CSV WITH HEADERS FROM 'file:///ml-latest-small/movies.csv' AS row RETURN row";
                var queryCypherString = "CREATE (m:Movie {movieId: row.movieId, title: row.title}) FOREACH (genre IN split(row.genres, '|') | MERGE (g:Genre {genre: genre}) MERGE (m)-[:HAS]->(g))";
                var query = new Query("CALL apoc.periodic.iterate("+ "\"" + queryLoadCSVString + "\",\"" + queryCypherString + "\"" +",{batchSize:2000, parallel:true, retries: 3})");
                tx.run(query).consume();
            });
        }
    }

    /**
     * Create an import function that imports ratings.csv data to form such Graph patterns:
     * - one nodes (user:User {userId})
     * - edge between : (User)-[RATED {rating, timestamp}]->(Movie)
     */
    public void importRatings() {
        try (var session = driver.session()) {
            session.executeWriteWithoutResult(tx -> {
                System.out.println("Create user constraint...");
                var queryConstraintString = "CREATE CONSTRAINT IF NOT EXISTS FOR (u:User) REQUIRE u.userId IS UNIQUE";
                tx.run(queryConstraintString).consume();
            });

            session.executeWriteWithoutResult(tx -> {
                System.out.println("Importing ratings.csv...");
                var queryLoadCSVString = "LOAD CSV WITH HEADERS FROM 'file:///ml-latest-small/ratings.csv' AS row RETURN row";
                var queryCypherString = "MERGE (u:User {userId: row.userId}) MERGE (m:Movie {movieId: row.movieId}) MERGE (u)-[r:RATED {rating: row.rating, timestamp: row.timestamp}]->(m)";
                var query = new Query("CALL apoc.periodic.iterate(" + "\"" + queryLoadCSVString + "\",\"" + queryCypherString + "\"" + ",{batchSize:20000, parallel:true, retries: 5})");
                tx.run(query).consume();
            });
        }
    }

    public static void main(String... args) {
        try (var app = new App("bolt://localhost:7687")) {
            app.cleanDatabase();
            app.importMovies();
            app.importRatings();
        }
    }
}
