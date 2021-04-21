import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

@ExtendWith(VertxExtension.class)
public class VertxConnectionPool {
	
	static JDBCPool pool;
	static PreparedQuery<RowSet<Row>> query;
	
	@BeforeAll
	static void createPoolAndPrepareQuery(Vertx vertx) {
		pool = JDBCPool.pool(vertx, new JsonObject()
				.put("driver_class", "org.mariadb.jdbc.Driver")
				.put("url", "jdbc:mysql://ensembldb.ensembl.org:3306")
				.put("user", "anonymous")
				.put("password", "")
				.put("min_pool_size", 1)
				.put("initial_pool_size", 1)
				.put("max_pool_size", 5)
				.put("max_idle_time", 60));
		query = pool.preparedQuery("SELECT * FROM acanthochromis_polyacanthus_core_100_1.analysis WHERE analysis_id = ?");
	}
	
	@RepeatedTest(1000)
	void executePreparedQuery(VertxTestContext testContext) throws Throwable {
		query.execute(Tuple.of(5)).onComplete(testContext.succeedingThenComplete());
		assertThat(testContext.awaitCompletion(2, SECONDS)).isTrue();
		if (testContext.failed())
			throw testContext.causeOfFailure();
	}
	
	@RepeatedTest(1000)
	void getConnectionAndExecutePreparedQuery(VertxTestContext testContext) throws Throwable {
		pool.getConnection()
				.onComplete(testContext.succeeding(connection -> connection.preparedQuery("SELECT * FROM acanthochromis_polyacanthus_core_100_1.analysis WHERE analysis_id = ?")
						.execute(Tuple.of(5))
						.onFailure(e -> {
							testContext.failNow(e);
							connection.close();
						})
						.onSuccess(rows -> {
							testContext.completeNow();
							connection.close();
						})));
		assertThat(testContext.awaitCompletion(2, SECONDS)).isTrue();
		if (testContext.failed())
			throw testContext.causeOfFailure();
	}
	
}
