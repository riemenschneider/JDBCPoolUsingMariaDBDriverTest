import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.jdbc.spi.impl.C3P0DataSourceProvider;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;

@ExtendWith(VertxExtension.class)
public class VertxJDBCClient {
	
	static JDBCClient client;
	
	@BeforeAll
	static void createClient(Vertx vertx) throws SQLException {
		client = JDBCClient.create(vertx, new C3P0DataSourceProvider().getDataSource(new JsonObject()
				.put("driver_class", "org.mariadb.jdbc.Driver")
				.put("url", "jdbc:mysql://ensembldb.ensembl.org:3306?autocommit=false")
				.put("user", "anonymous")
				.put("password", "")
				.put("min_pool_size", 1)
				.put("initial_pool_size", 1)
				.put("max_pool_size", 5)
				.put("max_idle_time", 60)));
	}
	
	@RepeatedTest(1000)
	void executePreparedQuery(VertxTestContext testContext) throws Throwable {
		client.getConnection(testContext.succeeding(connection -> {
			String sql = "SELECT * FROM acanthochromis_polyacanthus_core_100_1.analysis WHERE analysis_id = ?";
			JsonArray params = new JsonArray().add(5);
			connection.queryWithParams(sql, params, testContext.succeeding(result -> {
				if (result.getNumRows() == 1)
					testContext.completeNow();
				else
					testContext.failNow("Expected one result.");
				connection.close();
			}));
		}));
		assertThat(testContext.awaitCompletion(10, SECONDS)).isTrue();
		if (testContext.failed())
			throw testContext.causeOfFailure();
	}
	
}
