import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class C3P0ConnectionPool {
	
	static ComboPooledDataSource datasource;
	
	@BeforeAll
	static void createPool() throws Exception {
		datasource =  new ComboPooledDataSource();
		datasource.setDriverClass("org.mariadb.jdbc.Driver");
		datasource.setJdbcUrl("jdbc:mysql://ensembldb.ensembl.org:3306");
		datasource.setUser("anonymous");
		datasource.setPassword("");
		datasource.setMinPoolSize(1);
		datasource.setInitialPoolSize(1);
		datasource.setMaxPoolSize(5);
		datasource.setMaxIdleTime(60);
	}
	
	@RepeatedTest(1000)
	void executeQuery() throws Exception {
		try (
			Connection connection = datasource.getConnection();
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM acanthochromis_polyacanthus_core_100_1.analysis WHERE analysis_id = ?", Statement.RETURN_GENERATED_KEYS)
		) {
			statement.setInt(1, 5);
			try (ResultSet result = statement.executeQuery()) {
				try (ResultSet keys = statement.getGeneratedKeys()) {
					ResultSetMetaData metadata = keys.getMetaData();
					int cols = metadata.getColumnCount();
					for (int i = 1; i <= cols; i++)
						metadata.getColumnLabel(i);
				}
			}
		}
	}
	
}
