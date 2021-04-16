import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;

import com.mchange.v2.c3p0.ComboPooledDataSource;

public class C3P0ConnectionPool {
	
	static ComboPooledDataSource datasource;
	
	@BeforeAll
	static void createPool() throws PropertyVetoException {
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
	void executeQuery() throws SQLException {
		try (
			Connection connection = datasource.getConnection();
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM acanthochromis_polyacanthus_core_100_1.analysis WHERE analysis_id = ?", Statement.RETURN_GENERATED_KEYS)
		) {
			statement.setInt(1, 5);
			try (ResultSet result = statement.executeQuery()) {
				consume(result);
				consume(statement.getGeneratedKeys());
			}
		}
	}
	
	static void consume(ResultSet result) throws SQLException {
		ResultSetMetaData metadata = result.getMetaData();
		int columnCount = metadata.getColumnCount();
		for (int index = 1; index < columnCount; index++)
			assertNotNull(metadata.getColumnLabel(index));
		while (result.next())
			for (int index = 1; index < columnCount; index++)
				result.getObject(index);
	}
	
}
