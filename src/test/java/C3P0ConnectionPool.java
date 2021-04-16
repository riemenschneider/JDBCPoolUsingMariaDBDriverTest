import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

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
			PreparedStatement preparedStatement = connection.prepareStatement("SELECT * FROM acanthochromis_polyacanthus_core_100_1.analysis WHERE analysis_id = ?")
		) {
			preparedStatement.setInt(1, 5);
			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
				for (int column = 1; column < resultSetMetaData.getColumnCount(); column++)
					assertNotNull(resultSetMetaData.getColumnLabel(column));
				assertTrue(resultSet.next());
				assertFalse(resultSet.next());
			}
		}
	}
	
}
