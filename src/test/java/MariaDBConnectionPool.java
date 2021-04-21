import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;

public class MariaDBConnectionPool {
	
	@BeforeAll
	static void registerDriver() throws Exception {
		Class.forName("org.mariadb.jdbc.Driver");
	}
	
	@RepeatedTest(1000)
	void executeQuery() throws Exception {
		try (
			Connection connection = DriverManager.getConnection("jdbc:mysql://ensembldb.ensembl.org:3306?pool=true&minPoolSize=1&maxPoolSize=5&maxIdleTime=60", "anonymous", "");
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
