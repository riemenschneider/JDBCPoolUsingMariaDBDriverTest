import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;

public class MariaDBConnectionPool {
	
	@BeforeAll
	static void registerDriver() throws ClassNotFoundException {
		Class.forName("org.mariadb.jdbc.Driver");
	}
	
	@RepeatedTest(1000)
	void executeQuery() throws SQLException {
		try (
			Connection connection = DriverManager.getConnection("jdbc:mysql://ensembldb.ensembl.org:3306?pool=true&minPoolSize=1&maxPoolSize=5&maxIdleTime=60", "anonymous", "");
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM acanthochromis_polyacanthus_core_100_1.analysis WHERE analysis_id = ?")
		) {
			statement.setInt(1, 5);
			try (ResultSet result = statement.executeQuery()) {
				ResultSetMetaData metadata = result.getMetaData();
				for (int index = 1; index < metadata.getColumnCount(); index++)
					assertNotNull(metadata.getColumnLabel(index));
				assertTrue(result.next());
				assertFalse(result.next());
			}
		}
	}
	
}
