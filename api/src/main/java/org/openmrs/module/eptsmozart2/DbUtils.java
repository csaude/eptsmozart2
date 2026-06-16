package org.openmrs.module.eptsmozart2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/10/22.
 */
public class DbUtils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DbUtils.class);
	
	private static final int BATCH_SIZE = 20000;
	
	public static void createNewDatabase() throws SQLException {
        String drop = "DROP DATABASE IF EXISTS " + Mozart2Properties.getInstance().getNewDatabaseName();
        String sql = "CREATE DATABASE IF NOT EXISTS " + Mozart2Properties.getInstance().getNewDatabaseName();
        try (Connection connection = ConnectionPool.getConnection();
             Statement s = connection.createStatement()) {
            s.execute(drop);
            s.execute(sql);
        } catch (SQLException sqle) {
            LOGGER.error("An error occured while running sql: {}", sql, sqle);
            throw sqle;
        }
    }
	
	public  static void runSqlStatements(String[] sqls, String databaseName) throws SQLException {
        try (Connection connection = ConnectionPool.getConnection();
             Statement s = connection.createStatement()) {
            s.addBatch("USE ".concat(databaseName));
            for(String sql:sqls) {
                s.addBatch(sql.trim());
            }
            s.executeBatch();
        } catch (SQLException sqle) {
            LOGGER.error("An error occured while running sqls: {}", sqls, sqle);
            throw sqle;
        }
    }
	
	public  static void runSqlStatement(String sql, String databaseName) throws SQLException {
        try (Connection connection = ConnectionPool.getConnection();
             Statement s = connection.createStatement()) {
            s.addBatch("USE ".concat(databaseName));
            s.execute(sql);
        } catch (SQLException sqle) {
            LOGGER.error("An error occured while running sqls: {}", sql, sqle);
            throw sqle;
        }
    }
	
	public static boolean checkIfEncounterObsExist(String databaseName) {
        try (Connection conn = ConnectionPool.getConnection()) {
            String sqlExistEncounterObs = "SELECT COUNT(*) as matched FROM information_schema.tables WHERE table_schema = '" + databaseName +"' AND table_name = 'encounter_obs';";
            int existEncounterObs = countData(conn, databaseName, sqlExistEncounterObs);
            if(existEncounterObs == 1) {
                String sqlCountEncounterObs = "SELECT COUNT(*) FROM encounter_obs;";
                String sqlCountEncounterJoinObs = "SELECT COUNT(*) FROM encounter e JOIN obs o on e.encounter_id = o.encounter_id AND !e.voided AND !o.voided \n"
                        + " AND (o.obs_datetime NOT LIKE '%00-00-00 00:00:00%' OR e.encounter_datetime NOT LIKE '%00-00-00 00:00:00%')";
                int countEnconterObs = countData(conn, databaseName, sqlCountEncounterObs);
                int countEnconterJoinObs = countData(conn, databaseName, sqlCountEncounterJoinObs);
                if(countEnconterObs == countEnconterJoinObs) {
                    return true;
                }else {
                    return false;}
            } else {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
	
	private static int countData(Connection conn, String databaseName, String sql) throws SQLException {

        try (Statement stmt = conn.createStatement();) {
            stmt.execute("USE " + databaseName);
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return 0;
    }
	
	public static void insertEncounterObs(String databaseName, String[] sql) {
        try (Connection conn = ConnectionPool.getConnection()) {

            int maxObsId = getMaxObsId(conn, databaseName);
            int batch_size = getBatchSize(conn, databaseName);

            for (int start = 1; start <= maxObsId; start += batch_size) {
                int end = start + batch_size - 1;
                insertBatchLoop(conn, start, end, databaseName, sql);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	private static int getMaxObsId(Connection conn, String databaseName) throws SQLException {
        String sql = "SELECT MAX(obs_id) FROM obs";
        try (Statement stmt = conn.createStatement();) {
            stmt.execute("USE " + databaseName);
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return 0;
    }
	
	private static int getBatchSize(Connection conn, String databaseName) throws SQLException {
        String sql = "SELECT property_value FROM global_property WHERE property = 'eptsmozart2.batch.size'";
        try (Statement stmt = conn.createStatement();) {
            stmt.execute("USE " + databaseName);
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                return rs.getInt(1);
            }
        }
        return BATCH_SIZE;
    }
	
	private static void insertBatchLoop(Connection conn, int start, int end, String databaseName, String[] sql) throws SQLException {
        try (
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT location_id FROM " + databaseName + ".location");
                PreparedStatement ps = conn.prepareStatement(sql[1]) // INSERT statement with 3 parameters
        ) {
            // Disable binlog
            try (Statement st = conn.createStatement()) {
                st.execute(sql[0]); // SET sql_log_bin = 0;
            }
            ps.setInt(1, start);
            ps.setInt(2, end);

            ps.executeUpdate();

            // Re-enable binlog
            try (Statement st = conn.createStatement()) {
                st.execute(sql[2]); // SET sql_log_bin = 1;
            }

        } catch (SQLException e) {
            LOGGER.error("Error running encounter_obs insert", e);
            throw e;
        }
    }
}
