package org.openmrs.module.eptsmozart2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
	
	public static void insertEncounterObs(String databaseName) {
        try (Connection conn = ConnectionPool.getConnection()) {

            int maxObsId = getMaxObsId(conn, databaseName);
            int batch_size = getBatchSize(conn, databaseName);

            for (int start = 1; start <= maxObsId; start += batch_size) {
                int end = start + batch_size - 1;
                insertBatch(conn, start, end, databaseName);
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
	
	private static void insertBatch(Connection conn, int start, int end, String databaseName) throws SQLException {
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO encounter_obs (");
		sb.append("encounter_id, encounter_type, patient_id, location_id, form_id, encounter_datetime, ");
		sb.append("e_date_created, e_date_changed, encounter_uuid, ");
		sb.append("obs_id, concept_id, obs_datetime, obs_group_id, value_coded, value_drug, ");
		sb.append("value_datetime, value_numeric, value_text, comments, ");
		sb.append("o_date_created, obs_uuid) ");
		
		sb.append("SELECT ");
		sb.append("e.encounter_id, e.encounter_type, e.patient_id, e.location_id, e.form_id, e.encounter_datetime, ");
		sb.append("e.date_created AS e_date_created, e.date_changed AS e_date_changed, e.uuid AS encounter_uuid, ");
		sb.append("o.obs_id, o.concept_id, o.obs_datetime, o.obs_group_id, o.value_coded, o.value_drug, ");
		sb.append("o.value_datetime, o.value_numeric, o.value_text, o.comments, ");
		sb.append("o.date_created AS o_date_created, o.uuid AS obs_uuid ");
		sb.append("FROM encounter e ");
		sb.append("JOIN obs o ON e.encounter_id = o.encounter_id ");
		sb.append("WHERE e.voided = 0 AND o.voided = 0 ");
		sb.append("AND (o.obs_datetime NOT LIKE '%00-00-00 00:00:00%' OR e.encounter_datetime NOT LIKE '%00-00-00 00:00:00%') ");
		sb.append("AND o.obs_id BETWEEN ? AND ? ;");
		
		try (PreparedStatement stmt = conn.prepareStatement(sb.toString())) {
			 stmt.execute("USE " + databaseName); 
			 stmt.execute("SET sql_log_bin = 0;");
			 stmt.setInt(1, start); stmt.setInt(2, end);
			 stmt.executeUpdate();
			 stmt.execute("SET sql_log_bin = 1;");
		}
	}
}
