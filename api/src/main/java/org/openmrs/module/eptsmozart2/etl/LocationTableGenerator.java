package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 7/21/22.
 */
public class LocationTableGenerator extends InsertFromSelectGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractNonScrollableResultSetGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "location.sql";
	
	@Override
	public String getTable() {
		return "location";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected void etl() throws SQLException {
        String insertSql = new StringBuilder("INSERT INTO ").append(Mozart2Properties.getInstance().getNewDatabaseName())
                .append(".location (location_id,location_uuid, name, province_name, province_district) ")
                .append("SELECT location_id,uuid,name,state_province,county_district FROM ")
                .append(Mozart2Properties.getInstance().getDatabaseName())
				.append(".location WHERE location_id IN ")
				.append(inClause(Mozart2Properties.getInstance().getLocationIdsSet().toArray(new Integer[0])))
				.append(" ORDER BY location_id").toString();

        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            int moreToGo = statement.executeUpdate(insertSql);

            String sismaHddIdUpdateSql = getUpdateCodeQuery("sisma_hdd_id", 1);
            int updatedRows = statement.executeUpdate(sismaHddIdUpdateSql);
            LOGGER.debug("{} locations has their sisma hdd id updated", updatedRows);

            String datimIdUpdateSql = getUpdateCodeQuery("datim_id", 2);
            updatedRows = statement.executeUpdate(datimIdUpdateSql);
            LOGGER.debug("{} locations has their datim id updated", updatedRows);

            String sismaIdUpdateSql = getUpdateCodeQuery("sisma_id", 4);
            updatedRows = statement.executeUpdate(sismaIdUpdateSql);
            LOGGER.debug("{} locations has their datim id updated", updatedRows);

            updatedRows = statement.executeUpdate(getMarkSelectedLocationsuUpdateQuery());
			LOGGER.debug("{} locations have been selected for this mozart2 operation", updatedRows);
			
            toBeGenerated += moreToGo;
            currentlyGenerated += moreToGo;
        } catch (SQLException e) {
            LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), insertSql, e);
			this.setChanged();
			Utils.notifyObserversAboutException(this, e);
            throw e;
        }

    }
	
	private String getUpdateCodeQuery(String codeColumn, int attributeTypeId) {
		return new StringBuilder("UPDATE ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".location l,").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location_attribute la ").append("SET l.").append(codeColumn)
		        .append(" = la.value_reference WHERE l.location_id=la.location_id AND la.attribute_type_id = ")
		        .append(attributeTypeId).append(" AND !la.voided").toString();
	}
	
	private String getMarkSelectedLocationsuUpdateQuery() {
		return new StringBuilder("UPDATE ").append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".location SET selected = TRUE WHERE location_id IN (")
		        .append(Mozart2Properties.getInstance().getLocationsIdsString()).append(")").toString();
	}
}
