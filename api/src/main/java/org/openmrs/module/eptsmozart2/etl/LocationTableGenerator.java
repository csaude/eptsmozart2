package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.AppProperties;
import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 7/21/22.
 */
public class LocationTableGenerator implements Generator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "location.sql";
	
	private Integer toBeGenerated = 0;
	
	private Integer currentlyGenerated = 0;
	
	@Override
	public String getTable() {
		return "location";
	}
	
	@Override
	public Integer getCurrentlyGenerated() {
		return currentlyGenerated;
	}
	
	@Override
	public Integer getToBeGenerated() {
		return toBeGenerated;
	}
	
	@Override
	public Void call() throws Exception {
		long startTime = System.currentTimeMillis();
		try {
			createTable(Utils.readFileToString(CREATE_TABLE_FILE_NAME));
			locationEtl();
			return null;
		}
		finally {
			LOGGER.info("MozART II {} table generation duration: {} ms", getTable(), System.currentTimeMillis() - startTime);
		}
	}
	
	private void locationEtl() throws SQLException {
        String insertSql = new StringBuilder("INSERT INTO ").append(AppProperties.getInstance().getNewDatabaseName())
                .append(".location (location_id,location_uuid, name, province_name, province_district, source_database) ")
                .append("SELECT location_id,uuid,name,state_province,county_district, '")
                .append(AppProperties.getInstance().getDatabaseName()).append("' AS source_database FROM ")
                .append(AppProperties.getInstance().getDatabaseName()).append(".location WHERE !retired ORDER BY location_id").toString();

        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            int moreToGo = statement.executeUpdate(insertSql);

            String sismaHddIdUpdateSql = getUpdateCodeQuery("sisma_hdd_id", 1);
            int updatedRows = statement.executeUpdate(sismaHddIdUpdateSql);
            LOGGER.debug("{} location has their sisma hdd id updated", updatedRows);

            String datimIdUpdateSql = getUpdateCodeQuery("datim_id", 2);
            updatedRows = statement.executeUpdate(datimIdUpdateSql);
            LOGGER.debug("{} location has their datim id updated", updatedRows);

            String sismaIdUpdateSql = getUpdateCodeQuery("sisma_id", 4);
            updatedRows = statement.executeUpdate(sismaIdUpdateSql);
            LOGGER.debug("{} location has their datim id updated", updatedRows);

            toBeGenerated += moreToGo;
            currentlyGenerated += moreToGo;
        } catch (SQLException e) {
            LOGGER.error("An error has occured while inserting records to {} table, running SQL: {}", getTable(), insertSql, e);
            throw e;
        }

    }
	
	private String getUpdateCodeQuery(String codeColumn, int attributeTypeId) {
		return new StringBuilder("UPDATE ").append(AppProperties.getInstance().getNewDatabaseName()).append(".location l,")
		        .append(AppProperties.getInstance().getDatabaseName()).append(".location_attribute la ").append("SET l.")
		        .append(codeColumn)
		        .append(" = la.value_reference WHERE l.location_id=la.location_id AND la.attribute_type_id = ")
		        .append(attributeTypeId).append(" AND !la.voided").toString();
	}
}
