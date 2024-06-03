package org.openmrs.module.eptsmozart2.etl;

import org.openmrs.module.eptsmozart2.ConnectionPool;
import org.openmrs.module.eptsmozart2.Mozart2Properties;
import org.openmrs.module.eptsmozart2.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.openmrs.module.eptsmozart2.Utils.inClause;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 6/29/22.
 */
public class CounselingTableGenerator extends AbstractNonScrollableResultSetGenerator {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CounselingTableGenerator.class);
	
	private static final String CREATE_TABLE_FILE_NAME = "counseling.sql";
	
	public static final Integer[] COUNSELING_CONCEPT_IDS = new Integer[] { 6340, 1048, 23716, 23887, 6223, 165463, 6315,
	        6306, 23775, 6193, 6317, 6318, 6319, 6320, 5271, 6321, 6322, 23771 };
	
	public static final Integer ENCOUNTER_TYPE_ID = 35;
	
	protected final int ENCOUNTER_UUID_POS = 1;
	
	protected final int ENCOUNTER_DATE_POS = 2;
	
	protected final int DIAGNOSIS_REVEAL_POS = 3;
	
	protected final int HIV_DISCLOSURE_POS = 4;
	
	protected final int ADH_PLAN_POS = 5;
	
	protected final int SEC_EFFECTS_POS = 6;
	
	protected final int ADH_ART_POS = 7;
	
	protected final int ADH_PERCENT_POS = 8;
	
	protected final int CONSULT_REASON_POS = 9;
	
	protected final int ACCEPT_CONTACT_POS = 10;
	
	protected final int ACCEPT_DATE_POS = 11;
	
	protected final int PSY_REFUSAL_POS = 12;
	
	protected final int PSY_SICK_POS = 13;
	
	protected final int PSY_NOTBELIEVE_POS = 14;
	
	protected final int PSY_LOTOFPILLS_POS = 15;
	
	protected final int PSY_FEELBETTER_POS = 16;
	
	protected final int PSY_LACKFOOD_POS = 17;
	
	protected final int PSY_LACKSUPPORT_POS = 18;
	
	protected final int PSY_DEPRESSION_POS = 19;
	
	protected final int PSY_NOTREVEAL_POS = 20;
	
	protected final int PSY_TOXICITY_POS = 21;
	
	protected final int PSY_LOSTPILLS_POS = 22;
	
	protected final int PSY_STIGMA_POS = 23;
	
	protected final int PSY_TRANSPORT_POS = 24;
	
	protected final int PSY_GBV_POS = 25;
	
	protected final int PSY_CULTURAL_POS = 26;
	
	protected final int PSY_DRUGUSE_POS = 27;
	
	protected final int PP1_POS = 28;
	
	protected final int PP2_POS = 29;
	
	protected final int PP3_POS = 30;
	
	protected final int PP4_POS = 31;
	
	protected final int PP5_POS = 32;
	
	protected final int PP6_POS = 33;
	
	protected final int PP7_POS = 34;
	
	protected final int KEYPOP_LUBS_POS = 35;
	
	protected final int ENC_TYPE_POS = 36;
	
	protected final int ENC_CREATED_DATE_POS = 37;
	
	protected final int ENC_CHANGE_DATE_POS = 38;
	
	protected final int FORM_ID_POS = 39;
	
	protected final int PATIENT_UUID_POS = 40;
	
	protected final int LOC_UUID_POS = 41;
	
	protected final int SRC_DB_POS = 42;
	
	@Override
    protected PreparedStatement prepareInsertStatement(ResultSet results, Integer batchSize) throws SQLException {
        if (batchSize == null)
            batchSize = Integer.MAX_VALUE;
        String insertSql = new StringBuilder("INSERT INTO ")
                .append(Mozart2Properties.getInstance().getNewDatabaseName())
                .append(".counseling (encounter_uuid, encounter_date, diagnosis_reveal, hiv_disclosure, adherence_plan, ")
                .append("secondary_effects, adherence_art, adherence_percent, consultation_reason, accept_contact, ")
                .append("accept_date, psychosocial_refusal, psychosocial_sick, psychosocial_notbelieve, ")
                .append("psychosocial_lotofpills, psychosocial_feelbetter, psychosocial_lackfood, psychosocial_lacksupport, ")
                .append("psychosocial_depression, psychosocial_notreveal, psychosocial_toxicity, psychosocial_lostpills, ")
                .append("psychosocial_stigma, psychosocial_transport, psychosocial_gbv, psychosocial_cultural, ")
                .append("psychosocial_druguse, pp1, pp2, pp3, pp4, pp5, pp6, pp7, keypop_lubricants, ")
                .append("encounter_type, encounter_created_date, encounter_change_date, form_id, patient_uuid, location_uuid, source_database) ")
                .append("VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ")
                .append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)").toString();

        PreparedStatement counselingObsStatement = null;
        ResultSet counselingObsResults = null;
        try {
            if (insertStatement == null) {
                insertStatement = ConnectionPool.getConnection().prepareStatement(insertSql);
            } else {
                insertStatement.clearParameters();
            }
            int count = 0;
            String tbDataObsQuery = new StringBuilder("SELECT * FROM ")
                    .append(Mozart2Properties.getInstance().getDatabaseName())
                    .append(".obs WHERE !voided and concept_id IN ").append(inClause(COUNSELING_CONCEPT_IDS))
                    .append(" AND encounter_id = ?")
                    .toString();

            counselingObsStatement = ConnectionPool.getConnection().prepareStatement(tbDataObsQuery);
            Set<Integer> positionsNotSet = new HashSet<>();
            while (results.next() && count < batchSize) {
                Integer encounterId = results.getInt("encounter_id");

                positionsNotSet.addAll(Arrays.asList(DIAGNOSIS_REVEAL_POS, HIV_DISCLOSURE_POS, ADH_PLAN_POS, SEC_EFFECTS_POS,
                                                     ADH_ART_POS, ADH_PERCENT_POS, CONSULT_REASON_POS, ACCEPT_CONTACT_POS,
                                                     ACCEPT_DATE_POS, PSY_REFUSAL_POS, PSY_SICK_POS, PSY_NOTBELIEVE_POS,
                                                     PSY_LOTOFPILLS_POS, PSY_FEELBETTER_POS, PSY_LACKFOOD_POS, PSY_LACKSUPPORT_POS,
                                                     PSY_DEPRESSION_POS, PSY_NOTREVEAL_POS, PSY_TOXICITY_POS, PSY_LOSTPILLS_POS,
                                                     PSY_STIGMA_POS, PSY_TRANSPORT_POS, PSY_GBV_POS, PSY_CULTURAL_POS, PSY_DRUGUSE_POS,
                                                     PP1_POS, PP2_POS, PP3_POS, PP4_POS, PP5_POS, PP6_POS, PP7_POS, KEYPOP_LUBS_POS
                ));

                insertStatement.setString(ENCOUNTER_UUID_POS, results.getString("encounter_uuid"));
                insertStatement.setTimestamp(ENCOUNTER_DATE_POS, results.getTimestamp("encounter_datetime"));
                insertStatement.setInt(ENC_TYPE_POS, results.getInt("encounter_type"));
                insertStatement.setTimestamp(ENC_CREATED_DATE_POS, results.getTimestamp("e_date_created"));
                insertStatement.setTimestamp(ENC_CHANGE_DATE_POS, results.getTimestamp("e_date_changed"));
                insertStatement.setInt(FORM_ID_POS, results.getInt("form_id"));
                insertStatement.setString(PATIENT_UUID_POS, results.getString("patient_uuid"));
                insertStatement.setString(LOC_UUID_POS, results.getString("loc_uuid"));
                insertStatement.setString(SRC_DB_POS, Mozart2Properties.getInstance().getSourceOpenmrsInstance());

                counselingObsStatement.setInt(1, encounterId);
                counselingObsResults = counselingObsStatement.executeQuery();
                while(counselingObsResults.next()) {
                    int resultConceptId = counselingObsResults.getInt("concept_id");
                    int valueCoded = counselingObsResults.getInt("value_coded");
                    if(resultConceptId == 6340) {
                        insertStatement.setInt(DIAGNOSIS_REVEAL_POS, valueCoded);
                        positionsNotSet.remove(DIAGNOSIS_REVEAL_POS);
                    } else if(resultConceptId == 1048) {
                        insertStatement.setInt(HIV_DISCLOSURE_POS, valueCoded);
                        positionsNotSet.remove(HIV_DISCLOSURE_POS);
                    } else if(resultConceptId == 23716) {
                        insertStatement.setInt(ADH_PLAN_POS, valueCoded);
                        positionsNotSet.remove(ADH_PLAN_POS);
                    } else if(resultConceptId == 23887) {
                        insertStatement.setInt(SEC_EFFECTS_POS, valueCoded);
                        positionsNotSet.remove(SEC_EFFECTS_POS);
                    } else if(resultConceptId == 6223) {
                        insertStatement.setInt(ADH_ART_POS, valueCoded);
                        positionsNotSet.remove(ADH_ART_POS);
                    } else if(resultConceptId == 165463) {
                        insertStatement.setDouble(ADH_PERCENT_POS, counselingObsResults.getDouble("value_numeric"));
                        positionsNotSet.remove(ADH_PERCENT_POS);
                    } else if(resultConceptId == 6315) {
                        insertStatement.setInt(CONSULT_REASON_POS, valueCoded);
                        positionsNotSet.remove(CONSULT_REASON_POS);
                    } else if(resultConceptId == 6306) {
                        insertStatement.setInt(ACCEPT_CONTACT_POS, valueCoded);
                        positionsNotSet.remove(ACCEPT_CONTACT_POS);
                    } else if(resultConceptId == 23775) {
                        insertStatement.setTimestamp(ACCEPT_DATE_POS, counselingObsResults.getTimestamp("value_datetime"));
                        positionsNotSet.remove(ACCEPT_DATE_POS);
                    } else if(resultConceptId == 6193) {
                        switch (valueCoded) {
                            case 1956:
                                insertStatement.setInt(PSY_REFUSAL_POS, valueCoded);
                                positionsNotSet.remove(PSY_REFUSAL_POS);
                                break;
                            case 1936:
                                insertStatement.setInt(PSY_SICK_POS, valueCoded);
                                positionsNotSet.remove(PSY_SICK_POS);
                                break;
                            case 6186:
                                insertStatement.setInt(PSY_NOTBELIEVE_POS, valueCoded);
                                positionsNotSet.remove(PSY_NOTBELIEVE_POS);
                                break;
                            case 23766:
                                insertStatement.setInt(PSY_LOTOFPILLS_POS, valueCoded);
                                positionsNotSet.remove(PSY_LOTOFPILLS_POS);
                                break;
                            case 23767:
                                insertStatement.setInt(PSY_FEELBETTER_POS, valueCoded);
                                positionsNotSet.remove(PSY_FEELBETTER_POS);
                                break;
                            case 18698:
                                insertStatement.setInt(PSY_LACKFOOD_POS, valueCoded);
                                positionsNotSet.remove(PSY_LACKFOOD_POS);
                                break;
                            case 2153:
                                insertStatement.setInt(PSY_LACKSUPPORT_POS, valueCoded);
                                positionsNotSet.remove(PSY_LACKSUPPORT_POS);
                                break;
                            case 207:
                                insertStatement.setInt(PSY_DEPRESSION_POS, valueCoded);
                                positionsNotSet.remove(PSY_DEPRESSION_POS);
                                break;
                            case 2155:
                                insertStatement.setInt(PSY_NOTREVEAL_POS, valueCoded);
                                positionsNotSet.remove(PSY_NOTREVEAL_POS);
                                break;
                            case 2015:
                                insertStatement.setInt(PSY_TOXICITY_POS, valueCoded);
                                positionsNotSet.remove(PSY_TOXICITY_POS);
                                break;
                            case 23768:
                                insertStatement.setInt(PSY_LOSTPILLS_POS, valueCoded);
                                positionsNotSet.remove(PSY_LOSTPILLS_POS);
                                break;
                            case 6436:
                                insertStatement.setInt(PSY_STIGMA_POS, valueCoded);
                                positionsNotSet.remove(PSY_STIGMA_POS);
                                break;
                            case 820:
                                insertStatement.setInt(PSY_TRANSPORT_POS, valueCoded);
                                positionsNotSet.remove(PSY_TRANSPORT_POS);
                                break;
                            case 6303:
                                insertStatement.setInt(PSY_GBV_POS, valueCoded);
                                positionsNotSet.remove(PSY_GBV_POS);
                                break;
                            case 23769:
                                insertStatement.setInt(PSY_CULTURAL_POS, valueCoded);
                                positionsNotSet.remove(PSY_CULTURAL_POS);
                                break;
                            case 1603:
                                insertStatement.setInt(PSY_DRUGUSE_POS, valueCoded);
                                positionsNotSet.remove(PSY_DRUGUSE_POS);
                                break;
                        }
                    } else if(resultConceptId == 6317) {
                        insertStatement.setInt(PP1_POS, valueCoded);
                        positionsNotSet.remove(PP1_POS);
                    } else if(resultConceptId == 6318) {
                        insertStatement.setInt(PP2_POS, valueCoded);
                        positionsNotSet.remove(PP2_POS);
                    } else if(resultConceptId == 6319) {
                        insertStatement.setInt(PP3_POS, valueCoded);
                        positionsNotSet.remove(PP3_POS);
                    } else if(resultConceptId == 6320) {
                        insertStatement.setInt(PP4_POS, valueCoded);
                        positionsNotSet.remove(PP4_POS);
                    } else if(resultConceptId == 5271) {
                        insertStatement.setInt(PP5_POS, valueCoded);
                        positionsNotSet.remove(PP5_POS);
                    } else if(resultConceptId == 6321) {
                        insertStatement.setInt(PP6_POS, valueCoded);
                        positionsNotSet.remove(PP6_POS);
                    } else if(resultConceptId == 6322) {
                        insertStatement.setInt(PP7_POS, valueCoded);
                        positionsNotSet.remove(PP7_POS);
                    } else if(resultConceptId == 23771) {
                        insertStatement.setInt(KEYPOP_LUBS_POS, valueCoded);
                        positionsNotSet.remove(KEYPOP_LUBS_POS);
                    }
                }

                setEmptyPositions(positionsNotSet);
                insertStatement.addBatch();
                ++count;
            }
            return insertStatement;
        }
        catch (SQLException e) {
            LOGGER.error("Error preparing insert statement for table {}", getTable());
            this.setChanged();
            Utils.notifyObserversAboutException(this, e);
            throw e;
        } finally {
            if(counselingObsResults != null) {
                counselingObsResults.close();
            }
            if(counselingObsStatement != null) {
                counselingObsStatement.close();
            }
        }
    }
	
	@Override
	public String getTable() {
		return "counseling";
	}
	
	@Override
	protected String getCreateTableSql() throws IOException {
		return Utils.readFileToString(CREATE_TABLE_FILE_NAME);
	}
	
	@Override
	protected String countQuery() {
		return new StringBuilder("SELECT COUNT(DISTINCT e.encounter_id) FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND !e.voided AND e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type = ")
		        .append(ENCOUNTER_TYPE_ID).append(" WHERE !o.voided AND o.concept_id IN ")
		        .append(inClause(COUNSELING_CONCEPT_IDS)).toString();
	}
	
	@Override
	protected String fetchQuery(Integer start, Integer batchSize) {
		StringBuilder sb = new StringBuilder("SELECT e.encounter_id, e.encounter_datetime, e.uuid as encounter_uuid, ")
		        .append("e.encounter_type, e.date_created as e_date_created, e.date_changed as e_date_changed, ")
		        .append("e.form_id, p.patient_uuid, l.uuid as loc_uuid FROM ")
		        .append(Mozart2Properties.getInstance().getDatabaseName()).append(".obs o JOIN ")
		        .append(Mozart2Properties.getInstance().getNewDatabaseName())
		        .append(".patient p ON o.person_id = p.patient_id JOIN ")
		        .append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".encounter e on o.encounter_id = e.encounter_id AND !e.voided AND e.encounter_datetime <= '")
		        .append(Date.valueOf(Mozart2Properties.getInstance().getEndDate())).append("' AND e.encounter_type = ")
		        .append(ENCOUNTER_TYPE_ID).append(" JOIN ").append(Mozart2Properties.getInstance().getDatabaseName())
		        .append(".location l on l.location_id = e.location_id WHERE !o.voided AND o.concept_id IN ")
		        .append(inClause(COUNSELING_CONCEPT_IDS))
		        .append(" GROUP BY e.encounter_id, e.encounter_datetime, e.uuid ORDER BY e.encounter_id");
		
		if (start != null) {
			sb.append(" limit ?");
		}
		
		if (batchSize != null) {
			sb.append(", ?");
		}
		
		return sb.toString();
	}
	
	private void setEmptyPositions(Set<Integer> positionsNotSet) throws SQLException {
		if (!positionsNotSet.isEmpty()) {
			Iterator<Integer> iter = positionsNotSet.iterator();
			while (iter.hasNext()) {
				int pos = iter.next();
				switch (pos) {
					case ADH_PERCENT_POS:
						insertStatement.setNull(pos, Types.DOUBLE);
						break;
					case ACCEPT_DATE_POS:
						insertStatement.setNull(pos, Types.DATE);
						break;
					default:
						insertStatement.setNull(pos, Types.INTEGER);
				}
			}
		}
	}
}
