DROP TABLE IF EXISTS `type_id_lookup`;

CREATE TABLE `type_id_lookup`(
  `id` int(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `table_name` varchar(255) DEFAULT NULL,
  `column_name` varchar(255) DEFAULT NULL,
  `id_type_lookup` varchar(11) DEFAULT NULL,
  `id_type_desc` varchar(255) DEFAULT NULL,
  `notes` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

INSERT INTO `type_id_lookup`(`table_name`, `column_name`, `id_type_lookup`, `id_type_desc`, `notes`)
VALUES('FORM', 'FORM_ID', '60', 'LABORATORIO GERAL-V2008', 'RETIRED'),
('FORM', 'FORM_ID', '62', 'TARV: RASTREIO DE TUBERCULOSE', 'RETIRED'),
('FORM', 'FORM_ID', '99', 'ADULTO: PROCESSO PARTE A - ANAMNESE -', 'RETIRED'),
('FORM', 'FORM_ID', '100', 'ADULTO: PROCESSO PARTE B - EXAME CLINICO', 'RETIRED'),
('FORM', 'FORM_ID', '101', 'ADULTO: SEGUIMENTO - v2008', 'RETIRED'),
('FORM', 'FORM_ID', '103', 'BUSCA ACTIVA', 'RETIRED'),
('FORM', 'FORM_ID', '105', 'CCR: SEGUIMENTO', ''),
('FORM', 'FORM_ID', '106', 'LABORATORIO GERAL', ''),
('FORM', 'FORM_ID', '108', 'PEDIATRIA: PROCESSO PARTE A- ANAMNESE', 'RETIRED'),
('FORM', 'FORM_ID', '109', 'PEDIATRIA: PROCESSO PARTE B - EXAME CLINICO', 'RETIRED'),
('FORM', 'FORM_ID', '110', 'PEDIATRIA: SEGUIMENTO - v2008', 'RETIRED'),
('FORM', 'FORM_ID', '115', 'TARV: ACONSELHAMENTO', 'RETIRED'),
('FORM', 'FORM_ID', '116', 'TARV: ACONSELHAMENTO SEGUIMENTO', 'RETIRED'),
('FORM', 'FORM_ID', '117', 'TARV: FRIDA - v2008', 'RETIRED'),
('FORM', 'FORM_ID', '118', 'TARV: RASTREIO DE TUBERCULOSE', 'RETIRED'),
('FORM', 'FORM_ID', '119', 'TARV: SOLICITACAO', 'RETIRED'),
('FORM', 'FORM_ID', '120', 'TUBERCULOSE: LIVRO', 'RETIRED'),
('FORM', 'FORM_ID', '121', 'TARV: VISITA DOMICILIARIA', ''),
('FORM', 'FORM_ID', '122', 'CCU: RASTREIO', ''),
('FORM', 'FORM_ID', '123', 'TARV: AVALICAO E PREPARACAO DO CANDIDATO AO TARV', 'RETIRED'),
('FORM', 'FORM_ID', '124', 'TARV: AVALIACAO DE ADESAO', 'RETIRED'),
('FORM', 'FORM_ID', '125', 'TARV: TERMO DE CONSENTIMENTO', 'RETIRED'),
('FORM', 'FORM_ID', '126', 'ADULTO: SEGUIMENTO - v2012', 'RETIRED'),
('FORM', 'FORM_ID', '127', 'PEDIATRIA: SEGUIMENTO - v2012', 'RETIRED'),
('FORM', 'FORM_ID', '128', 'LIVRO DE REGISTO PRE-TARV', 'RETIRED'),
('FORM', 'FORM_ID', '129', 'LIVRO DE REGISTO TARV', 'RETIRED'),
('FORM', 'FORM_ID', '130', 'FILA', ''),
('FORM', 'FORM_ID', '131', 'TARV: AVALIACAO PSICOLOGICA E PP - INICIAL', 'RETIRED'),
('FORM', 'FORM_ID', '132', 'TARV: AVALIACAO PSICOLOGICA E PP - SEGUIMENTO', 'RETIRED'),
('FORM', 'FORM_ID', '146', 'CONSENTIMENTO DE TESTE DE CASO INDICE', ''),
('FORM', 'FORM_ID', '147', 'FSR', ''),
('FORM', 'FORM_ID', '163', 'FICHE MESTRA - FICHE CLINICA', ''),
('FORM', 'FORM_ID', '164', 'FICHE MESTRA - FICHE APSS E PP', ''),
('FORM', 'FORM_ID', '165', 'FICHE MESTRA- FICHE RESUMO', ''),
('FORM', 'FORM_ID', '166', 'FICHE MESTRA - RECEPCAO - LEVANTOU TARV', ''),
('FORM', 'FORM_ID', '171', 'FILT', ''),
('FORM', 'FORM_ID', '172', 'LIVRO DE REGISTO DIARIO DE CHAMADAS E VISITAS DOMICILIARES', ''),
('FORM', 'ENCOUNTER_TYPE', '1', 'S.TARV: ADULTO INICIAL B', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '3', 'S.TARV: PEDIATRIA INICIAL B', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '5', 'S.TARV: ADULTO INICIAL A', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '6', 'S.TARV: ADULTO SEGUIMENTO', ''),
('FORM', 'ENCOUNTER_TYPE', '7', 'S.TARV: PEDIATRIA INICIAL A', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '9', 'S.TARV: PEDIATRIA SEGUIMENTO', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '10', 'CCR: SEGUIMENTO', ''),
('FORM', 'ENCOUNTER_TYPE', '11', 'PTV: PRE-NATAL INICIAL', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '13', 'MISAU: LABORATORIO', ''),
('FORM', 'ENCOUNTER_TYPE', '17', 'S.TARV: SOLICITAÇÃO ARV', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '18', 'S.TARV: FARMACIA', ''),
('FORM', 'ENCOUNTER_TYPE', '19', 'S.TARV: ACONSELHAMENTO', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '20', 'TUBERCULOSE: RASTREIO' ,'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '21', 'S.TARV: BUSCA ACTIVA', ''),
('FORM', 'ENCOUNTER_TYPE', '22', 'CCR: PCR', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '24', 'S.TARV: ACONSELHAMENTO SEGUIMENTO', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '28', 'CCU: RASTREIO', ''),
('FORM', 'ENCOUNTER_TYPE', '29', 'S.TARV: AVALIACAO E PREPARACAO DO CANDIDATO TARV', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '30', 'S.TARV: TERMO DE CONSENTIMENTO DE VISITA', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '31', 'S.TARV: AVALIACAO DE ADESAO', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '32', 'S.TARV: LIVRO PRE-TARV', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '33', 'S.TARV: LIVRO TARV', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '34', 'APSS: PREVENCAO POSITIVA - INICIAL', 'RETIRED'),
('FORM', 'ENCOUNTER_TYPE', '35', 'APSS: PREVENCAO POSITIVA - SEGUIMENTO', ''),
('FORM', 'ENCOUNTER_TYPE', '47', 'RASTREIO FAMILIAR', ''),
('FORM', 'ENCOUNTER_TYPE', '51', 'FSR - Carga Viral', ''),
('FORM', 'ENCOUNTER_TYPE', '52', 'FICHE MESTRA- LEVANTAMENTO DE ARV', ''),
('FORM', 'ENCOUNTER_TYPE', '53', 'FICHE MESTRA - FICHE RESUMO', ''),
('FORM', 'ENCOUNTER_TYPE', '60', 'TRATAMENTO PROFILATICO DA TUBERCULOSE (TPT)', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '1', 'OPEN MRS ID', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '2', 'NID - SERVICO TARV', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '5', 'CODIGO ATS/UATS', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '6', 'CODIGO PTV (PRE-NATAL)', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '7', 'CODIGO ITS', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '8', 'CODIGO PTV (MATERNIDADE)', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '9', 'NID (CCR)', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '10', 'PCR (NUMERO DE REGISTO)', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '11', 'NIT (SECTOR DE TB)', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '12', 'NUMERO CANCRO CERVICAL', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '15', 'DISA NID', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '16', 'CRAM ID', ''),
('IDENTIFIER', 'IDENTIFIER_TYPE', '17', 'NID PREP', ''),
('PATIENT', 'GENDER', 'M', 'MASCULINO', ''),
('PATIENT', 'GENDER', 'F', 'FEMININO', ''),
('PATIENT','BIRTHDATE_ESTIMATED', '1', "YES, DATE OF BIRTH IS ESTIMATED", ''),
('PATIENT','BIRTHDATE_ESTIMATED', '0', "NO, DATE OF BIRTH IS NOT ESTIMATED", ''),
('PATIENT_STATE', 'PROGRAM_ID', '1', 'SERVICO TARV - CUIDADO', ''),
('PATIENT_STATE', 'PROGRAM_ID', '2', 'SERVICO TARV - TRATAMENTO', ''),
('PATIENT_STATE', 'PROGRAM_ID', '5', 'TUBERCULOSE', ''),
('PATIENT_STATE', 'PROGRAM_ID', '6', 'CCR', ''),
('PATIENT_STATE', 'PROGRAM_ID', '7', 'CCU', ''),
('PATIENT_STATE', 'PROGRAM_ID', '8', 'PTV/ETV', ''),
('PATIENT_STATE', 'PROGRAM_ID', '9', 'CLINICA MOVEL', ''),
('PATIENT_STATE', 'PROGRAM_ID', '25', 'PREP', ''),
('PATIENT_STATE', 'SOURCE_ID', '1', 'SERVICO TARV - CUIDADO', ''),
('PATIENT_STATE', 'SOURCE_ID', '2', 'SERVICO TARV - TRATAMENTO', ''),
('PATIENT_STATE', 'SOURCE_ID', '5', 'TUBERCULOSE', ''),
('PATIENT_STATE', 'SOURCE_ID', '6', 'CCR', ''),
('PATIENT_STATE', 'SOURCE_ID', '7', 'CCU', ''),
('PATIENT_STATE', 'SOURCE_ID', '8', 'PTV/ETV', ''),
('PATIENT_STATE', 'SOURCE_ID', '9', 'CLINICA MOVEL', ''),
('PATIENT_STATE', 'SOURCE_ID', '25', 'PREP', ''),
('PATIENT_STATE', 'SOURCE_ID', '0', 'DEMOGRAPHIC', ''),
('PATIENT_STATE', 'SOURCE_ID', '165', 'FICHA RESUMO', ''),
('PATIENT_STATE', 'SOURCE_ID', '164', 'FICHA CLINICA', ''),
('PATIENT_STATE', 'SOURCE_ID', '121', 'TARV: VISITA DOMICILIARIA', ''),
('PATIENT_STATE', 'STATE_ID', '50', 'ABORTION', ''),
('PATIENT_STATE', 'STATE_ID', '1256', 'START', ''),
('PATIENT_STATE', 'STATE_ID', '1366', 'DIED', ''),
('PATIENT_STATE', 'STATE_ID', '1369', 'TRANSFERRED IN', ''),
('PATIENT_STATE', 'STATE_ID', '1705', 'RESTART', ''),
('PATIENT_STATE', 'STATE_ID', '1706', 'TRANSFERRED OUT', ''),
('PATIENT_STATE', 'STATE_ID', '1707', 'DROPPED FROM TREATMENT', ''),
('PATIENT_STATE', 'STATE_ID', '1709', 'SUSPEND TREATMENT', ''),
('PATIENT_STATE', 'STATE_ID', '1798', 'MEDICAL CONSULTATION', ''),
('PATIENT_STATE', 'STATE_ID', '1982', 'PREGNANT', ''),
('PATIENT_STATE', 'STATE_ID', '6248', 'BIRTH', ''),
('PATIENT_STATE', 'STATE_ID', '6269', 'ACTIVE', ''),
('PATIENT_STATE', 'STATE_ID', '6301', 'CURED', ''),
('PATIENT_STATE', 'STATE_ID', '6302', 'TB CURED', ''),
('PATIENT_STATE', 'STATE_ID', '23903', 'HIV NEGATIVE', ''),
('PATIENT_STATE', 'STATE_ID', '23863', 'AUTO TRANSFERENCE', ''),
('PATIENT_STATE', 'STATE_ID', '165231', 'EXIT - HIV POSITIVE TESTED', ''),
('PATIENT_STATE', 'STATE_ID', '165232', 'EXIT - NO MORE SUBSTANTIAL RISKS', ''),
('PATIENT_STATE', 'STATE_ID', '165233', 'EXIT- SIDE EFFECTS', ''),
('PATIENT_STATE', 'STATE_ID', '165234', 'EXIT - USER PREFERENCE', ''),
('MEDICATION','REGIMEN_ID','1703','AZT+3TC+EFV',''),
('MEDICATION','REGIMEN_ID','6100','AZT+3TC+LPV/r',''),
('MEDICATION','REGIMEN_ID','1651','AZT+3TC+NVP',''),
('MEDICATION','REGIMEN_ID','6324','TDF+3TC+EFV',''),
('MEDICATION','REGIMEN_ID','6104','ABC+3TC+EFV',''),
('MEDICATION','REGIMEN_ID','23784','TDF+3TC+DTG',''),
('MEDICATION','REGIMEN_ID','23785','TDF+3TC+DTG',''),
('MEDICATION','REGIMEN_ID','23786','ABC+3TC+DTG',''),
('MEDICATION','REGIMEN_ID','6116','AZT+3TC+ABC',''),
('MEDICATION','REGIMEN_ID','6106','ABC+3TC+LPV/r',''),
('MEDICATION','REGIMEN_ID','6105','ABC+3TC+NVP',''),
('MEDICATION','REGIMEN_ID','6108','TDF+3TC+LPV/r',''),
('MEDICATION','REGIMEN_ID','6424','TDF+3TC+LPV/r',''),
('MEDICATION','REGIMEN_ID','23790','TDF+3TC+LPV/r+RTV',''),
('MEDICATION','REGIMEN_ID','23791','TDF+3TC+ATV/r',''),
('MEDICATION','REGIMEN_ID','23792','ABC+3TC+ATV/r',''),
('MEDICATION','REGIMEN_ID','23793','AZT+3TC+ATV/r',''),
('MEDICATION','REGIMEN_ID','23795','ABC+3TC+ATV/r+RAL',''),
('MEDICATION','REGIMEN_ID','23796','TDF+3TC+ATV/r+RAL',''),
('MEDICATION','REGIMEN_ID','23801','AZT+3TC+RAL',''),
('MEDICATION','REGIMEN_ID','23802','AZT+3TC+DRV/r',''),
('MEDICATION','REGIMEN_ID','23815','AZT+3TC+DTG',''),
('MEDICATION','REGIMEN_ID','6329','TDF+3TC+RAL+DRV/r',''),
('MEDICATION','REGIMEN_ID','23797','ABC+3TC+DRV/r+RAL',''),
('MEDICATION','REGIMEN_ID','23798','3TC+RAL+DRV/r',''),
('MEDICATION','REGIMEN_ID','23803','AZT+3TC+RAL+DRV/r',''),
('MEDICATION','REGIMEN_ID','6243','TDF+3TC+NVP',''),
('MEDICATION','REGIMEN_ID','6103','D4T+3TC+LPV/r',''),
('MEDICATION','REGIMEN_ID','792','D4T+3TC+NVP',''),
('MEDICATION','REGIMEN_ID','1827','D4T+3TC+EFV',''),
('MEDICATION','REGIMEN_ID','6102','D4T+3TC+ABC',''),
('MEDICATION','REGIMEN_ID','1311','ABC+3TC+LPV/r',''),
('MEDICATION','REGIMEN_ID','1312','ABC+3TC+NVP',''),
('MEDICATION','REGIMEN_ID','1313','ABC+3TC+EFV',''),
('MEDICATION','REGIMEN_ID','1314','AZT+3TC+LPV/r',''),
('MEDICATION','REGIMEN_ID','1315','TDF+3TC+EFV',''),
('MEDICATION','REGIMEN_ID','6330','AZT+3TC+RAL+DRV/r',''),
('MEDICATION','REGIMEN_ID','6325','D4T+3TC+ABC+LPV/r',''),
('MEDICATION','REGIMEN_ID','6326','AZT+3TC+ABC+LPV/r',''),
('MEDICATION','REGIMEN_ID','6327','D4T+3TC+ABC+EFV',''),
('MEDICATION','REGIMEN_ID','6328','AZT+3TC+ABC+EFV',''),
('MEDICATION','REGIMEN_ID','6109','AZT+DDI+LPV/r',''),
('MEDICATION','REGIMEN_ID','21163','AZT+3TC+LPV/r',''),
('MEDICATION','REGIMEN_ID','23799','TDF+3TC+DTG',''),
('MEDICATION','REGIMEN_ID','23800','ABC+3TC+DTG',''),
('MEDICATION','REGIMEN_ID','6110','D4T20+3TC+NVP',''),
('MEDICATION','REGIMEN_ID','1702','AZT+3TC+NFV',''),
('MEDICATION','REGIMEN_ID','817','AZT+3TC+ABC',''),
('MEDICATION','REGIMEN_ID','6244','AZT+3TC+RTV',''),
('MEDICATION','REGIMEN_ID','1700','AZT+DDl+NFV',''),
('MEDICATION','REGIMEN_ID','633','EFV',''),
('MEDICATION','REGIMEN_ID','625','D4T',''),
('MEDICATION','REGIMEN_ID','631','NVP',''),
('MEDICATION','REGIMEN_ID','628','3TC',''),
('MEDICATION','REGIMEN_ID','635','NFV',''),
('MEDICATION','REGIMEN_ID','797','AZT',''),
('MEDICATION','REGIMEN_ID','814','ABC',''),
('MEDICATION','REGIMEN_ID','6107','TDF+AZT+3TC+LPV/r',''),
('MEDICATION','REGIMEN_ID','6236','D4T+DDI+RTV-IP',''),
('MEDICATION','REGIMEN_ID','1701','ABC+DDI+NFV',''),
('MEDICATION','REGIMEN_ID','6114','3DFC',''),
('MEDICATION','REGIMEN_ID','6115','DFC+EFV',''),
('MEDICATION','REGIMEN_ID','6233','AZT+3TC+DDI+LPV',''),
('MEDICATION','REGIMEN_ID','6234','ABC+TDF+LPV',''),
('MEDICATION','REGIMEN_ID','6242','D4T+DDI+NVP',''),
('MEDICATION','REGIMEN_ID','6118','DI50+ABC+LPV',''),
('MEDICATION','REGIMEN_ID','23787','ABC+AZT+LPV/r',''),
('MEDICATION','REGIMEN_ID','5424','OTHER',''),
('MEDICATION','FORMULATION_ID','11','[TDF/3TC/DTG] Tenofovir 300mg/Lamivudina 300mg/Dolutegravir 50mg TLD30',''),
('MEDICATION','FORMULATION_ID','12','[TDF/3TC/DTG] Tenofovir 300mg/Lamivudina 300mg/Dolutegravir 50mg TLD90',''),
('MEDICATION','FORMULATION_ID','13','[TDF/3TC/DTG] Tenofovir 300mg/Lamivudina 300mg/Dolutegravir 50mg TLD180',''),
('MEDICATION','FORMULATION_ID','17','[LPV/RTV] Lopinavir/Ritonavir -Aluvia 200mg/50mg',''),
('MEDICATION','FORMULATION_ID','18','[ABC/3TC] Abacavir 600mg/Lamivudina 300mg',''),
('MEDICATION','FORMULATION_ID','19','[DTG] Dolutegravir 50mg',''),
('MEDICATION','FORMULATION_ID','20','[ABC/3TC] Abacavir 120mg/Lamivudina 60mg',''),
('MEDICATION','FORMULATION_ID','21','[ABC/3TC] Abacavir 60 and Lamivudina 30mg',''),
('MEDICATION','FORMULATION_ID','22','[3TC/AZT] Lamivudina 150mg/ Zidovudina 300mg',''),
('MEDICATION','FORMULATION_ID','23','[3TC/AZT] Lamivudina 30mg/ Zidovudina 60mg',''),
('MEDICATION','FORMULATION_ID','24','[TDF/3TC] Tenofovir 300mg/Lamivudina 300mg',''),
('MEDICATION','FORMULATION_ID','25','[RAL] Raltegravir 400mg',''),
('MEDICATION','FORMULATION_ID','26','[LPV/RTV] Lopinavir/Ritonavir 400mg/100mg',''),
('MEDICATION','FORMULATION_ID','27','[LPV/RTV] Lopinavir/Ritonavir -Aluvia 100mg/25mg',''),
('MEDICATION','FORMULATION_ID','28','[LPV/RTV] Lopinavir/Ritonavir 200mg/50mg',''),
('MEDICATION','FORMULATION_ID','29','[LPV/RTV] Lopinavir/Ritornavir 40mg/10mg Pellets/Granulos',''),
('MEDICATION','FORMULATION_ID','30','[LPV/RTV] Lopinavir/Ritonavir-Kaletra 80/20 mg/ml',''),
('MEDICATION','FORMULATION_ID','31','[ATV/RTV] Atazanavir 300mg/Ritonavir 100mg',''),
('MEDICATION','FORMULATION_ID','32','[NVP] Nevirapine 200mg',''),
('MEDICATION','FORMULATION_ID','33','[NVP] Nevirapina 50mg',''),
('MEDICATION','FORMULATION_ID','34','[NVP] Nevirapine 50mg/5ml',''),
('MEDICATION','FORMULATION_ID','35','[AZT] Zidovudine 50mg/5ml',''),
('MEDICATION','FORMULATION_ID','36','[AZT] Zidovudine 300mg',''),
('MEDICATION','FORMULATION_ID','37','[ABC] Abacavir 300mg',''),
('MEDICATION','FORMULATION_ID','38','[ABC] Abacavir 60mg',''),
('MEDICATION','FORMULATION_ID','39','[EFV] Efavirenz 600mg',''),
('MEDICATION','FORMULATION_ID','40','[EFV] Efavirenz 200mg',''),
('MEDICATION','FORMULATION_ID','41','[3TC] Lamivudine150mg',''),
('MEDICATION','FORMULATION_ID','42','[TDF] Tenofovir 300mg',''),
('MEDICATION','FORMULATION_ID','43','[TDF/3TC/EFV] Tenofovir 300mg/Lamivudina 300mg/Efavirenze 400mg TLE90',''),
('MEDICATION','FORMULATION_ID','44','[TDF/3TC/EFV] Tenofovir 300mg/Lamivudina 300mg/Efavirenze 400mg TLE30',''),
('MEDICATION','FORMULATION_ID','45','[TDF/3TC/EFV] Tenofovir 300mg/Lamivudina 300mg/Efavirenze 400mg TLE180',''),
('MEDICATION','FORMULATION_ID','46','[TDF/3TC/EFV] Tenofovir 300mg/Lamivudina 300mg/Efavirenze 600mg',''),
('MEDICATION','FORMULATION_ID','47','[3TC/AZT/NVP] Lamivudina 150mg/Zidovudina 300mg/Nevirapina 200mg',''),
('MEDICATION','FORMULATION_ID','48','[3TC/AZT/NVP] Lamivudina 30mg/Zidovudina 60mg/Nevirapina 50mg',''),
('MEDICATION','FORMULATION_ID','49','[3TC/AZT/ABC] Lamivudina 150mg/Zidovudina 300mg/Abacavir 300mg',''),
('MEDICATION','FORMULATION_ID','50','[TDF/FTC] Tenofovir 300mg/Emtricitabina 200mg',''),
('MEDICATION','FORMULATION_ID','51','[DTG] Dolutegravir 10 mg 90 Comp',''),
('MEDICATION','FORMULATION_ID','52','[DTG] Dolutegravir 10 mg 30 Comp',''),
('MEDICATION','FORMULATION_ID','53','[ABC/3TC] Abacavir 120mg/Lamivudina 60mg 30 Comp',''),
('MEDICATION','MODE_DISPENSATION_ID','165175','NORMAL EXPEDIENT SCHEDULE',''),
('MEDICATION','MODE_DISPENSATION_ID','165176','OUT OF TIME',''),
('MEDICATION','MODE_DISPENSATION_ID','165177','FARMAC/PRIVATE PHARMACY',''),
('MEDICATION','MODE_DISPENSATION_ID','165178','COMMUNITY DISPENSE VIA PROVIDER',''),
('MEDICATION','MODE_DISPENSATION_ID','165179','COMMUNITY DISPENSE VIA APE',''),
('MEDICATION','MODE_DISPENSATION_ID','165180','DAILY MOBILE BRIGADES',''),
('MEDICATION','MODE_DISPENSATION_ID','165181','NIGHT MOBILE BRIGADES (HOTSPOTS)',''),
('MEDICATION','MODE_DISPENSATION_ID','165182','DAILY MOBILE CLINICS',''),
('MEDICATION','MODE_DISPENSATION_ID','165183','NIGHT MOBILE CLINICS (HOTSPOTS)',''),
('MEDICATION','MED_LINE_ID','21150','FIRST LINE',''),
('MEDICATION','MED_LINE_ID','21148','SECOND LINE',''),
('MEDICATION','MED_LINE_ID','21149','THIRD LINE',''),
('MEDICATION','MED_LINE_ID','23741','ALTERNATIVE 1st LINE OF THE ART',''),
('MEDICATION','MED_LINE_ID','23893','FIRST LINE',''),
('MEDICATION','MED_LINE_ID','21190','ALTERNATIVE 1st LINE OF THE ART',''),
('MEDICATION','MED_LINE_ID','21187','SECOND LINE',''),
('MEDICATION','MED_LINE_ID','21188','THIRD LINE',''),
('MEDICATION','TYPE_DISPENSATION_ID','1098','MONTHLY',''),
('MEDICATION','TYPE_DISPENSATION_ID','23720','QUARTERLY',''),
('MEDICATION','TYPE_DISPENSATION_ID','23888','SEMESTER',''),
('MEDICATION','TYPE_DISPENSATION_ID','165314','ANNUAL',''),
('MEDICATION','ALTERNATIVE_LINE_ID','1066','NO',''),
('MEDICATION','ALTERNATIVE_LINE_ID','1371','REGIMEN SWITCH',''),
('MEDICATION','ALTERNATIVE_LINE_ID','23741','ALTERNATIVE 1st LINE OF THE ART',''),
('MEDICATION','REASON_CHANGE_REGIMEN_ID','102','TOXICITY',''),
('MEDICATION','REASON_CHANGE_REGIMEN_ID','1982','PREGNANT',''),
('MEDICATION','REASON_CHANGE_REGIMEN_ID','1368','PREGNANT RISK',''),
('MEDICATION','REASON_CHANGE_REGIMEN_ID','1113','TB DRUG START',''),
('MEDICATION','REASON_CHANGE_REGIMEN_ID','23744','NEW MEDICATION AVAILABLE',''),
('MEDICATION','REASON_CHANGE_REGIMEN_ID','23745','ART STOCK BREAK',''),
('MEDICATION','REASON_CHANGE_REGIMEN_ID','1790','CLINICAL FAILURE',''),
('MEDICATION','REASON_CHANGE_REGIMEN_ID','1791','IMMUNOLOGICAL FAILURE',''),
('MEDICATION','REASON_CHANGE_REGIMEN_ID','23746','VIROLOGICAL FAULT',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','151','ABDOMINAL PAIN',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','14475','Nausea and Vomiting',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','16','DIARRHEA',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','6295','PSYCHOLOGICAL CHANGES',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','821','PERIPHERAL NEUROPATHY',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','11428','GENERALIZED SKIN RASH',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','14671','Hyperglycaemia',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','23747','DYSLIPIDEMIA',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','6298','LIPODYSTROPHY',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','23748','CYTOPENIA',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','6293','PANCREATITIS',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','23749','NEPHROTOXICITY',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','29','HEPATITIS',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','23750','STEVENS-JOHNSON SYNDROME',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','23751','HYPERSENSITIVITY TO ABC/RAL',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','6299','LACTIC ACIDOSIS',''),
('MEDICATION','ARV_SIDE_EFFECT_ID','23752','HEPATIC STEATOSIS WITH HYPERLACTATEMIA',''),
('MEDICATION','ADHERENCE_ID','1383','GOOD',''),
('MEDICATION','ADHERENCE_ID','1749','ARV ADHERENCE RISK',''),
('MEDICATION','ADHERENCE_ID','1385','BAD',''),
('DSD','DSD_ID','23730','QUARTERLY DISPENSATION (DT)',''),
('DSD','DSD_ID','23888','SEMESTER ARV PICKUP',''),
('DSD','DSD_ID','165314','ARV ANUAL DISPENSATION',''),
('DSD','DSD_ID','165315','DECENTRALIZED ARV DISPENSATION',''),
('DSD','DSD_ID','165178','COMMUNITY DISPENSE VIA PROVIDER',''),
('DSD','DSD_ID','165179','COMMUNITY DISPENSE VIA APE',''),
('DSD','DSD_ID','165264','MOBILE BRIGADES',''),
('DSD','DSD_ID','165265','MOBILE CLINICS',''),
('DSD','DSD_ID','23725','FAMILY APPROACH',''),
('DSD','DSD_ID','23729','RAPID FLOW (FR)',''),
('DSD','DSD_ID','23724','GAAC',''),
('DSD','DSD_ID','23726','ACCESSION CLUBS (CA)',''),
('DSD','DSD_ID','165316','HOURS EXTENSION',''),
('DSD','DSD_ID','165317','SINGLE STOP IN TB SECTOR',''),
('DSD','DSD_ID','165318','SINGLE STOP ON TARV SERVICES',''),
('DSD','DSD_ID','165319','SINGLE STOP SAAJ',''),
('DSD','DSD_ID','165320','SINGLE STOP SMI',''),
('DSD','DSD_ID','165321','HIV ADVANCED DISEASE',''),
('DSD','DSD_STATE_ID','1256','START',''),
('DSD','DSD_STATE_ID','1257','CONTINUE',''),
('DSD','DSD_STATE_ID','1267','COMPLETED',''),
('KEYPOP_VULNERABLE','POP_TYPE','23703','KEY POPULATION',''),
('KEYPOP_VULNERABLE','POP_TYPE','23710','VULNERABLE PEOPLE',''),
('KEYPOP_VULNERABLE','POP_ID','1377','MEN WHO HAVE SEX WITH MEN',''),
('KEYPOP_VULNERABLE','POP_ID','20454','DRUG USE',''),
('KEYPOP_VULNERABLE','POP_ID','20426','IMPRISONMENT AND OTHER INCARCERATION',''),
('KEYPOP_VULNERABLE','POP_ID','1901','SEX WORKER',''),
('KEYPOP_VULNERABLE','POP_ID','165205','TRANSGENDER',''),
('KEYPOP_VULNERABLE','POP_ID','5622','OTHER',''),
('KEYPOP_VULNERABLE','POP_ID','23885','KEY POPULATION - OTHERS',''),
('KEYPOP_VULNERABLE','POP_ID','23711','GIRL BETWEEN 10-14 YEARS',''),
('KEYPOP_VULNERABLE','POP_ID','23712','YOUNG WOMAN BETWEEN 15-24 YEARS',''),
('KEYPOP_VULNERABLE','POP_ID','1995','THE COUPLES RESULTS ARE DIFFERENT',''),
('KEYPOP_VULNERABLE','POP_ID','1977','ASSOCIATION OF PEOPLE WITH HIV',''),
('KEYPOP_VULNERABLE','POP_ID','1174','ORPHAN',''),
('KEYPOP_VULNERABLE','POP_ID','23713','PERSON WITH DISABILITY',''),
('KEYPOP_VULNERABLE','POP_ID','1908','MINER',''),
('KEYPOP_VULNERABLE','POP_ID','1903','DRIVER',''),
('KEYPOP_VULNERABLE','POP_ID','1904','MIGRANT WORKER',''),
('KEYPOP_VULNERABLE','POP_ID','23890','SEASONAL WORKERS',''),
('LABORATORY', 'LAB_TEST_ID', '856', 'HIV VIRAL LOAD', ''),
('LABORATORY', 'LAB_TEST_ID', '1305', 'VIRAL LOAD - QUAL', ''),
('LABORATORY', 'LAB_TEST_ID', '1695', 'CD4 COUNT',''),
('LABORATORY', 'LAB_TEST_ID', '5497', 'CD4 COUNT',''),
('LABORATORY', 'LAB_TEST_ID', '23896', 'CD4 COUNT',''),
('LABORATORY', 'LAB_TEST_ID', '730', 'CD4%', ''),
('LABORATORY', 'LAB_TEST_ID', '1030', 'HIV PCR TEST', ''),
('LABORATORY', 'LAB_TEST_ID', '1040', 'HIV RAPID TEST', ''),
('LABORATORY', 'LAB_TEST_ID', '23722', 'LAB TEST REQUEST', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '703', 'POSITIVE', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '664', 'NEGATIVE', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '1067', 'UNKNOWN', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '1138', 'INDETERMINATE', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '1306', 'BEYOND DETECTABLE LIMIT', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '1304', 'POOR SAMPLE QUALITY', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '23814', 'UNDETECTABLE', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '23907', 'LESS THAN 40 COPIES/ML', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '23905', 'LESS THAN 10 COPIES/ML', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '23904', 'LESS THAN 839 COPIES/ML', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '23906', 'LESS THAN 20 COPIES/ML', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '23908', 'LESS THAN 400 COPIES/ML', ''),
('LABORATORY', 'RESULT_QUALITATIVE_ID', '165331','LESS THAN', ''),
('LABORATORY', 'SPECIMEN_TYPE_ID', '1002', 'PLASMA', ''),
('LABORATORY', 'SPECIMEN_TYPE_ID', '23831', 'DRY BLOOD SPOT', '')