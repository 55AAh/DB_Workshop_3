import cx_Oracle
from tqdm import tqdm

def parse_line(line):
	line.replace('""', '')
	if line[0] == '"' and line[-1] == '"':
		line = line[1:-1]
	if line=="" or line[:2]==",,":
		return None
	line_sp = line.split(",")
	try:
		_ = int(line_sp[1])
	except ValueError:
		line_sp[0] += line_sp[1]
		del line_sp[1]
	if line_sp[23]=="\"\"no-call":
		del line_sp[24]
		line_sp[23] = "no-call, no-show"
	if line_sp[10] == "1" and line_sp[23] == "":
		line_sp[23] = "unknown"
	return line_sp

db_conn = cx_Oracle.connect('kostia', 'my_password', 'localhost:1521/XE')
db_curr = db_conn.cursor()

db_curr.execute("DELETE FROM Empl_Manager")
db_curr.execute("DELETE FROM Manager")
db_curr.execute("DELETE FROM Empl_TermReason")
db_curr.execute("DELETE FROM TermReason")
db_curr.execute("DELETE FROM Empl")
db_curr.execute("DELETE FROM Human_ZipCode")
db_curr.execute("DELETE FROM ZipCode")
db_curr.execute("DELETE FROM StateInfo")
db_curr.execute("DELETE FROM Human")
db_curr.execute("DELETE FROM MrtStatus")
db_curr.execute("DELETE FROM Company")
db_curr.execute("DELETE FROM Department")
db_curr.execute("DELETE FROM EmplPos")
db_curr.execute("DELETE FROM EmplStatus")
db_curr.execute("DELETE FROM PerfScore")
db_conn.commit()

with open("data_source/HRDataset_v13.csv") as source_file:
	for line in tqdm(source_file.readlines()[1:]):
		record = parse_line(line.strip())
		if record is None:
			continue
		employee_name = record[0]
		employee_id = record[1]
		married_id = record[2]
		marital_status_id = record[3]
		gender_id = record[4]
		employment_status_id = record[5]
		dept_id = record[6]
		perf_score_id = record[7]
		from_diversity_job_fair = record[8]
		pay_rate = record[9]
		term_d = record[10]
		position_id = record[11]
		position = record[12]
		state = record[13]
		zipcode = record[14]
		birth_date = record[15]
		sex = record[16]
		marital_status_desc = record[17]
		citizen_desc = record[18]
		hispanic_latino = record[19]
		race_desc = record[20]
		hire_date = record[21]
		term_date = record[22]
		term_reason = record[23]
		employment_status = record[24]
		department = record[25]
		manager_name = record[26]
		manager_id = record[27]
		recruitment_source = record[28]
		performance_score = record[29]
		engagement_survey = record[30]
		employee_satisfaction = record[31]
		special_projects_count = record[32]
		last_performance_review_date = record[33]
		days_late_last_30 = record[34]
	# MrtStatus
		_marital_status_id = db_curr.execute("SELECT mstatus_id FROM MrtStatus WHERE status = :status", status = marital_status_desc).fetchall()
		if len(_marital_status_id) == 0:
			_marital_status_id = db_curr.execute("SELECT NVL(MAX(mstatus_id)+1,0) FROM MrtStatus").fetchall()[0][0]
			db_curr.execute("INSERT INTO MrtStatus (mstatus_id, status) VALUES (:mstatus_id, :status)", mstatus_id = _marital_status_id, status = marital_status_desc)
		else:
			_marital_status_id = _marital_status_id[0][0]
	# Human 
		_human_id = db_curr.execute("SELECT NVL(MAX(human_id)+1,0) FROM Human").fetchall()[0][0]
		db_curr.execute("INSERT INTO Human (human_id, MrtStatus_mstatus_id) VALUES (:human_id, :mstatus_id)", human_id = _human_id, mstatus_id = _marital_status_id)
	# StateInfo
		_state_id = db_curr.execute("SELECT state_id FROM StateInfo WHERE name = :name", name = state).fetchall()
		if len(_state_id) == 0:
			_state_id = db_curr.execute("SELECT NVL(MAX(state_id)+1,0) FROM StateInfo").fetchall()[0][0]
			db_curr.execute("INSERT INTO StateInfo (state_id, name) VALUES (:state_id, :name)", state_id = _state_id, name = state)
		else:
			_state_id = _state_id[0][0]
	# ZipCode
		if db_curr.execute("SELECT COUNT(*) FROM ZipCode WHERE zipcode = :zipcode", zipcode = zipcode).fetchall()[0][0] == 0:
			db_curr.execute("INSERT INTO ZipCode (zipcode, StateInfo_state_id) VALUES(:zipcode, :state_id)", zipcode=zipcode, state_id = _state_id)
	# HumanZipCode
		db_curr.execute("INSERT INTO Human_ZipCode (ZipCode_zipcode, Human_human_id) VALUES (:zipcode, :human_id)", zipcode = zipcode, human_id = _human_id)
	# Company
		_company_id = db_curr.execute("SELECT company_id FROM Company WHERE name = :name", name = recruitment_source).fetchall()
		if len(_company_id) == 0:
			_company_id = db_curr.execute("SELECT NVL(MAX(company_id)+1,0) FROM Company").fetchall()[0][0]
			db_curr.execute("INSERT INTO Company (company_id, name) VALUES (:company_id, :name)", company_id = _company_id, name = recruitment_source)
		else:
			_company_id = _company_id[0][0]
	# Department
		_department_id = db_curr.execute("SELECT department_id FROM Department WHERE name = :name", name = department).fetchall()
		if len(_department_id) == 0:
			_department_id = db_curr.execute("SELECT NVL(MAX(department_id)+1,0) FROM Department").fetchall()[0][0]
			db_curr.execute("INSERT INTO Department (department_id, name) VALUES (:department_id, :name)", department_id = _department_id, name = department)
		else:
			_department_id = _department_id[0][0]
	# EmplPos
		_position_id = db_curr.execute("SELECT position_id FROM EmplPos WHERE name = :name", name = position).fetchall()
		if len(_position_id) == 0:
			_position_id = db_curr.execute("SELECT NVL(MAX(position_id)+1,0) FROM EmplPos").fetchall()[0][0]
			db_curr.execute("INSERT INTO EmplPos (position_id, name) VALUES (:position_id, :name)", position_id = _position_id, name = position)
		else:
			_position_id = _position_id[0][0]
	# PerfScore
		_pscore_id = db_curr.execute("SELECT pscore_id FROM PerfScore WHERE score = :score", score = performance_score).fetchall()
		if len(_pscore_id) == 0:
			_pscore_id = db_curr.execute("SELECT NVL(MAX(pscore_id)+1,0) FROM PerfScore").fetchall()[0][0]
			db_curr.execute("INSERT INTO PerfScore (pscore_id, score) VALUES (:pscore_id, :score)", pscore_id = _pscore_id, score = performance_score)
		else:
			_pscore_id = _pscore_id[0][0]
	# Manager
		_manager_id = db_curr.execute("SELECT manager_id FROM Manager WHERE name = :name", name = manager_name).fetchall()
		if len(_manager_id) == 0:
			_manager_id = db_curr.execute("SELECT NVL(MAX(manager_id)+1,0) FROM Manager").fetchall()[0][0]
			db_curr.execute("INSERT INTO Manager (manager_id, name) VALUES (:manager_id, :name)", manager_id = _manager_id, name = manager_name)
		else:
			_manager_id = _manager_id[0][0]
	#EmplStatus
		_estatus_id = db_curr.execute("SELECT estatus_id FROM EmplStatus WHERE status = :status", status = employment_status).fetchall()
		if len(_estatus_id) == 0:
			_estatus_id = db_curr.execute("SELECT NVL(MAX(estatus_id)+1,0) FROM EmplStatus").fetchall()[0][0]
			db_curr.execute("INSERT INTO EmplStatus (estatus_id, status) VALUES (:estatus_id, :status)", estatus_id = _estatus_id, status = employment_status)
		else:
			_estatus_id = _estatus_id[0][0]
	# TermReason
		_treason_id = db_curr.execute("SELECT treason_id FROM TermReason WHERE reason = :reason", reason = term_reason).fetchall()
		if len(_treason_id) == 0:
			_treason_id = db_curr.execute("SELECT NVL(MAX(treason_id)+1,0) FROM TermReason").fetchall()[0][0]
			db_curr.execute("INSERT INTO TermReason (treason_id, reason) VALUES (:treason_id, :reason)", treason_id = _treason_id, reason = term_reason)
		else:
			_treason_id = _treason_id[0][0]
	# Empl
		db_curr.execute("INSERT INTO Empl (Human_human_id, Company_company_id, Department_department_id, EmplPos_position_id, EmplStatus_estatus_id, PerfScore_pscore_id, empl_satisfaction) "
		"VALUES (:human_id, :company_id, :department_id, :position_id, :estatus_id, :pscore_id, :empl_satisfaction)",
		human_id = _human_id, company_id = _company_id, department_id = _department_id, position_id = _position_id, estatus_id = _estatus_id, pscore_id = _pscore_id, empl_satisfaction = employee_satisfaction)
	# Empl_TermReason
		if term_d == "1":
			db_curr.execute("INSERT INTO Empl_TermReason (Empl_Human_human_id, Empl_Company_company_id, Empl_Department_department_id, Empl_EmplPos_position_id, TermReason_treason_id) "
			"VALUES (:human_id, :company_id, :department_id, :position_id, :treason_id)",
			human_id = _human_id, company_id = _company_id, department_id = _department_id, position_id = _position_id, treason_id = _treason_id)
	# EmplManager
		db_curr.execute("INSERT INTO Empl_Manager (Empl_Human_human_id, Empl_Company_company_id, Empl_Department_department_id, Empl_EmplPos_position_id, Manager_manager_id) "
		"VALUES (:human_id, :company_id, :department_id, :position_id, :manager_id)",
		human_id = _human_id, company_id = _company_id, department_id = _department_id, position_id = _position_id, manager_id = _manager_id)
	db_conn.commit()
