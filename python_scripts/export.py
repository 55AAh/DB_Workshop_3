import cx_Oracle
import csv
from tqdm import tqdm

db_conn = cx_Oracle.connect(user="kostia", password="my_password")
db_curr = db_conn.cursor()

def save_csv(rel_name, fields):
	with open("export_data/%s.csv" % rel_name, "w", newline="") as csv_file:
		csv_writer = csv.writer(csv_file)
		csv_writer.writerow(fields.split(","))
		for row in tqdm(db_curr.execute("SELECT %s FROM %s" % (
		",".join(["TRIM(%s)" % f for f in fields.split(",")]),
		rel_name)), desc=rel_name):
			csv_writer.writerow(row)

save_csv("StateInfo", "state_id,name")
save_csv("ZipCode", "zipcode,StateInfo_state_id")
save_csv("MrtStatus", "mstatus_id,status")
save_csv("Human", "human_id,MrtStatus_mstatus_id")
save_csv("Human_ZipCode", "Human_human_id,ZipCode_zipcode")
save_csv("Company", "company_id,name")
save_csv("Department", "department_id,name")
save_csv("EmplPos", "position_id,name")
save_csv("PerfScore", "pscore_id,score")
save_csv("EmplStatus", "estatus_id,status")
save_csv("Empl", "Human_human_id,Company_company_id,Department_department_id,EmplPos_position_id,EmplStatus_estatus_id,PerfScore_pscore_id,empl_satisfaction")
save_csv("TermReason", "treason_id,reason")
save_csv("Empl_TermReason", "Empl_Human_human_id,Empl_Company_company_id,Empl_Department_department_id,Empl_EmplPos_position_id,TermReason_treason_id")
save_csv("Manager", "manager_id,name")
save_csv("Empl_Manager", "Empl_Human_human_id,Empl_Company_company_id,Empl_Department_department_id,Empl_EmplPos_position_id,Manager_manager_Id")

db_conn.close()
