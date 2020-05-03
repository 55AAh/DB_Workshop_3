/*DROP VIEW ActiveEmployee;
DROP VIEW MoreMoneyTermEmployeeManager;*/

CREATE VIEW
	ActiveEmployee
AS
SELECT
    Human_human_id AS human_id,
    Company_company_id AS company_id,
    Department_department_id AS department_id,
    EmplPos_position_id AS position_id,
    PerfScore_pscore_id AS pscore_id,
    empl_satisfaction
FROM
	Empl
    JOIN EmplStatus
        ON Empl.EmplStatus_estatus_id = EmplStatus.estatus_id
WHERE
    TRIM(EmplStatus.status) = 'Active';



CREATE VIEW
    MoreMoneyTermEmployeeManager
AS
SELECT
    Human_human_id AS human_id,
    Company_company_id AS company_id,
    Department_department_id AS department_id,
    EmplPos_position_id AS position_id,
    PerfScore_pscore_id AS pscore_id,
    empl_satisfaction,
    Manager.name as manager_name
FROM
	Empl
    JOIN Empl_TermReason
        ON Empl.Human_human_id = Empl_TermReason.Empl_Human_human_id
        AND Empl.Company_company_id = Empl_TermReason.Empl_Company_company_id
        AND Empl.Department_department_id = Empl_TermReason.Empl_Department_department_id
        AND Empl.EmplPos_position_id = Empl_TermReason.Empl_EmplPos_position_id
    JOIN TermReason
        ON Empl_TermReason.TermReason_treason_id = TermReason.treason_id
    JOIN Empl_Manager
        ON Empl.Human_human_id = Empl_Manager.Empl_Human_human_id
        AND Empl.Company_company_id = Empl_Manager.Empl_Company_company_id
        AND Empl.Department_department_id = Empl_Manager.Empl_Department_department_id
        AND Empl.EmplPos_position_id = Empl_Manager.Empl_EmplPos_position_id    
    JOIN Manager
        ON Empl_Manager.Manager_manager_id = Manager.manager_id
WHERE
    TRIM(TermReason.reason) = 'more money';