--  Запит 1 - Вивести кількість неодружених працюючих техніків 1-го розряду в кожній компанії

SELECT
    TRIM(Company.name) AS company,
    COUNT(*) AS count
FROM
    ActiveEmployee
    JOIN EmplPos
        ON ActiveEmployee.position_id = EmplPos.position_id
    JOIN Human
        ON ActiveEmployee.human_id = Human.human_id
    JOIN MrtStatus
        ON Human.MrtStatus_mstatus_id = MrtStatus.mstatus_id
    JOIN Company
        ON ActiveEmployee.company_id = Company.company_id
WHERE
    TRIM(MrtStatus.status) != 'Married'
    AND TRIM(EmplPos.name) = 'Production Technician I'
GROUP BY
    TRIM(Company.name)
ORDER BY
    COUNT(*) DESC;


--  Запит 2 - Вивести імена п'яти менеджерів, у яких звільнилося найбільше низькооплачуваних робітників з Массачусетсу, разом з відповідною кількістю

SELECT
    manager_name,
    terminated_count
FROM (
    SELECT
        TRIM(MoreMoneyTermEmployeeManager.manager_name) AS manager_name,
        COUNT(*) AS terminated_count
    FROM
        MoreMoneyTermEmployeeManager
        JOIN Human_ZipCode
            ON MoreMoneyTermEmployeeManager.human_id = Human_ZipCode.Human_human_id
        JOIN ZipCode
            ON Human_ZipCode.ZipCode_zipcode = zipcode.zipcode
        JOIN StateInfo
            ON ZipCode.StateInfo_state_id = StateInfo.state_id
    WHERE
        TRIM(StateInfo.name) = 'MA'
    GROUP BY
        TRIM(MoreMoneyTermEmployeeManager.manager_name)
    ORDER BY
        terminated_count DESC
)
WHERE ROWNUM <= 5;


--  Запит 3 - Вивести середній рівень задоволення активно працюючих професіоналів в кожному департаменті всіх компаній

SELECT
    Department.name AS department_name,
    ROUND(AVG(ActiveEmployee.empl_satisfaction), 1) AS avg_satisfaction_level
FROM
    ActiveEmployee
    JOIN PerfScore
        ON ActiveEmployee.pscore_id = PerfScore.pscore_id
    JOIN Department
        ON ActiveEmployee.department_id = Department.department_id
WHERE
    TRIM(PerfScore.score) = 'Fully Meets'
GROUP BY
    Department.name;