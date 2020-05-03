import cx_Oracle
import chart_studio.plotly as py
import plotly.graph_objects as go
import re
import chart_studio.dashboard_objs as dashboard

def fileId_from_url(url):
    """Return fileId from a url."""
    raw_fileId = re.findall("~[A-z.]+/[0-9]+", url)[0][1: ]
    return raw_fileId.replace('/', ':')

db_conn = cx_Oracle.connect(user="kostia", password="my_password")
db_cur = db_conn.cursor()


# Запит 1 - Вивести кількість неодружених працюючих техніків 1-го розряду в кожній компанії
print("\tTask 1", flush=True)
fig1_x = []
fig1_y = []
print("Quering db...", end="", flush=True)
for row in db_cur.execute("""
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
    COUNT(*) DESC
"""):
    fig1_x.append(row[0])
    fig1_y.append(row[1])
print(" Done", flush=True)
print("Creating figure...", end="", flush=True)
fig1 = go.Figure(data=go.Bar(x=fig1_x, y=fig1_y), layout=go.Layout(
    xaxis=dict(
        title = "Компанії"
    ),
    yaxis=dict(
        title = "Кількість техніків"
    )
))
print(" Done", flush=True)
print("Plotting figure...", end="", flush=True)
plot1_url = py.plot(fig1, filename="DB_Laboratory_3_plot1")
print(" Done", flush=True)


# Запит 2 - Вивести імена п'яти менеджерів, у яких звільнилося найбільше низькооплачуваних робітників з Массачусетсу, разом з відповідною кількістю
print("\tTask 2", flush=True)
fig2_values = []
fig2_labels = []
print("Quering db...", end="", flush=True)
for row in db_cur.execute("""
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
WHERE ROWNUM <= 5
"""):
    fig2_labels.append(row[0])
    fig2_values.append(row[1])
print(" Done", flush=True)
print("Creating figure...", end="", flush=True)
fig2 = go.Figure(data=go.Pie(values=fig2_values, labels=fig2_labels))
print(" Done", flush=True)
print("Plotting figure...", end="", flush=True)
plot2_url = py.plot(fig2, filename="DB_Laboratory_3_plot2")
print(" Done", flush=True)


# Запит 3 - Вивести середній рівень задоволення активно працюючих професіоналів в кожному департаменті всіх компаній
print("\tTask 3", flush=True)
fig3_x = []
fig3_y = []
print("Quering db...", end="", flush=True)
for row in db_cur.execute("""
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
    Department.name
"""):
    fig3_x.append(row[0])
    fig3_y.append(row[1])
print(" Done", flush=True)
print("Creating figure...", end="", flush=True)
fig3 = go.Figure(data=go.Bar(x=fig3_x, y=fig3_y), layout=go.Layout(
    xaxis = dict(
        title = "Департамент"
    ),
    yaxis = dict(
        title = "Рівень задоволення"
    )
))
print(" Done", flush=True)
print("Plotting figure...", end="", flush=True)
plot3_url = py.plot(fig3, filename="DB_Laboratory_3_plot3")
print(" Done", flush=True)


print("Assembling dashboard...", end="", flush=True)
my_dboard = dashboard.Dashboard()

plot1_id = fileId_from_url(plot1_url)
plot2_id = fileId_from_url(plot2_url)
plot3_id = fileId_from_url(plot3_url)
box_1 = {
    "type": "box",
    "boxType": "plot",
    "fileId": plot1_id,
    "title": "Кількість неодружених техніків 1-го розряду"
}
box_2 = {
    "type": "box",
    "boxType": "plot",
    "fileId": plot2_id,
    "title": "Топ-5 менеджерів, у яких звільнилося найбільше низькооплачуваних робітників з Массачусетсу"
}
box_3 = {
    "type": "box",
    "boxType": "plot",
    "fileId": plot3_id,
    "title": "Рівень задоволення активно працюючих професіоналів"
}
my_dboard.insert(box_1)
my_dboard.insert(box_2, "below", 1)
my_dboard.insert(box_3, "left", 2)
py.dashboard_ops.upload(my_dboard, "DB_Laboratory_3")
print(" Done", flush=True)
