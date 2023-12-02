from flask import Flask, jsonify, request
from neo4j import GraphDatabase

app = Flask(__name__)

uri = "bolt://localhost:7687"
username = "neo4j"
password = "test1234"

driver = GraphDatabase.driver(uri, auth=(username, password))


def close_driver(driver):
    driver.close()


app.config["driver"] = driver


def get_employees(tx, filters=None, sort_by=None):
    query = "MATCH (emp:Employee)-[:WORKS_IN]->(dept:Department) "
    if filters:
        query += "WHERE " + " AND ".join(f"emp.{key}='{value}'" for key, value in filters.items())
    query += " RETURN ID(emp) as id, emp, dept"

    if sort_by:
        query += f" ORDER BY emp.{sort_by}"

    results = tx.run(query).data()
    employees = [{'id': result['id'], 'name': result['emp']['name'], 'position': result['emp']['position'],
                  'department': result['dept']['name']} for result in results]
    return employees

@app.route('/employees', methods=['GET'])
def get_employees_route():
    filters = request.args.to_dict()
    sort_by = request.args.get('sort_by', default=None)
    with driver.session() as session:
        employees = session.read_transaction(get_employees, filters=filters, sort_by=sort_by)

    response = {'employees': employees}
    return jsonify(response)

def add_employee(tx, name, position, department):
    query = "MATCH (d:Department {name: $department}) " \
            "CREATE (emp:Employee {name: $name, position: $position})-[:WORKS_IN]->(d) " \
            "RETURN ID(emp) as emp_id"
    result = tx.run(query, name=name, position=position, department=department)
    emp_id = result.single()['emp_id']
    return emp_id

@app.route('/employees', methods=['POST'])
def add_employee_route():
    data = request.json
    required_fields = ['name', 'position', 'department']

    if not all(field in data for field in required_fields):
        response = {'message': 'Missing required fields'}
        return jsonify(response), 400

    name = data['name']
    position = data['position']
    department = data['department']

    with driver.session() as session:
        emp_id = session.write_transaction(add_employee, name, position, department)

    response = {'status': 'success', 'id': emp_id}
    return jsonify(response)

def update_employee(tx, emp_id, name, position, department):
    query = "MATCH (emp:Employee)-[oldRel:WORKS_IN]->(:Department) WHERE ID(emp)=$emp_id " \
            "DELETE oldRel " \
            "WITH emp " \
            "SET emp.name=$name, emp.position=$position " \
            "WITH emp " \
            "MATCH (newDept:Department {name: $department}) " \
            "MERGE (emp)-[:WORKS_IN]->(newDept)"
    tx.run(query, emp_id=emp_id, name=name, position=position, department=department)



@app.route('/employees/<int:emp_id>', methods=['PUT'])
def update_employee_route(emp_id):
    data = request.json
    required_fields = ['name', 'position', 'department']

    if not all(field in data for field in required_fields):
        response = {'message': 'Missing required fields'}
        return jsonify(response), 400

    name = data['name']
    position = data['position']
    department = data['department']

    with driver.session() as session:
        session.write_transaction(update_employee, emp_id, name, position, department)

    response = {'status': 'success'}
    return jsonify(response)


def delete_employee(tx, emp_id):
    query = "MATCH (emp:Employee)-[r]-() WHERE ID(emp)=$emp_id DELETE emp, r"
    tx.run(query, emp_id=emp_id)

@app.route('/employees/<int:emp_id>', methods=['DELETE'])
def delete_employee_route(emp_id):
    with driver.session() as session:
        session.write_transaction(delete_employee, emp_id)

    response = {'status': 'success'}
    return jsonify(response)

def get_subordinates(tx, emp_id):
    query = "MATCH (manager:Employee)-[:MANAGES]->(subordinate:Employee) WHERE ID(manager)=$emp_id RETURN ID(subordinate) as id, subordinate"
    results = tx.run(query, emp_id=emp_id).data()
    subordinates = [{'id': result['id'], 'name': result['subordinate']['name'], 'position': result['subordinate']['position']} for result in results]
    return subordinates

@app.route('/employees/<int:emp_id>/subordinates', methods=['GET'])
def get_subordinates_route(emp_id):
    with driver.session() as session:
        subordinates = session.read_transaction(get_subordinates, emp_id)

    response = {'subordinates': subordinates}
    return jsonify(response)

def get_department_info(tx, emp_id):
    query = "MATCH (emp:Employee)-[:WORKS_IN]->(dept:Department) WHERE ID(emp)=$emp_id RETURN dept"
    result = tx.run(query, emp_id=emp_id).single()
    department_info = {'name': result['dept']['name']}

    query_extra = "MATCH (dept:Department)<-[:WORKS_IN]-(employee:Employee) " \
                  "WHERE ID(dept)=$dept_id RETURN COUNT(employee) as num_employees, " \
                  "head(collect(employee.name)) as manager"
    result_extra = tx.run(query_extra, dept_id=result['dept'].id).single()
    department_info['num_employees'] = result_extra['num_employees']
    department_info['manager'] = result_extra['manager']

    return department_info

@app.route('/employees/<int:emp_id>/department', methods=['GET'])
def get_department_info_route(emp_id):
    with driver.session() as session:
        department_info = session.read_transaction(get_department_info, emp_id)

    response = {'department_info': department_info}
    return jsonify(response)


def get_departments(tx, filters=None, sort_by=None):
    query = "MATCH (dept:Department) "\
        "OPTIONAL MATCH (dept)<-[:WORKS_IN]-(:Employee) "\
        "RETURN dept, COUNT(*) as num_employees"
    
    if filters:
        query += " WHERE " + " AND ".join(f"dept.{key}='{value}'" for key, value in filters.items())
    
    if sort_by:
        query += f" ORDER BY dept.{sort_by}"

    results = tx.run(query).data()
    departments = [
        {
            'name': result['dept']['name'],
            'num_employees': result['num_employees']
        }
        for result in results
    ]
    return departments

@app.route('/departments', methods=['GET'])
def get_departments_route():
    filters = request.args.to_dict()
    sort_by = request.args.get('sort_by', default=None)
    with driver.session() as session:
        departments = session.read_transaction(get_departments, filters=filters, sort_by=sort_by)

    response = {'departments': departments}
    return jsonify(response)

def get_department_employees(tx, dept_id):
    query = "MATCH (dept:Department)<-[:WORKS_IN]-(employee:Employee) "\
        "WHERE ID(dept)=$dept_id "\
        "RETURN employee, ID(employee) as employee_id"

    results = tx.run(query, dept_id=dept_id).data()
    department_employees = [{'id': result['employee_id'], 'name': result['employee']['name'], 'position': result['employee']['position']} for result in results]
    return department_employees

@app.route('/departments/<int:dept_id>/employees', methods=['GET'])
def get_department_employees_route(dept_id):
    with driver.session() as session:
        department_employees = session.read_transaction(get_department_employees, dept_id)

    response = {'department_employees': department_employees}
    return jsonify(response)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
