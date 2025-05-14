--Task two

CREATE TABLE IF NOT EXISTS departments(
	department_id INT primary key,
	department_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS employees(
	employee_id INT primary key,
	employee_name VARCHAR(100) NOT NULL,
	salary NUMERIC,
	department_id INT,
	hire_date DATE,
	FOREIGN KEY (department_id) REFERENCES departments(department_id) 
);

TRUNCATE employees;

-- Task three

INSERT INTO departments (department_id,department_name) VALUES
(1,'IT'),
(2,'HR'),
(3,'Finance'),
(4,'Sales'),
(5,'Marketing'),
(6,'Operations');

INSERT INTO employees (employee_id,employee_name,salary,department_id,hire_date) VALUES
(1, 'Alice',70000,1,'2021-06-15'),
(2, 'Bob',50000,2,'2020-07-22'),
(3, 'Charlie',80000,1,'2019-03-10'),
(4, 'David',NULL,3,'2022-01-05'),
(5, 'Eve',45000,4,'2018-09-12'),
(6, 'Frank',60000,2,'2017-11-30'),
(7, 'Grace',75000,3,'2020-05-14'),
(8, 'Hannah',90000,5,'2023-08-19'),
(9, 'Isaac',72000,6,'2021-04-23'),
(10, 'Jack',85000,1,'2019-07-07')

--task four
--1)
SELECT * FROM employees;
--2)
SELECT employee_name,salary FROM employees;
--3)
SELECT * FROM employees WHERE hire_date > '2020-01-01';

--task five
--1)
SELECT * FROM employees WHERE salary > 50000;
--2)
SELECT * FROM employees WHERE department_id !=2;
--3)
SELECT * FROM employees WHERE hire_date IN  RANGE('2019', '2021');

--task six
--1) 
SELECT * FROM employees ORDER BY salary DESC;
--2)
SELECT * FROM employees ORDER BY hire_date ASC;
--3)
SELECT * FROM employees ORDER BY department_id,salary DESC;

--task 7 (JOINS)
--1)
SELECT employees.employee_name,departments.department_name
FROM employees
INNER JOIN departments
ON employees.department_id=departments.department_id;
--2)

SELECT employees.employee_name
FROM employees
INNER JOIN departments
ON employees.department_id=departments.department_id
WHERE departments.department_name='IT';

--3)
SELECT departments.department_name
FROM departments
LEFT JOIN employees
ON departments.department_id=employees.department_id;

--Task 8 (Group by)
--1)
SELECT departments.department_name, COUNT(*) as employees_in_each_department
FROM employees
FULL JOIN departments
ON employees.department_id=departments.department_id
GROUP BY departments.department_name

--2)
SELECT departments.department_name, AVG(salary) as average_salary_per_department
FROM employees
FULL JOIN departments
ON employees.department_id=departments.department_id
GROUP BY departments.department_name

--3)
SELECT departments.department_name, MAX(salary) as highest_salary_per_department
FROM employees
FULL JOIN departments
ON employees.department_id=departments.department_id
GROUP BY departments.department_name

--task 9 (Window Functions)
--1)

SELECT coalesce (salary,0), (employee_name , salary),
RANK() OVER (ORDER BY salary DESC) AS salary_rank
FROM employees;
--2)

--3)

--task 10 (Distinct)
--1)
SELECT DISTINCT(department_name) FROM departments;
--2)
SELECT DISTINCT(salary) FROM employees;
--3)
SELECT DISTINCT ON(employees.department_id )department_name,employee_name,salary, coalesce (salary,0)
FROM employees
FULL JOIN departments
ON employees.department_id=departments.department_id
ORDER BY employees.department_id, employee_name, salary DESC;