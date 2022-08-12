from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.functions import *
from datetime import datetime

if __name__ == '__main__':
    spark =SparkSession.builder.master("local[*]").appName("hremployee").getOrCreate()
    #print(spark)

    emp=spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\employee.csv",header=True,inferSchema=True)
    #emp.show()

    dept=spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\hrdepartments.csv",header=True,inferSchema=True)
    #dept.show()

    countries=spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\hrcountries.csv",header=True,inferSchema=True)
    #countries.show()

    job=spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\hrjobs.csv",header=True,inferSchema=True)
    #job.show()

    jhistory=spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\hrjobhistory.csv",header=True,inferSchema=True)
    #jhistory.show()

    location=spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\hrlocation.csv",header=True,inferSchema=True)
    #location.show()

    regions=spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\hrregions.csv",header=True,inferSchema=True)
    #regions.show()


    emp.createOrReplaceTempView("employee")
    dept.createOrReplaceTempView("hrdepartments")
    countries.createOrReplaceTempView("hrcountries")
    job.createOrReplaceTempView("hrjobs")
    jhistory.createOrReplaceTempView("hrjobhistory")
    location.createOrReplaceTempView("hrlocation")
    regions.createOrReplaceTempView("hrregions")

#1. Display details of jobs where the minimum salary is greater than 10000.

#dataframe
    #job.filter(job.MIN_SALARY > 10000).show()

#sqlspark
    #spark.sql("select * from hrjobs where MIN_SALARY > 10000").show()

#RDD
    jobsrdd =job.rdd
    #print(jobsrdd.collect())
    rdd1 = jobsrdd.filter(lambda x: int(x.MIN_SALARY) > 10000)
    #print(rdd1.collect())

#2. Display the first name and join date of the employees who joined between 2002 and 2005.

#dataframe

#sqlspark
    #print(emp.printSchema())
    # spark.sql("select FIRST_NAME,HIRE_DATE from employee where date_format(HIRE_DATE,'YYYY') between 2002 and 2005 order by HIRE_DATE").show()

    #spark.sql("select first_name,hire_date from employees where to_date('hire_date','yyyy') between 2002 and 2005 order by hire_date").show()

#3. Display first name and join date of the employees who is either IT Programmer or Sales Man.

#SQL
    #spark.sql("select FIRST_NAME,HIRE_DATE ,JOB_ID from employee where JOB_ID in ('IT_PROG' ,'Sa_Man')").show()
#DF

    #emp.select(emp.FIRST_NAME,emp.HIRE_DATE,emp.JOB_ID).filter((emp.JOB_ID =='IT_PROG' ) | (emp.JOB_ID=='Sa_Man')).show()

#4. Display employees who joined after 1st January 2008.

#SQL
    #spark.sql("select * from employee where HIRE_DATE >'1/1/2008' ").show()
#DF
    #emp.filter(emp.HIRE_DATE >'1-Jan-2008').show()

#5. Display details of employee with ID 150 or 160.

#SQL
    #spark.sql("select * from employee where EMPLOYEE_ID in (150,160)").show()

#DF
    #emp.filter((emp.EMPLOYEE_ID==150) |(emp.EMPLOYEE_ID==160)).show()

#6. Display first name, salary, commission pct, and hire date for employees with salary less than 10000.

#SQL
    #spark.sql("select FIRST_NAME,SALARY,COMMISSION_PCT,HIRE_DATE FROM employee where SALARY <10000").show()

#df
    #emp.select('FIRST_NAME','SALARY','COMMISSION_PCT','HIRE_DATE').filter(emp.SALARY <10000).show()

#7. Display job Title, the difference between minimum and maximum salaries for jobs with max salary in the range 10000 to 20000.

#SQL
    #spark.sql("select JOB_TITLE,MIN_SALARY-MAX_SALARY DIFFERENCE ,MAX_SALARY from hrjobs where MAX_SALARY  between 10000 and 20000").show()

#DF
    a=job.select('JOB_TITLE','MAX_SALARY','MIN_SALARY').filter(job.MAX_SALARY.between(10000,20000) )

    #a.withColumn('Difference', job.MAX_SALARY - job.MIN_SALARY).show()

#8. Display first name, salary, and round the salary to thousands.

#SQL
    #spark.sql("select FIRST_NAME,SALARY ,round(SALARY,-3) from employee").show()
#DF
    #emp.select('FIRST_NAME','SALARY').withColumn('NEW_SALARY',round(emp.SALARY,-3)).show()

#9. Display details of jobs in the descending order of the title.

#SQL
    #spark.sql("select * from hrjobs order by JOB_TITLE DESC").show()

#DF
    #job.select('*').orderBy(job.JOB_TITLE.desc()).show()

#10. Display employees where the first name or last name starts with S.
#SQL
    #spark.sql("select * from employee where FIRST_NAME like 'S%' or LAST_NAME like 'S%'").show()
#DF
    #emp.select('*').filter((emp.FIRST_NAME.like ('S%')) | (emp.LAST_NAME.like ('S%'))).show()

#11. Display employees who joined in the month of May.
#SQL
    #spark.sql("select * from employee where to_char(hire_date,'mon')='may' ").show()
#df

#12. Display details of the employees where commission percentage is null and salary in the range 5000 to 10000 and department is 30.

#SQL
    #spark.sql("select * from employee where COMMISSION_PCT is null").show()
#DF
    #b=emp.select('*').filter(isnull(emp.COMMISSION_PCT)).filter(emp.DEPARTMENT_ID==30).show()
    #b.filter(emp.SALARY.between(5000,10000)).show()

#13. Display first name and date of first salary of the employees.
#SQL
    #spark.sql("select first_name,last_name,hire_date, last_day(hire_date)first_salary from employee").show()
#DF
    #emp.select('FIRST_NAME','LAST_NAME','HIRE_DATE')

#14. Display first name and experience of the employees.
#sql
    #spark.sql("select FIRST_NAME,floor(to_date(SYSDATE-HIRE_DATE/365))A FROM EMPLOYEE").show()

#16. Display first name and last name after converting the first letter of each name to upper case and the rest to lower case.
#SQL
    #spark.sql("select initcap(FIRST_NAME)first_name,initcap(LAST_NAME)last_name from employee").show()
#DF
    C=emp.select('FIRST_NAME','LAST_NAME').withColumn('NEW_FIRST_NAME',initcap(emp.FIRST_NAME) )
    #C.withColumn('NEW_LAST_NAME',initcap(emp.LAST_NAME)).show()

#17. Display the first word in job title.

#SQL
    # spark.sql("select JOB_TITLE ,substr(job_title,1,instr(job_title,' ')-1) from hrjobs").show()
#df
    #job.select(job.JOB_TITLE,substring_index(job.JOB_TITLE,' ',1).alias("First")).show()

#18. Display the length of first name for employees where last name contain character ‘b’ after 3rd position.
#SQL
    #spark.sql("select FIRST_NAME,length(FIRST_NAME) ,LAST_NAME from employee where instr(LAST_NAME,'b')>3").show()
#df
    #emp.select(emp.FIRST_NAME,length(emp.FIRST_NAME)).show()

#23. Display manager ID and number of employees managed by the manager.
#SQL
    #spark.sql("select MANAGER_ID ,count(*) from employee group by MANAGER_ID ").show()
#DF
    d=emp.select('MANAGER_ID').groupBy(emp.MANAGER_ID).count()
    #d.show()

#24. Display employee ID and the date on which he ended his previous job.
#SQL
    #spark.sql("select employee_id,max(END_DATE) from hrjobhistory group by employee_id").show()
#DF
    #ee=jhistory.select('EMPLOYEE_ID').groupBy(col("EMPLOYEE_ID")).max(jhistory.END_DATE)
    #ee.show()

    e=jhistory.groupBy(col("EMPLOYEE_ID")).agg({"END_DATE": "max"})
    #e.show()

#26. Display the country ID and number of cities we have in the country.
#sql
    #spark.sql("select COUNTRY_ID ,count(*) from hrlocation group by COUNTRY_ID ").show()
#DF
    #location.select('COUNTRY_ID').groupBy(location.COUNTRY_ID).count().show()

#27. Display average salary of employees in each department who have commission percentage.
#sql
    #spark.sql("select DEPARTMENT_ID,avg(salary) from employee where commission_pct is not null group by DEPARTMENT_ID").show()
#df
    #emp.groupBy(col("DEPARTMENT_ID")).agg({"SALARY":"avg"}).show()
    #emp.filter(col("COMMISSION_PCT").isNotNull()).groupBy(col("DEPARTMENT_ID")).agg({"SALARY" :"avg"}).show()

#28. Display job ID, number of employees, sum of salary, and difference between highest salary and lowest salary of the employees of the job.
#SQL
    #spark.sql("select JOB_ID,count(*),sum(SALARY),max(SALARY)-min(SALARY) from employee group by JOB_ID").show()
#DF
    #f=emp.select("JOB_ID","SALARY").groupBy(col("JOB_ID")).agg({"SALARY": "sum","JOB_ID":"count"}).show()
    f2=emp.groupBy(col("JOB_ID")).agg({"SALARY" : "max"})
    f3=emp.groupBy(col("JOB_ID")).agg({"SALARY" : "min"})
    #f4=f2.join(f3,f2.JOB_ID == f3.JOB_ID,'full').select(col('max(SALARY)') - col('min(SALARY)')).show()
    # f5=f4.join(f,f4.)

    #f = emp.select("JOB_ID", "SALARY").groupBy(col("JOB_ID"))


    #emp.withColumn('Difference', (max(emp.SALARY) - min(emp.SALARY))).show()

    #emp.join(job,emp.EMPLOYEE_ID==job.JOB_ID,'full').show()

    #job.withColumn('Difference', job.MAX_SALARY - job.MIN_SALARY).show()

    #emp.select("JOB_ID","SALARY","EMPLOYEE_ID").groupBy(emp.JOB_ID).sum('SALARY')

   # emp.select('*').show()

#29. Display job ID for jobs with average salary more than 10000.
#SQL
    #spark.sql("select JOB_ID,avg(salary) from employee group by JOB_ID having avg(salary)").show()

#DF
    #emp.select("JOB_ID","SALARY").groupBy(col("JOB_ID")).agg({"SALARY": "avg"}).show()

#31. Display departments in which more than five employees have commission percentage.
#SQL
    #spark.sql("select DEPARTMENT_ID from employee where COMMISSION_PCT is not null group by DEPARTMENT_ID having count(COMMISSION_PCT)>5").show()
#DF
    #emp.select("DEPARTMENT_ID","COMMISSION_PCT").filter(col("COMMISSION_PCT").isNotNull()).filter(col("COMMISSION_PCT")>1).show()

#32. Display employee ID for employees who did more than one job in the past.
#SQL
    #spark.sql("select EMPLOYEE_ID from hrjobhistory group by EMPLOYEE_ID having count(EMPLOYEE_ID)>1").show()
#DF
    #jhistory.select("EMPLOYEE_ID").groupBy(col("EMPLOYEE_ID")).agg({"EMPLOYEE_ID": "count"}).filter(count("EMPLOYEE_ID")>1).show()

#33. Display job ID of jobs that were done by more than 3 employees for more than 100 days.
#SQL

    #spark.sql("select JOB_ID from hrjobhistory").show()

    #jhistory.select("*").show()

#35. Display departments where any manager is managing more than 5 employees.
#SQL
    #spark.sql("select MANAGER_ID,count(EMPLOYEE_ID),DEPARTMENT_ID from employee group by DEPARTMENT_ID,MANAGER_ID having count(EMPLOYEE_ID)>5").show()
#DF
    #emp.select('DEPARTMENT_ID','EMPLOYEE_ID','MANAGER_ID').groupBy(col("DEPARTMENT_ID"),col("MANAGER_ID")).agg({"EMPLOYEE_ID":"count"}).filter(count("EMPLOYEE_ID")>5).distinct().show()

#36. Change salary of employee 115 to 8000 if the existing salary is less than 6000.
#SQL
    #spark.sql("update employee set salary=8000 where employee_id=115 and salary<6000").show()
#df

#41. Display department name and number of employees in the department.
#SQL
    #spark.sql("select department_name, count(*) from employee e inner join hrdepartments d on e.department_id=d.department_id group by department_name order by count(*) ").show()
#DF
    #emp.join(dept,emp.DEPARTMENT_ID==dept.DEPARTMENT_ID,'inner').select("DEPARTMENT_NAME").groupBy(col('DEPARTMENT_NAME')).count().orderBy(col('count')).show()

#REPLCE NAME
    #a1 = emp.na.replace(['Neena'],['Swati'],'FIRST_NAME').show()

# CONCAT TWO COLUMNS WITH SEPARATER

    emp.withColumn("new_name",concat_ws("_",col("FIRST_NAME"),col("LAST_NAME"))).show()




