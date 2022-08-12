from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import *
from pyspark.sql.functions import *
from datetime import datetime
from pyspark.sql import Window


if __name__ == '__main__':
    spark =SparkSession.builder.master("local[*]").appName("hremployee").getOrCreate()
    #print(spark)

    customer_tab= spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\GarageTableCSVfiles\customer_tab.csv", header=True, inferSchema=True)
    # customer_tab.show()
    # customer_tab.printSchema()

    employee_tab= spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\GarageTableCSVfiles\employee_tab.csv", header=True, inferSchema=True)
    # employee_tab.show()
    # employee_tab.printSchema()

    purchase_tab= spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\GarageTableCSVfiles\purchase_tab.csv", header=True, inferSchema=True)
    # purchase_tab.show()
    # purchase_tab.printSchema()

    vendors_tab= spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\GarageTableCSVfiles\vendors_tab.csv", header=True, inferSchema=True)
    # vendors_tab.show()
    # vendors_tab.printSchema()

    sparepart= spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\GarageTableCSVfiles\sparepart.csv", header=True, inferSchema=True)
    # sparepart.show()
    # sparepart.printSchema()

    ser_det= spark.read.csv(r"C:\Users\Shree\Documents\csv file emp\GarageTableCSVfiles\ser_det.csv", header=True, inferSchema=True)
    # ser_det.show()
    # ser_det.printSchema()

    customer_tab.createOrReplaceTempView("customer_tab")
    employee_tab.createOrReplaceTempView("employee_tab")
    vendors_tab.createOrReplaceTempView("vendors_tab")
    purchase_tab.createOrReplaceTempView("purchase_tab")
    sparepart.createOrReplaceTempView("sparepart")
    ser_det.createOrReplaceTempView("ser_det")

#QUESTIONS

#--Q.1 List all the customers serviced.
#SQL
    #spark.sql("select c.cid,c.cname,sd.ser_date,sd.typ_ser from customer_tab c join ser_det sd on c.cid=sd.cid").show()
#DF
    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(customer_tab.CID,customer_tab.CNAME,ser_det.TYP_SER,ser_det.SER_DATE).show()

#--Q.2 Customers who are not serviced.
#SQL
    #spark.sql("select * from customer_tab c where c.cid not in (select sd.cid from ser_det sd)").show()
#DF
    # customer_tab.select('*').filter(customer_tab.CID,is)

    a= customer_tab.select(customer_tab.CID).subtract(ser_det.select(ser_det.CID))
    customer_tab.join(a,a.CID==customer_tab.CID).show()

    #customer_tab.join(ser_det,ser_det.CID==customer_tab.CID,'leftanti').show()

#---Q.3 Employees who have not received the commission.

    #employee_tab.join(ser_det,employee_tab.EID==ser_det.EID,'leftanti').show()

    #employee_tab.join(ser_det, employee_tab.EID == ser_det.EID, 'inner').select(ser_det.COMM,employee_tab.EID,employee_tab.ENAME).filter(ser_det.COMM==0).show()

#--Q.4 Name the employee who have maximum Commission.

    #a=employee_tab.join(ser_det,employee_tab.EID == ser_det.EID, 'inner').select(ser_det.COMM,employee_tab.EID,employee_tab.ENAME).agg({"COMM": "max"})

    #b=a.join(ser_det, a['max(COMM)'] == ser_det.COMM, 'inner').select(ser_det.COMM,ser_det.EID)

    #c=b.join(employee_tab,employee_tab['EID']==b['EID']).select(employee_tab.EID,employee_tab.ENAME,b['*']).show()


#---Q.5 Show employee name and minimum commission amount received by an employee.

    #employee_tab.join(ser_det, employee_tab.EID == ser_det.EID, 'inner').select(ser_det.COMM, employee_tab.EID, employee_tab.ENAME).groupBy(
    # employee_tab.EID, employee_tab.ENAME).agg({"COMM": "min"}).show()

    #employee_tab.join(ser_det, employee_tab.EID == ser_det.EID, 'inner').agg({"COMM": "min"}).show()

#---Q.6 Display the Middle record from any table.
    # win = Window.orderBy(col('EID'))
    # a=employee_tab.withColumn('rn',row_number().over(win))
    # b=a.filter('rn'==round(count(col('rn'))/2))
    # #b.show()

#----Q.7 Display last 4 records of any table.

    # c=employee_tab.tail(4)
    # d=spark.createDataFrame(c)
    # #d.show()
    # employee_tab.orderBy(col('EID').desc()).show(4)
    #
    # print(c)
    # print(customer_tab.tail(4))

#---Q.8 Count the number of records without count function from any table.

    # win = Window.orderBy(col('EID'))
    # a=employee_tab.withColumn('rn',row_number().over(win)).show()
    # print(employee_tab.count())
    # win1 = Window.orderBy(col('CID'))
    # customer_tab.withColumn('rn',row_number().over(win1)).show()

#--Q.9 Delete duplicate records from "Ser_det" table on cid.(note Please rollback after execution).

    # ser_det.select('*').distinct().show()
    # ser_det.select('CID').distinct().show()
    # ser_det.dropDuplicates(['CID']).show()
    # ser_det.drop_duplicates(['CID']).show()

#--Q.10 Show the name of Customer who have paid maximum amount

    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').agg({"SER_AMT":"max"}).show()

    #e=customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').agg({'SER_AMT':'max'},customer_tab.CID).show()
    #e.join(ser_det,e.max(SER_AMT)==ser_det.SER_AMT,'inner').select(ser_det.CID,ser_det.SER_AMT).show()

#---Q.11 Display Employees who are not currently working.

    #employee_tab.join(ser_det,employee_tab.EID==ser_det.EID,'leftanti').show()

#---Q.12 How many customers serviced their two wheelers.

    # ser_det.select("CID","TYP_VEH").filter(ser_det.TYP_VEH=="TWO WHEELER").show()
    # ser_det.select("CID","TYP_VEH").filter(col('TYP_VEH').like('%TWO%')).show()
    # ser_det.select("CID", "TYP_VEH").filter(ser_det.TYP_VEH.like('%TWO%')).show()

    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(customer_tab.CID,customer_tab.CNAME,ser_det.TYP_VEH).groupBy(customer_tab.CNAME,ser_det.TYP_VEH).agg({'CID':"count"}).filter(col('TYP_VEH').like('%TWO%')).show()

#--Q.13 List the Purchased Items which are used for Customer Service with Unit of that Item.

    #purchase_tab.join(ser_det,purchase_tab.SPID==ser_det.SPID,'inner').select(ser_det.CID,ser_det.QTY,ser_det.TYP_SER).groupBy(ser_det.CID,ser_det.QTY,ser_det.TYP_SER).agg({'CID':"count"}).show()

#--Q.14 Customers who have Colored their vehicles.

    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(customer_tab.CID,customer_tab.CNAME,ser_det.TYP_SER).filter(ser_det.TYP_SER.like("%COLOR%")).show()

#--Q.15 Find the annual income of each employee inclusive of Commission

#--Q.16 Vendor Names who provides the engine oil.

    #a=vendors_tab.join(purchase_tab,vendors_tab.VID==purchase_tab.VID,'inner').select(vendors_tab.VNAME,purchase_tab.VID,purchase_tab.SPID)

    #b = a.join(sparepart, a.SPID == sparepart.SPID, 'inner').select(a.VNAME,a.SPID,sparepart.SPNAME).filter(sparepart.SPNAME.like("%ENGINE OIL%")).show()

    # b=purchase_tab.join(sparepart,purchase_tab.SPID==sparepart.SPID,'inner').select(sparepart.SPID,sparepart.SPNAME).filter(sparepart.SPNAME.like("%ENGINE OIL%")).show()

#--Q.18 Purchased Items which are not used in "Ser_det".

    #purchase_tab.join(ser_det,purchase_tab.SPID==ser_det.SPID,'leftanti').show()

#--Q.19 Spare Parts Not Purchased but existing in Sparepart

    #sparepart.join(purchase_tab,sparepart.SPID==purchase_tab.SPID,'leftanti').show()

#--Q.20 Calculate the Profit/Loss of the Firm. consider one month salary of each employee for Calculation.

#--Q.21 Specify the names of customers who have serviced their vehicles more than one time.

    # customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'full').select(customer_tab.CNAME,ser_det.TYP_SER,customer_tab.CID).agg({'CID':"count"}).filter(count(CID)>2).show()
    #df1= customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'full').select(ser_det.TYP_SER,ser_det.CID).groupBy(ser_det.CID,ser_det.TYP_SER).count()
    #df1.filter(col('count')>1).show()

#--Q.22 List the Items purchased from vendors locationwise.

    #a=purchase_tab.join(vendors_tab,vendors_tab.VID==purchase_tab.VID,'full').select(vendors_tab.VADD,purchase_tab.SPID)

    #b=a.join(sparepart,a.SPID==sparepart.SPID).select(a.VADD,sparepart.SPNAME).groupBy(a.VADD,sparepart.SPNAME).count().show()

#--Q.23 Display count of two wheeler and four wheeler from ser_details.

    #ser_det.select('TYP_VEH').groupBy(col('TYP_VEH')).count().show()

#--Q24 Display name of customers who paid highest SPGST and for which item.

    #a=customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'full').select(customer_tab.CNAME,ser_det.TYP_SER,ser_det.SP_G,ser_det.CID).agg({"SP_G":'max'})

    #b=a.join(ser_det,a['max(SP_G)']==ser_det.SP_G).select(ser_det.TYP_SER,ser_det.SP_G,ser_det.CID)
    #b.join(customer_tab,customer_tab['CID']==b['CID']).select(customer_tab.CNAME,b['*']).show()

#--Q25 Display vendors name who have charged highest SPGST rate  for which item.

    #a=vendors_tab.join(purchase_tab,vendors_tab.VID==purchase_tab.VID,'inner').agg({'SPGST':'max'})
    #b = a.join(purchase_tab, a['max(SPGST)'] == purchase_tab.SPGST).select(purchase_tab.VID,purchase_tab.SPID,purchase_tab.SPGST)
    #c=b.join(vendors_tab,vendors_tab.VID==b['VID']).select(vendors_tab.VNAME,b['*'])
    #d=c.join(sparepart,sparepart.SPID==c['SPID']).select(sparepart.SPNAME,c['*']).show()

#---Q26   list name of item and employee name who have received item .

    #a=employee_tab.join(ser_det,employee_tab.EID == ser_det.EID,'inner').select(employee_tab.ENAME,ser_det.SPID).groupBy(employee_tab.ENAME,ser_det.SPID).agg({'ENAME':'count'})
    #b=a.join(sparepart,a.SPID==sparepart.SPID,'inner').select(sparepart.SPNAME,a['*']).show()

#--Q27 Display the Name and Vehicle Number of Customer who serviced his vehicle,
#-- And Name the Item used for Service,
#--And specify the purchase date of that Item with his vendor and Item Unit and Location,
#--And employee Name who serviced the vehicle. for Vehicle NUMBER "MH-14PA335".

    #spark.sql("select c.cname,sd.veh_no,sd.typ_ser,sp.spname,p.pdate,v.vname,sp.spunit,v.vadd,e.ename from customer_tab c join ser_det sd on c.cid=sd.cid join purchase_tab p on sd.spid=p.spid join vendors_tab v on v.vid=p.vid join sparepart sp on sp.spid=sd.spid join employee_tab e on e.eid=sd.eid where veh_no like ('%MH-14PA335%')").show()

#--Q28 who belong this vehicle  MH-14PA335" Display the customer name.

    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(customer_tab.CNAME,ser_det.VEH_NO).filter(ser_det.VEH_NO.like("%MH-14PA335%")).show()

#--Q29 Display the name of customer who belongs to New York and when he /she service their vehicle on which date .

    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(customer_tab.CNAME,customer_tab.CADD,ser_det.SER_DATE).filter(customer_tab.CADD.like("%Newyork%")).show()

#--Q 30 from whom we have purchased items having maximum cost?

    #a=vendors_tab.join(purchase_tab,vendors_tab.VID==purchase_tab.VID,'inner').select(vendors_tab.VNAME,purchase_tab.TOTAL,vendors_tab.VID).agg({"TOTAL":'max'})
    #b=a.join(purchase_tab,purchase_tab.TOTAL==a['max(TOTAL)']).select(a["*"],purchase_tab.VID)
    #c=b.join(vendors_tab,b['VID']==vendors_tab['VID']).select(vendors_tab.VNAME,b['*']).show()

#--Q31 Display the names of employees who are not working as Mechanic and that employee done services.

    #employee_tab.join(ser_det,employee_tab.EID==ser_det.EID).select(employee_tab.EID,employee_tab.ENAME,employee_tab.EJOB,ser_det.TYP_SER).filter(employee_tab.EJOB.notlike("%Mechanic%")).show()

#--Q32 Display the various jobs along with total number of employees in each job. The output should contain only those jobs with more than two employees.

    #employee_tab.select('EJOB').groupBy(employee_tab.EJOB).agg({'EJOB':"count"}).filter(count('EJOB')>2).show()

#--Q33 Display the details of employees who done service and give them rank according to their no. of services.

    #spark.sql("select  a.ename ,rk, decode (rk,0,'low',1,'good',2,'better',4,'best') ranking  from (select e.ename,count(sd.eid) rk from employee_tab e left join ser_det sd on e.eid=sd.eid group by e.ename order by count(sd.eid)) a order by rk desc").show()

    #spark.sql("select a.ename,rk,case when rk=0 then 'low' when rk=1 then 'good' when rk=2 then 'better' when rk=4 then 'best' end ranking from (select e.ename,count(sd.eid) rk from employee_tab e left join ser_det sd on e.eid=sd.eid group by e.ename order by count(sd.eid)) a order by rk desc").show()

    #a=employee_tab.join(ser_det,employee_tab.EID==ser_det.EID).select(employee_tab.ENAME,ser_det.EID).groupBy(ser_det.EID,employee_tab.ENAME).agg({"EID":'count'})

    #a = ser_det.withColumn("rank", rank().over(Window.orderBy('TYP_SER'))).show()
    #b = employee_tab.join(ser_det, employee_tab.EID == ser_det.EID).select(ser_det.EID, employee_tab.ENAME, employee_tab.EJOB, ser_det.TYP_SER) .withColumn("rank", dense_rank().over(Window.orderBy('TYP_SER')))
    #b.show()

#--Q 34 Display those employees who are working as Painter and fitter and who provide service and total count of service done by fitter and painter.

    #a=employee_tab.join(ser_det,employee_tab.EID==ser_det.EID,'inner').select(employee_tab.ENAME,employee_tab.EJOB,ser_det.TYP_SER).groupBy(employee_tab.EJOB,ser_det.TYP_SER,employee_tab.ENAME).agg({"TYP_SER":"count"}).filter(col('EJOB').like("%Fitter%")|col('EJOB').like("%painter%")).show()
    #b=a.filter(col('EJOB').like("%painter%")).show()

#--Q35 Display employee salary and as per highest  salary provide Grade to employee.

    #employee_tab.select("ENAME",employee_tab.ESAL,when(employee_tab.ESAL>2000, 'A').when(employee_tab.ESAL>1000,'B').when(employee_tab.ESAL<=1000,'C').otherwise('D').alias('Grade')).orderBy(desc(employee_tab.ESAL)).show()

#---Q36  display the 4th record of emp table without using group by and rowid.

    #spark.sql("select rownum,e.* from employee_tab e where rownum<=4 minus select rownum,e.* from employee_tab e where rownum<=3").show()

    #a = employee_tab.withColumn("rn", row_number().over(Window.orderBy('EID')))
    #b=a.filter(col('rn')==4).show()

#---Q37 Provide a commission 100 to employees who are not earning any commission.

    #employee_tab.join(ser_det,ser_det.EID==employee_tab.EID,'inner').select(employee_tab.EID,employee_tab.ENAME,ser_det.COMM,when(ser_det.COMM==0,100).alias('NEW_COMM')).show()

#----Q38 write a query that totals no. of services  for each day and place the results in descending order.

    #a=ser_det.select('SID','TYP_SER','SER_DATE').groupBy('SID','TYP_SER','SER_DATE').agg({'TYP_SER':'count'}).orderBy(desc('SER_DATE')).show()

#--Q39 Display the service details of those customer who belong from same city.

    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(customer_tab.CNAME,customer_tab.CADD,ser_det.TYP_SER).groupBy(customer_tab.CADD,customer_tab.CNAME).agg({"CADD":"count"}).filter(col('count(CADD)')>1).show()

#---Q40 write a query join customers table to itself to find all pairs of customers service by a single employee. ---------ask----??

    #a=customer_tab.alias("t1").join(customer_tab.alias("t2") ,col("t1.CID")==col("t2.CID"),'inner').select('t1.CID','t2.CNAME').show()


    # empDF.alias("emp1").join(empDF.alias("emp2"), \
    #                          col("emp1.superior_emp_id") == col("emp2.emp_id"), "inner") \
    #     .select(col("emp1.emp_id"), col("emp1.name"), \
    #             col("emp2.emp_id").alias("superior_emp_id"), \
    #             col("emp2.name").alias("superior_emp_name")) \
    #     .show(truncate=False)

    #b=a.join(ser_det,ser_det.CID==a.CID,'inner').select(ser_det.TYP_SER).show()


#---Q41 List each service number follow by name of the customer who made  that service.

    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(customer_tab.CID,customer_tab.CNAME,ser_det.TYP_SER,ser_det.VEH_NO).show()

#--Q42 Write a query to get details of employee and provide rating on basis of maximum services provide by employee .Note (rating should be like A,B,C,D)

    #a=employee_tab.join(ser_det,employee_tab.EID==ser_det.EID,'inner').select(employee_tab.EID,employee_tab.ENAME,ser_det.TYP_SER).groupBy(ser_det.TYP_SER,employee_tab.EID,employee_tab.ENAME).agg({"TYP_SER":'count'})
    #b=a.withColumn("rating",when(a['count(TYP_SER)'] == 1,'d').when(a['count(TYP_SER)'] == 2,'c').when(a['count(TYP_SER)'] == 3,'b').when(a['count(TYP_SER)'] == 4,'a').otherwise(a['count(TYP_SER)']))
    #b.show()

#--Q43 Write a query to get maximum service amount of each customer with their customer details ?

    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(customer_tab.CID,customer_tab.CNAME,ser_det.SER_AMT).groupBy(customer_tab.CID,customer_tab.CNAME,ser_det.SER_AMT).agg({'SER_AMT':"max"}).orderBy(desc(col('SER_AMT'))).show()

#----Q44 Get the details of customers with his total no of services ?

    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(customer_tab.CID,customer_tab.CNAME,customer_tab.CADD,ser_det.TYP_SER).groupBy(customer_tab.CID,customer_tab.CNAME,customer_tab.CADD,ser_det.TYP_SER).agg({"TYP_SER":'count'}).show()

#--Q45 From which location sparpart purchased  with highest cost ?

    #vendors_tab.join(purchase_tab,vendors_tab.VID==purchase_tab.VID,'inner').select(vendors_tab.VADD,purchase_tab.SPRATE).groupBy(vendors_tab.VADD,purchase_tab.SPRATE).agg({"VADD":"count"}).orderBy(desc(purchase_tab.SPRATE)).show()

#--Q46 Get the details of employee with their service details who has salary is null.

    #employee_tab.join(ser_det,employee_tab.EID==ser_det.EID,'inner').select(employee_tab.EID,employee_tab.ENAME,employee_tab.ESAL,ser_det.TYP_SER).filter(col('ESAL').isNull()).show()

#--Q47 find the sum of purchase location wise.

    #vendors_tab.join(purchase_tab,vendors_tab.VID==purchase_tab.VID,'full').select(vendors_tab.VADD,purchase_tab.TOTAL).groupBy(vendors_tab.VADD,purchase_tab.TOTAL).agg({"TOTAL":"sum","VADD":"count"}).show()

    #vendors_tab.select("*").show()


#---Q48 write a query sum of purchase amount in word location wise ?   ----ask-------?????

#---Q49 Has the customer who has spent the largest amount money has been give highest rating.

    #a=customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(customer_tab.CNAME,ser_det.SP_AMT).groupBy(customer_tab.CNAME,ser_det.SP_AMT).agg({"SP_AMT":"max"})
    #b=a.withColumn("rating",when(a["max(SP_AMT)"]>=500,"Best").when(a['max(SP_AMT)']<500,'Good')).orderBy(desc(a['SP_AMT'])).show()

#---Q50 select the total amount in service for each customer for which the total is greater than the amount of the largest service amount in the table


#---Q51  List the customer name and sparepart name used for their vehicle and  vehicle type.

    #a=customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(customer_tab.CNAME,ser_det.TYP_VEH,ser_det.VEH_NO,ser_det.TYP_SER,ser_det.SPID)
    #b=a.join(sparepart,a['SPID']==sparepart.SPID,'inner').select(a['*'],sparepart.SPNAME).show()

#----Q52 Write a query to get spname ,ename,cname quantity ,rate ,service amount for record exist in service table.

    #a=customer_tab.join(ser_det,customer_tab.CID==ser_det.CID,'inner').select(ser_det.CID,ser_det.EID,ser_det.SPID,customer_tab.CNAME,ser_det.SER_AMT)
    #b=a.join(employee_tab,employee_tab.EID==a['EID']).select(a['*'],employee_tab.ENAME)
    #c=b.join(sparepart,sparepart.SPID==b['SPID']).select(sparepart.SPNAME,b['*']).show()

#--Q53 specify the vehicles owners who’s tube damaged.

    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID).select(customer_tab.CID,customer_tab.CNAME,ser_det.TYP_VEH,ser_det.VEH_NO,ser_det.TYP_SER).filter(col('TYP_SER').like("%TUBE%")).show()

#---Q.54 Specify the details who have taken full service.

    #customer_tab.join(ser_det,customer_tab.CID==ser_det.CID).select(customer_tab.CID,customer_tab.CNAME,ser_det.TYP_VEH,ser_det.VEH_NO,ser_det.TYP_SER).filter(col('TYP_SER').like("%FUL%")).show()

#---Q.55 Select the employees who have not worked yet and left the job.

    #employee_tab.select('EID','ENAME','EDOL','EDOJ').filter(col('EDOL').isNull()).show()

#--Q.56  Select employee who have worked first ever.       -----ask------?????

    #employee_tab.select('EID','ENAME','EDOL','EDOJ').orderBy(desc('EDOJ')).show()

#--Q.57 Display all records falling in odd date      -------ask------????

#---Q.58 Display all records falling in even date ------ask----????

#---Q.59 Display the vendors whose material is not yet used.


#---Q.60 Difference between purchase date and used date of spare part.

    #purchase_tab.join(ser_det,purchase_tab.SPID==ser_det.SPID).select(purchase_tab.PDATE,ser_det.SER_DATE).show()

    # a = purchase_tab.select(purchase_tab.PDATE).subtract(ser_det.select(ser_det.SER_DATE))
    # ser_det.join(a,a.SPID==ser_det.SPID).show()












































