#!/usr/bin/env python3
import psycopg2

# Connecting to DB
db = psycopg2.connect("dbname=news")

cursor = db.cursor()

# Query to find Most Popular three articles of all time
cursor.execute("SELECT title, count(1) as cnt FROM log JOIN articles AS art "
               "ON SUBSTR(log.path,10) = art.slug "
               "GROUP BY title ORDER BY cnt DESC LIMIT 3")
print ('')
print ("What are the most popular three articles of all time?")
for i in cursor.fetchall():
    print (i[0] + ' - ' + str(i[1]))

# Query to find Most popular article authors of all time
cursor.execute("SELECT auth.name, count(1) AS views FROM log JOIN articles "
               "AS art ON SUBSTR(log.path,10) = art.slug JOIN authors auth "
               "ON art.author = auth.id "
               "GROUP BY auth.name ORDER BY views desc")
print ('')
print ("Who are the most popular article authors of all time")
for i in cursor.fetchall():
    print (i[0] + ' - ' + str(i[1]))

# Query to find on which days more than 1% of requests leads to errors
cursor.execute("SELECT TO_CHAR(totdt, 'MONTH DD,YYYY'), "
               "ROUND(CAST (errcnt AS decimal) * 100 /totcnt,2) AS perc "
               "FROM (SELECT time::date AS totdt, count(1) AS totcnt "
               "FROM log GROUP BY time::date) AS tot, (SELECT time::date AS "
               "errdt, count(1) AS errcnt FROM log WHERE SUBSTR(status,1,3) "
               "= '404' GROUP BY time::date) AS err "
               "WHERE totdt = errdt AND errcnt * 100 /totcnt > 1")
print ('')
print ("On which days did more than 1% of requests lead to errors?")
for i in cursor.fetchall():
    print (i[0] + ' - ' + str(i[1]))

db.close()
