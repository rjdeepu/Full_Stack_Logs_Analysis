import psycopg2

# Connecting to DB
db = psycopg2.connect("dbname=news")

cursor = db.cursor()

# Query to find Most Popular three articles of all time
cursor.execute("select title, count(1) as cnt from log join articles as art "
               "on substr(log.path,10) = art.slug "
               "group by title order by cnt desc limit 3")
result1 = cursor.fetchall()
print (result1)

# Query to find Most popular article authors of all time
cursor.execute("select auth.name, count(1) as views from log join articles "
               "as art on substr(log.path,10) = art.slug join authors auth "
               "on art.author = auth.id "
               "group by auth.name order by views desc")
result2 = cursor.fetchall()
print (result2)

# Query to find on which days more than 1% of requests leads to errors
cursor.execute("select to_char(totdt, 'MONTH DD,YYYY'), "
               "round(cast (errcnt as decimal) * 100 /totcnt,2) as perc "
               "from (select time::date as totdt, count(1) as totcnt "
               "from log group by time::date) as tot, (select time::date as "
               "errdt, count(1) as errcnt from log where substr(status,1,3) "
               "= '404' group by time::date) as err "
               "where totdt = errdt and errcnt * 100 /totcnt > 1")
result3 = cursor.fetchall()
print (result3)

db.close()
