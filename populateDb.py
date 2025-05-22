import csv
import sqlite3
from collections import defaultdict
#function to read from csv file
def insertSheet1(file):
    product = []
    origin = []
    dest = []
    quantity = []

    with open(file, newline='') as csvfile:
        spamreader = csv.DictReader(csvfile)
        for row in spamreader:
            #extract 
            product.append(row["product"])
            origin.append(row["origin_warehouse"])
            dest.append(row["destination_store"])
            quantity.append(row["product_quantity"])
    #populate product table
    setProduct = set(product)

    con = sqlite3.connect("forage-walmart-task-4/shipment_database.db")

    cursor = con.cursor()
    for prod in setProduct:
        cursor.execute("INSERT INTO product (name) VALUES (?)", (prod,))
    
    cursor.execute("SELECT * FROM product")
    prodRows = cursor.fetchall()#create a map of product, product_id
    prodIdMap = {name: id for id, name in prodRows}
  
    #insert into shipment
    #iterate i to size of product 
    for i in range(len(origin)):
        prodId = prodIdMap[product[i]]
        org = origin[i]
        des = dest[i]
        quant = quantity[i]
        cursor.execute("INSERT INTO shipment (product_id,quantity,origin,destination) VALUES (?,?,?,?)", (prodId,quant,org, des,))

    cursor.execute("SELECT * FROM shipment")
    print(cursor.fetchall())
    # Commit changes and close
    con.commit()
    con.close()

        
# con = sqlite3.connect("forage-walmart-task-4/shipment_database.db")
# cursor = con.cursor()
# cursor.execute("SELECT name, sql FROM sqlite_master WHERE type='table';")
# print(cursor.fetchall())
# insertSheet1('forage-walmart-task-4/data/shipping_data_0.csv')
    
