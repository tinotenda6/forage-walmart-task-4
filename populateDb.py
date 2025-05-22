import csv
import sqlite3

class PopulateDatabase:
    def __init__(self):
        self.products = []
        self.origins = []
        self.destinations = []
        self.quantitys = []

    def extractContained(self, file):

        with open(file, newline='') as csvfile:
            spamreader = csv.DictReader(csvfile)
            for row in spamreader:
                #extract 
                self.products.append(row["product"])
                self.origins.append(row["origin_warehouse"])
                self.destinations.append(row["destination_store"])
                self.quantitys.append(row["product_quantity"])


    def extractNonContained(self,file1, file2):
        identifier = []
        prods = []
        with open(file1, newline='') as csvfile:
            spamreader = csv.DictReader(csvfile)
            for row in spamreader:
                #extract 
                prods.append(row["product"])
                identifier.append(row["shipment_identifier"])
        
        with open(file2, newline='') as csvfile:
            spamreader = csv.DictReader(csvfile)
            for row in spamreader:
                id = row["shipment_identifier"]
                indexIdFile1 = identifier.index(id)
                if indexIdFile1 != -1:
                    self.products.append(prods[indexIdFile1])
                    self.origins.append(row["origin_warehouse"])
                    self.destinations.append(row["destination_store"])
                    self.quantitys.append(identifier.count(id))
    
    def insertDb(self):
            #populate product table
        setProduct = set(self.products)

        con = sqlite3.connect("forage-walmart-task-4/shipment_database.db")

        cursor = con.cursor()
        for prod in setProduct:
            cursor.execute("INSERT INTO product (name) VALUES (?)", (prod,))
        
        cursor.execute("SELECT * FROM product")
        prodRows = cursor.fetchall()#create a map of product, product_id
        prodIdMap = {name: id for id, name in prodRows}
    
        # insert into shipment
        # iterate i to size of product 
        for i in range(len(self.origins)):
            prodId = prodIdMap[self.products[i]]
            org = self.origins[i]
            des = self.destinations[i]
            quant = self.quantitys[i]
            cursor.execute("INSERT INTO shipment (product_id,quantity,origin,destination) VALUES (?,?,?,?)", (prodId,quant,org, des,))

        cursor.execute("SELECT * FROM shipment")
        print(cursor.fetchall())
        # Commit changes and close
        con.commit()
        con.close()

pD = PopulateDatabase()
pD.extractContained('forage-walmart-task-4/data/shipping_data_0.csv')
pD.extractNonContained('forage-walmart-task-4/data/shipping_data_1.csv','forage-walmart-task-4/data/shipping_data_2.csv')
pD.insertDb()

