from pymongo import MongoClient
user = 'root'
password = 'cpXB1VbdFeTqH0k6m85guvFG' # Insert Your MongoDB Password
host='mongo'
#create the connection url
connecturl = "mongodb://{}:{}@{}:27017/?authSource=admin".format(user,password,host)

# connect to mongodb server
print("Connecting to mongodb server")
connection = MongoClient(connecturl)

# select the 'training' database 

db = connection.training

# select the 'mongodb_glossary' collection 

collection = db.mongodb_glossary

# create a sample document

doc1 = {"database" : "a database contains collections"}
doc2 = {"collection": "a collection stores the documents"}
doc3 = {"document" : "a document contains the data in the form of key value pairs."}

# insert a sample document

print("Inserting a document into collection.")
db.collection.insert_one(doc1)
db.collection.insert_one(doc2)
db.collection.insert_one(doc3)

# query for all documents in 'training' database and 'python' collection

docs = db.collection.find()

print("Printing the documents in the collection.")

for document in docs:
    print(document)

# close the server connecton
print("Closing the connection.")
connection.close()