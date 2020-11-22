from pymongo import MongoClient
client = MongoClient()

client = MongoClient('localhost', 27017)

db = client.FinalProject
collection = db['Netflix']

#To test connection to the db
my_query = {"title" : "Norm of the North: King Sized Adventure"}
my_doc = collection.find(my_query)

for x in my_doc:
    print(x)