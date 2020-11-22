from pymongo import MongoClient
import redis
import datetime
from bson.objectid import ObjectId

client = MongoClient()
client = MongoClient('localhost', 27017)

db = client.FinalProject
collection = db['Netflix']

r = redis.Redis(host='localhost', port=6379, db=0)
r.flushdb()

#Have a maximum memory capacity

#Queries about detailed information about db
#Given a movie name - Obtain the director cast, countries and release year.
def movie_name(movie):
	try:
		key = movie
		info = r.get(key).decode("utf-8")
	except:
		my_query = {"title" : movie}
		my_doc = list(collection.find(my_query))
		
		for x in my_doc:
			if (x["type"] == "Movie"):
				info = "Director: " + x["director"] +"\n" + "Cast: " + x["cast"] +"\n"+ "Country: " + x["country"] +"\n"+ "Release year: " + x["release_year"]
				key = movie
				#Redis cache
				r.set(key, info)
				#Idk about the time
				r.expire(key, "30")
				print(info)

#Given an actor name - Obtain a list with the movies and a list with the TV shows where he/she has participated.
#Given a TV show name - Obtain the director cast, countries, and release year.

#Queries about statistic about db
#Total number of movies and TV shows.
def total_tv_shows():
    try:
        total = r.get("total_movies").decode("utf-8")
        total_1 = r.get("total_tv_shows").decode("utf-8")
        print("Total number of movies: ", total)
        print("Total number of tv shows: ", total_1)
    except:
        my_doc = collection.count_documents({ "type": "Movie"})
        my_doc_1 = collection.count_documents({ "type": "TV Show"})
        r.set("total_movies", my_doc)
        r.set("total_tv_shows", my_doc_1)
        r.expire("total_movies", "30")
        r.expire("total_tv_shows", "30")
       	print("Total number of movies: ", my_doc)
       	print("Total number of tv shows: ", my_doc_1)
        
#Total number of movies for a given country.
#Total number of TV shows for given release year.

#One query to add or update an entity in the database
#Add a new movie
def add_movie(title, director, cast, country, date_added, release_year, rating, duration, listed_in, description):
    my_query = [{"$match" :{"title":title}}]
    my_doc = list(collection.aggregate(my_query))
    if(len(my_doc) > 0):
        print("The movie name already exists in the database.\n")
    else:
        my_query = {"type": "Movie", "title":title, "director":director, "cast":cast, "country":country, "date_added": date_added, "release_year": release_year, "rating": rating, "duration": duration, "listed_in": listed_in, "description": description}
        my_doc = collection.insert_one(my_query)
        print("The movie", title, "was succesfully added to the database")
#Add a new TV show

if __name__ == '__main__':
	movie_name("Norm of the North: King Sized Adventure")
	total_tv_shows()
	add_movie("The SpongeBob Movie: Sponge on the Run", "Tim Hill", "Tom Kenny", "United States", "November 22, 2020", "2020", "A", "91 min", "Comedy", "Live/Action film based on the animated television series SpongeBob SquarePants.")




