from pymongo import MongoClient
import redis
from bson.objectid import ObjectId
from datetime import date


client = MongoClient()
client = MongoClient('localhost', 27017)

db = client.FinalProject
collection = db['Netflix']

r = redis.Redis(host='localhost', port=6379, db=0)
r.flushdb()

# Have a maximum memory capacity - 100MB
r.config_set("maxmemory", "104857600")

# Queries about detailed information about db
# Given a movie name - Obtain the director cast, countries and release year.
def movie_name(movie):
	try:
		key = movie
		info = r.get(key).decode("utf-8")
	except:
		my_query = {"title": movie}
		my_doc = list(collection.find(my_query))

		for x in my_doc:
			if (x["type"] == "Movie"):
				info = "Director: " + x["director"] + "\n" + "Cast: " + x["cast"] + "\n" + \
					"Country: " + x["country"] + "\n" + \
					"Release year: " + x["release_year"]
				key = movie
				r.set(key, info)
				r.expire(key, "300")
				print(info)

# Given an actor name - Obtain a list with the movies and a list with the TV shows where he/she has participated.
def lists_of_actor(actor):
	try:
		key = actor
		info = r.get(key).decode("utf-8")
	except:
		info = ""
		key = actor
		my_query = collection.find({"cast": {"$regex": actor}})
		for x in my_query:
			info += x["title"]+"\n"
		print(info)
		r.set(key, info)
		r.expire(key, "300")

# Given a TV show name - Obtain the director cast, countries, and release year.
def tvShow_name(tvShow):
	try:
		key = tvShow
		info = r.get(key).decode("utf-8")
	except:
		my_query = {"title": tvShow}
		my_doc = list(collection.find(my_query))

	for x in my_doc:
		if (x["type"] == "TV Show"):
			info = "Director: " + x["director"] + "\n" + "Cast: " + x["cast"] + "\n" + \
				"Country: " + x["country"] + "\n" + \
				"Release year: " + x["release_year"]
	key = tvShow
	r.set(key, info)
	r.expire(key, "300")
	print(info)

# Queries about statistic about db
# Total number of movies and TV shows.
def total_tv_shows():
	try:
		total = r.get("total_movies").decode("utf-8")
		total_1 = r.get("total_tv_shows").decode("utf-8")
		print("Total number of movies: ", total)
		print("Total number of tv shows: ", total_1)
	except:
		my_doc = collection.count_documents({"type": "Movie"})
		my_doc_1 = collection.count_documents({"type": "TV Show"})
		r.set("total_movies", my_doc)
		r.set("total_tv_shows", my_doc_1)
		r.expire("total_movies", "300")
		r.expire("total_tv_shows", "300")
		print("Total number of movies: ", my_doc)
		print("Total number of tv shows: ", my_doc_1)

# Total number of movies for a given country.
def total_movies_country(country):
	try:
		key = country
		r.get(key).decode("utf-8")
	except:
		my_doc = collection.count_documents({"country": country, "type": "Movie"})
		key = country
		r.set(key, my_doc)
		r.expire(key, "300")
		print("Total number of movies", my_doc)

# Total number of TV shows for given release year.
def total_tv_shows_by_year(release_year):
	try:
		key = release_year
		r.get(key).decode("utf-8")
	except:
		my_doc = collection.count_documents({"release_year": release_year,"type": "TV Show"})
		r.set(key, my_doc)
		r.expire(key, "300")
		print("Total number of TV shows of ",release_year," are: ",my_doc)

# One query to add or update an entity in the database
# Add a new movie
def add_movie(title, director, cast, country, date_added, release_year, rating, duration, listed_in, description):
	try:
		key = title
		info = r.get(key).decode("utf-8")
	except:
		my_query = [{"$match": {"title": title}}]
		my_doc = list(collection.aggregate(my_query))
		if(len(my_doc) > 0):
			print("The movie already exists in the database.\n")
		else:
			my_query = {"type": "Movie", "title": title, "director": director, "cast": cast, "country": country, "date_added": date_added,
						"release_year": release_year, "rating": rating, "duration": duration, "listed_in": listed_in, "description": description}
			my_doc = collection.insert_one(my_query)
			key = title
			r.set(key, info)
			r.expire(key, "300")
			print("The movie", title, "was succesfully added to the database")
# Add a new TV show
def add_tvShow(title, director, cast, country, date_added, release_year, rating, duration, listed_in, description):
	try:
		key = title
		info = r.get(key).decode("utf-8")
	except:
		my_query = [{"$match": {"title": title}}]
		my_doc = list(collection.aggregate(my_query))
		if(len(my_doc) > 0):
			print("The tv show already exists in the database.\n")
		else:
			my_query = {"type": "TV Show", "title": title, "director": director, "cast": cast, "country": country, "date_added": date_added,
					"release_year": release_year, "rating": rating, "duration": duration, "listed_in": listed_in, "description": description}
			my_doc = collection.insert_one(my_query)
			key = title
			r.set(key, info)
			r.expire(key, "300")
			print("The tv show", title, "was succesfully added to the database")


if __name__ == '__main__':
	ans1 = True
	while ans1:
		print ("""
		1. Given a movie name - Obtain the director cast, countries and release year
		2. Given an actor name - Obtain a list with the movies and a list with the TV shows where he/she has participated.
		3. Given a TV show name - Obtain the director cast, countries, and release year.
		4. Total number of movies and TV shows.
		5. Total number of movies for a given country.
		6. Total number of TV shows for given release year.
		7. Add a new movie
		8. Add a new TV show
		9. Exit
		""")
		ans = int(input("What would you like to do?\n"))
		if ans == 1:
			ans = input("Movie name?\n") 
			movie_name(ans)
		elif ans== 2:
			ans = input("Actor name?\n") 
			lists_of_actor(ans)
		elif ans== 3:
			ans = input("TV Show name?\n")
			tvShow_name(ans)
		elif ans== 4:
			total_tv_shows()
		elif ans== 5:
			ans = input("Country?\n")
			total_movies_country(ans)
		elif ans== 6:
			ans = input("Year?\n")
			total_tv_shows_by_year(ans)
		elif ans== 7:
			title = input("Name of the movie:\n")
			director = input("Director:\n")
			cast = input("Movie cast:\n")
			country = input("Name of the country:\n")
			date_added = now.strftime("%B %d, %Y")
			release_year = input("Release year:\n")
			rating = input("Rating:\n")
			duration = input("Duration:\n")
			listed_in = input("Listed in:\n")
			description = input("Description\n")
			add_movie(title, director, cast, country, date_added, release_year, rating, duration, listed_in, description)
		elif ans== 8:
			title = input("Name of the TV Show:\n")
			director = input("Director:\n")
			cast = input("Movie cast:\n")
			country = input("Name of the country:\n")
			date_added = now.strftime("%B %d, %Y")
			release_year = input("Release year:\n")
			rating = input("Rating:\n")
			duration = input("Duration:\n")
			listed_in = input("Listed in:\n")
			description = input("Description\n")
			add_tvShow(title, director, cast, country, date_added, release_year, rating, duration, listed_in, description)
		else:
			ans1 = False
		