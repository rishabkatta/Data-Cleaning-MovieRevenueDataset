'''
@author-name: Rishab Katta

Python Program for performing Data Cleaning activities on json file before updating documents on the MongoDB Database

NOTE: This Program assumes that there's already a loaded Database present with name IMDB and Schema mentioned in Assignment 4.
'''

from pymongo import MongoClient
import pymongo
import gzip
import shutil
import json
import matplotlib.pyplot as plt

import numbers




class MongoDBManagement:

    def __init__(self,host,port):
        '''
        Constructor used to connect to mongodb databases and initialize a MongoDB database

        :param host: Hostname for MongoDB and Postgres databases
        :param port: Port number for MongoDB database
        '''
        self.client = MongoClient(host, port)
        self.database = self.client['IMDB']



    def update_document(self,path):
        '''
        Update documents in the MongoDB collection Movies from the documents read and matched from extra-data.json file

        :return: None
        '''



        with gzip.open(str(path) + 'extra-data.json.gz', 'rb') as f_in:
            with open(str(path) + 'extra-data.json', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        data = []
        for line in open(str(path) + 'extra-data.json', 'r'):
            data.append(json.loads(line))


        self.collection = self.database['Movies']
        count=0

        for movie_json in data:
            movie_id = None
            if "IMDb_ID" in movie_json:
                movie_id = movie_json.get("IMDb_ID").get("value")
                if movie_id.startswith('t'):
                    movie_id = int(movie_id.lstrip('tt0'))
                else:
                    continue
            doc={}
            bool =False
            if movie_id is not None:
                doc=self.collection.find_one({"_id": {"$eq": movie_id}})
            if doc:
                if "box_office_currencyLabel" in movie_json:
                    currency = movie_json.get("box_office_currencyLabel").get("value")
                    if currency == 'United States dollar':
                        if "box_office" in movie_json:
                            revenue = movie_json.get("box_office").get("value")
                            if isinstance(revenue, numbers.Integral) and revenue > 10000:                         # checking if revenue is a number and if it's greater than 10000(generally true)
                                self.collection.update_one({"_id": movie_id}, { "$set": {"revenue": revenue}} )
                                bool =True
                if "cost" in movie_json:
                    cost = movie_json.get("cost").get("value")
                    if isinstance(cost, numbers.Integral) and cost > 10000:                                        # checking if cost is a number and if it's greater than 10000(generally true)
                        self.collection.update_one({"_id": movie_id}, {"$set": {"cost": cost}})
                        bool = True
                if "distributorLabel" in movie_json:
                    distributor = movie_json.get("distributorLabel").get("value")
                    if isinstance(distributor, str):                                                                # checking if distributor label is a string
                        self.collection.update_one({"_id": movie_id}, {"$set": {"distributor": distributor}})
                        bool = True
                if "MPAA_film_ratingLabel" in movie_json:
                    rating = movie_json.get("MPAA_film_ratingLabel").get("value")
                    if rating in ["G", "PG", "PG-13", "R", "NC-17"]:                                                # checking if rating label is one of the values in the list.
                        self.collection.update_one({"_id": movie_id}, {"$set": {"rating": rating}})
                        bool = True
                if bool:
                    count +=1
        print("successful updates(matched by ID):" + str(count))


        ##############################################################################################################

        #Document matching without Movie IDs

        self.collection = self.database['Movies']


        self.collection.create_index([('title', pymongo.ASCENDING)])                                               #indexing title to speed up querying


        count = 0
        notmatch=0
        # countsimilar=0
        for movie_json in data:
            title=None
            mid=None
            if "titleLabel" in movie_json:
                title = movie_json.get("titleLabel").get("value")
                title=title.strip()
            if "IMDb_ID" in movie_json:
                movie_id = movie_json.get("IMDb_ID").get("value")
                if movie_id.startswith('t'):
                    movie_id = int(movie_id.lstrip('tt0'))
                    docs=self.collection.aggregate([{"$match": {"title": title}},{"$project": {"_id": 1}}])
                    for movie in docs:
                        mid = movie['_id']
                    if mid is not None and movie_id is not None and mid != movie_id:
                        notmatch +=1                                                                # movie ids from extra-data.json and mongodb not matching for the same title
                                                                                                    #notmatch is a count of all such documents
            doc = {}
            bool = False
            if title is not None:
                # numdocs = self.collection.count_documents({"title": {"$eq": title}})               #to check if more than 1 documents have the same title
                # if numdocs>1:                                                                      # commented because it's taking a long time to run.
                #     countsimilar +=1
                doc = self.collection.find_one({"title": {"$eq": title}})
            if doc:
                if "box_office_currencyLabel" in movie_json:
                    currency = movie_json.get("box_office_currencyLabel").get("value")
                    if currency == 'United States dollar':
                        if "box_office" in movie_json:
                            revenue = movie_json.get("box_office").get("value")
                            if isinstance(revenue, numbers.Integral) and revenue>10000:              #checking if revenue is a number and if it's greater than 10000(generally true)
                                self.collection.update_one({"title": title}, {"$set": {"revenue": revenue}})
                                bool = True
                if "cost" in movie_json:
                    cost = movie_json.get("cost").get("value")
                    if isinstance(cost, numbers.Integral) and cost > 10000:                          #checking if cost is a number and if it's greater than 10000(generally true)
                        self.collection.update_one({"title": title}, {"$set": {"cost": cost}})
                        bool = True
                if "distributorLabel" in movie_json:
                    distributor = movie_json.get("distributorLabel").get("value")
                    if isinstance(distributor, str):                                                  #checking if distributor label is a string
                        self.collection.update_one({"title": title}, {"$set": {"distributor": distributor}})
                        bool = True
                if "MPAA_film_ratingLabel" in movie_json:
                    rating = movie_json.get("MPAA_film_ratingLabel").get("value")
                    if rating in ["G", "PG", "PG-13" , "R", "NC-17"]:                                 #checking if rating label is one of the values in the list.
                        self.collection.update_one({"title": title}, {"$set": {"rating": rating}})
                        bool = True
                if bool:
                    count += 1
        print("successful updates(matched by title):" + str(count))
        print("Documents having same title but different IDs:" + str(notmatch))
        # print(countsimilar)


    def query_4_1(self):
        '''
        Generate Box Plot for average ratings of movies with more than 10K votes for each genre
        :return:
        '''

        self.collection = self.database['Movies']
        docs = self.collection.find({}, {"genres":1, "_id":0})

        completelist=[]
        for doc in docs:
            genrelist = doc['genres']
            for genre in genrelist:
                if genre not in completelist:
                    completelist.append(genre)

        genres_ratings = {}
        for genre in completelist:
            pipeline = [{"$match": {
                         "$and": [{"genres": genre}, {"numvotes": {"$gt": 10000}}]}},
                     {"$project": {"_id": 0, "avgrating": 1}}]
            docs=self.collection.aggregate(pipeline)
            listofratings = sorted([doc ['avgrating'] for doc in docs])
            if genre is not None and len(listofratings)!=0:
                genres_ratings[genre] = listofratings

        labels, data = genres_ratings.keys(), genres_ratings.values()

        plt.boxplot(data)
        plt.xticks(range(1, len(labels) + 1), labels)
        plt.show()

    def query_4_2(self):
        '''
        Generate bar chart for average number of actors in movies by genre and genre
        :return: None
        '''

        self.collection = self.database['Movies']
        docs = self.collection.find({}, {"genres": 1, "_id": 0})
        completelist = []
        for doc in docs:
            genrelist = doc['genres']
            for genre in genrelist:
                if genre not in completelist:
                    completelist.append(genre)

        genres_avgactors = {}
        for genre in completelist:

            pipeline = [{"$match": {
                         "$and": [{"genres": genre}, {"actors": {"$exists": True}}]}},
                    {"$group" : {"_id": None ,  "totalactorsbygenre": {"$sum": {"$size": "$actors"}}, "totalmoviesbygenre": { "$sum": 1 }}},
                     {"$project": {"_id":0, "totalactorsbygenre": 1, "totalmoviesbygenre": 1}}]
            docs = self.collection.aggregate(pipeline)
            for doc in docs:
                if genre is not None and doc['totalactorsbygenre'] is not None and doc['totalmoviesbygenre'] is not None:
                    genres_avgactors[genre] = doc['totalactorsbygenre']//doc['totalmoviesbygenre']

        plt.bar(range(len(genres_avgactors)), genres_avgactors.values(), align='center')
        plt.xticks(range(len(genres_avgactors)), list(genres_avgactors.keys()))

        plt.show()

    def query_4_3(self):
        '''
        Generate time series plot for startyear and number of movies made in that year
        :return: None
        '''

        self.collection = self.database['Movies']
        docs = self.collection.find({}, {"startyear": 1, "_id": 0})

        completelist = []
        for doc in docs:
            startyear = doc.get("startyear")
            if startyear not in completelist:
                    completelist.append(startyear)

        startyear_movies = {}
        for startyear in completelist:
            pipeline = [{"$match": {"startyear": startyear}},
                {"$group": {"_id": None, "totalmoviesbyyear": {"$sum": 1}}},
                {"$project": {"_id": 0, "totalmoviesbyyear": 1}}]

            docs = self.collection.aggregate(pipeline)
            for doc in docs:
                if startyear is not None and doc['totalmoviesbyyear'] is not None:
                    startyear_movies[startyear] = doc['totalmoviesbyyear']

        # print(startyear_movies)
        #
        labels, data = sorted(startyear_movies.keys()), sorted(startyear_movies.values())

        plt.plot(labels,data)
        plt.show()







if __name__ == '__main__':
    port = int(input("Enter port MongoDB's running on"))
    host = input("Enter host for MongoDB")
    path = input("Enter path for extra-data.json file with / at the end like /user/downloads/    ")


    mongodb =MongoDBManagement(host,port)
    mongodb.update_document(path)
    # close each chart generated to generate the next chart
    mongodb.query_4_1()
    mongodb.query_4_2()
    mongodb.query_4_3()
