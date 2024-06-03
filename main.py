from pymongo import MongoClient
from icecream import ic

client = MongoClient('mongodb://localhost:27017/')
db = client['IMDB']
titleCollection = db['Title']
ratingCollection = db['Rating']
nameCollection = db['Name']


def zad1():
    titleCount = titleCollection.count_documents({})
    ratingCount = ratingCollection.count_documents({})
    nameCount = nameCollection.count_documents({})

    print(f"Number of documents in 'Title' collection: {titleCount}")
    print(f"Number of documents in 'Rating' collection: {ratingCount}")
    print(f"Number of documents in 'Name' collection: {nameCount}\n")


def zad2():
    zad2Query = {'startYear': 2020, 'genres': {'$regex': 'Romance', '$options': 'i'}, 'runtimeMinutes': {'$gt': 90, '$lte': 120}}
    zad2Display = {'_id': 0, 'primaryTitle': 1, 'startYear': 1, 'genres': 1, 'runtimeMinutes': 1}
    zad2Results = titleCollection.find(zad2Query, zad2Display).sort('primaryTitle', 1).limit(4)
    # Wyświetlenie 4 dokumentow
    for doc in zad2Results:
        print(doc)
    totalCount = titleCollection.count_documents(zad2Query)
    print(f"Total documents matching the criteria: {totalCount}\n")


def zad3():
    zad3Query = [{'$match': {'startYear': 2000}},
                 {'$group': {'_id': '$titleType', 'count': {'$sum': 1}}},
                 {'$project': {'_id': 0, 'titleType': '$_id', 'count': 1}}]
    zad3Result = titleCollection.aggregate(zad3Query)
    for doc in zad3Result:
        print(doc)


def zad4a():
    zad4Query = {'genres': 'Documentary', 'startYear': {'$gte': 2010, '$lte': 2012}}
    zad4aResult = titleCollection.count_documents(zad4Query)
    print(f"\nTotal documents matching the criteria: {zad4aResult}\n")

# zad4bQuery = [{'$match': zad4Query},
#               {'$lookup': {'from': 'Rating', 'localField': 'tconst', 'foreignField': 'tconst', 'as': 'joinRating'}},
#               {'$unwind': '$joinRating'},
#               {'$project': {'primaryTitle': 1, 'startYear': 1, 'averageRating': '$joinRating.averageRating'}},
#               {'$sort': {'averageRating': -1}},
#               {'$limit': 5}]
# zad4bResult = list(titleCollection.aggregate(zad4bQuery))
# for doc in zad4bResult:
#     print(doc)


zad1()
zad2()
zad3()
zad4a()
