from pymongo import MongoClient
from icecream import ic

client = MongoClient('mongodb://localhost:27017/')
db = client['IMDB']
titleCollection = db['Title']
ratingCollection = db['Rating']
nameCollection = db['Name']


def printDocList(docList):
    for doc in docList:
        print(doc)


def zad1():
    titleCount = titleCollection.count_documents({})
    ratingCount = ratingCollection.count_documents({})
    nameCount = nameCollection.count_documents({})
    ic(titleCount)
    ic(ratingCount)
    ic(nameCount)


def zad2():
    zad2Query = {'startYear': 2020, 'genres': {'$regex': 'Romance', '$options': 'i'}, 'runtimeMinutes': {'$gt': 90, '$lte': 120}}
    zad2Display = {'_id': 0, 'primaryTitle': 1, 'startYear': 1, 'genres': 1, 'runtimeMinutes': 1}
    zad2Results = titleCollection.find(zad2Query, zad2Display).sort('primaryTitle', 1).limit(4)
    # Wyświetlenie 4 dokumentow
    printDocList(zad2Results)
    zad2Count = titleCollection.count_documents(zad2Query)
    ic(zad2Count)


def zad3():
    zad3Query = [{'$match': {'startYear': 2000}},
                 {'$group': {'_id': '$titleType', 'count': {'$sum': 1}}},
                 {'$project': {'_id': 0, 'titleType': '$_id', 'count': 1}}]
    zad3Result = titleCollection.aggregate(zad3Query)
    printDocList(zad3Result)


def zad4a():
    zad4Query = {'genres': 'Documentary', 'startYear': {'$gte': 2010, '$lte': 2012}}
    zad4aResult = titleCollection.count_documents(zad4Query)
    ic(zad4aResult)


def zad4b():
    zad4bQuery = [{'$match': {'genres': 'Documentary', 'startYear': {'$gte': 2010, '$lte': 2012}}},
                  {'$lookup': {'from': 'Rating', 'localField': 'tconst', 'foreignField': 'tconst', 'as': 'joinRating'}},
                  {'$project': {'_id': 0, 'primaryTitle': 1, 'startYear': 1, 'averageRating': '$joinRating.averageRating'}},
                  {'$sort': {'averageRating': -1}},
                  {'$limit': 5}]
    zad4bResult = list(titleCollection.aggregate(zad4bQuery))
    printDocList(zad4bResult)


def zad5():
    # nameCollection.drop_index('primaryName_text')
    nameCollection.create_index([('primaryName', 'text')])
    zad5Query = {'$text': {'$search': 'Fonda Coppola', '$caseSensitive': True}}
    zad5aResult = nameCollection.count_documents(zad5Query)
    ic(zad5aResult)
    zad5bResult = nameCollection.find(zad5Query, {'_id': 0, 'primaryName': 1, 'primaryProfession': 1}).limit(5)
    printDocList(zad5bResult)


def zad6():
    nameCollection.create_index([('birthYear', -1)])
    indexes = list(nameCollection.list_indexes())
    for index in indexes:
        ic(index)
    indexCount = len(indexes)
    ic(indexCount)


def zad8(title, year):
    movie = titleCollection.find_one({'primaryTitle': title, 'startYear': year})
    if movie:
        rating = ratingCollection.find_one({'tconst': movie['tconst']})
        if rating:
            averageRating = rating['averageRating']
            zad8Result = {'primaryTitle': title, 'startYear': year, 'averageRating': averageRating}
            ic(zad8Result)

# zad1()
# zad2()
# zad3()
# zad4a()
# zad4b()  # DZIALA, ALE WYKONUJE SIĘ PONAD 3 H
# zad5() # COS POPSUTE
# zad6()
zad8("Casablanca", 1942)

