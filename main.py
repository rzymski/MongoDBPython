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


def zad9():
    movie = titleCollection.find_one({'primaryTitle': 'Blade Runner', 'startYear': 1982})
    if movie:
        rating = ratingCollection.find_one({'tconst': movie['tconst']})
        if rating:
            average_rating = rating['averageRating']
            num_votes = rating['numVotes']
            titleCollection.update_one({'_id': movie['_id']}, {'$set': {'rating': [{'averageRating': average_rating, 'numVotes': num_votes}]}})
            print("Pole rating zostało pomyślnie dodane do dokumentu.")
            movieAfterUpdate = titleCollection.find_one({'primaryTitle': 'Blade Runner', 'startYear': 1982})
            ic(movieAfterUpdate)
        else:
            print("Ocena dla filmu 'Blade Runner' z roku 1982 nie została znaleziona.")
    else:
        print("Film 'Blade Runner' z roku 1982 nie został znaleziony.")


def zad10():
    movie = titleCollection.find_one({'primaryTitle': 'Blade Runner', 'startYear': 1982})
    if movie:
        titleCollection.update_one({'_id': movie['_id']}, {'$push': {'rating': {'averageRating': 10, 'numVotes': 55555}}})
        print("Nowy dokument został pomyślnie dodany do pola rating.")
        movieAfterUpdate = titleCollection.find_one({'primaryTitle': 'Blade Runner', 'startYear': 1982})
        ic(movieAfterUpdate)
    else:
        ic("Film 'Blade Runner' z roku 1982 nie został znaleziony.")


def zad11():
    movie = titleCollection.find_one({'primaryTitle': 'Blade Runner', 'startYear': 1982})
    if movie:
        titleCollection.update_one({'_id': movie['_id']}, {'$unset': {'rating': ''}})
        print("Pole rating zostało pomyślnie usunięte z dokumentu.")
        movieAfterUpdate = titleCollection.find_one({'primaryTitle': 'Blade Runner', 'startYear': 1982})
        ic(movieAfterUpdate)
    else:
        print("Film 'Blade Runner' z roku 1982 nie został znaleziony.")


def zad12():
    # titleCollection.delete_one({'primaryTitle': 'Pan Tadeusz', 'startYear': 1999})
    movie = titleCollection.find_one({'primaryTitle': 'Pan Tadeusz', 'startYear': 1999})
    if not movie:
        titleCollection.update_one({'primaryTitle': 'Pan Tadeusz', 'startYear': 1999}, {'$set': {'avgRating': 9.1}}, upsert=True)
        print("Film 'Pan Tadeusz' z 1999 roku został pomyślnie dodany z polem avgRating.")
        movieAfterUpdate = titleCollection.find_one({'primaryTitle': 'Pan Tadeusz', 'startYear': 1999})
        ic(movieAfterUpdate)
    else:
        titleCollection.update_one({'_id': movie['_id']}, {'$set': {'avgRating': 9.1}})
        print("Pole avgRating zostało dodane/zaaktualizowane do filmu 'Pan Tadeusz' z 1999 roku.")


def zad13():
    zad13Result = titleCollection.delete_many({'startYear': {'$lt': 1989}})
    ic(zad13Result.deleted_count)

# zad1()
# zad2()
# zad3()
# zad4a()
# zad4b()  # DZIALA, ALE WYKONUJE SIĘ PONAD 3 H
# zad5() # COS POPSUTE
# zad6()
# zad8("Blade Runner", 1982)
# zad9()
# zad10()
# zad11()
# zad12()
# ##zad13()