from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['IMDB']
titleCollection = db['Title']
ratingCollection = db['Rating']
nameCollection = db['Name']

# ZAD 1
titleCount = titleCollection.count_documents({})
ratingCount = ratingCollection.count_documents({})
nameCount = nameCollection.count_documents({})

print(f"Number of documents in 'Title' collection: {titleCount}")
print(f"Number of documents in 'Rating' collection: {ratingCount}")
print(f"Number of documents in 'Name' collection: {nameCount}\n\n")

# ZAD 2

zad2Query = {'startYear': 2020, 'genres': {'$regex': 'Romance', '$options': 'i'}, 'runtimeMinutes': {'$gt': 90, '$lte': 120}}
zad2Display = {'_id': 0, 'primaryTitle': 1, 'startYear': 1, 'genres': 1, 'runtimeMinutes': 1}
zad2Results = titleCollection.find(zad2Query, zad2Display).sort('primaryTitle', 1).limit(4)
# Wyświetlenie 4 dokumentow
for doc in zad2Results:
    print(doc)
totalCount = titleCollection.count_documents(zad2Query)
print(f"Total documents matching the criteria: {totalCount}\n")

# ZAD 3
zad3Query = [{'$match': {'startYear': 2000}},
             {'$group': {'_id': '$titleType', 'count': {'$sum': 1}}},
             {'$project': {'_id': 0, 'titleType': '$_id', 'count': 1}}]
zad3Result = titleCollection.aggregate(zad3Query)
for doc in zad3Result:
    print(doc)

