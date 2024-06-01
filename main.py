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

query = {'startYear': 2020, 'genres': {'$regex': 'Romance', '$options': 'i'}, 'runtimeMinutes': {'$gt': 90, '$lte': 120}}
zad2Display = {'_id': 0, 'primaryTitle': 1, 'startYear': 1, 'genres': 1, 'runtimeMinutes': 1}
zad2Results = titleCollection.find(query, zad2Display).sort('primaryTitle', 1).limit(4)
# Wyświetlenie 4 dokumentow
for doc in zad2Results:
    print(doc)
totalCount = titleCollection.count_documents(query)
print(f"\nTotal documents matching the criteria: {totalCount}\n\n")

# ZAD 3



