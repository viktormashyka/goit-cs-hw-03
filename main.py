from pymongo import MongoClient
from pymongo.server_api import ServerApi

client = MongoClient(
    "mongodb+srv://vmashyka:0gdAJopgSfzuVUPW@cluster0.q0h5hq6.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0&tlsAllowInvalidCertificates=true",
    server_api=ServerApi('1')
)

db = client.test

def add_cat_barsik():
    result = db.cats.find_one({"name": "barsik"})
    if result is None:
        db.cats.insert_one(
            {
                "name": "barsik",
                "age": 3,
                "features": ["ходить в капці", "дає себе гладити", "рудий"],
            }
        )
        print('Success, barsik added!')
    else:
        print('Barsik already exists!')

def add_cat_lama():
    result = db.cats.find_one({"name": "Lama"})
    if result is None:
        db.cats.insert_one(
            {
                "name": "Lama",
                "age": 2,
                "features": ["ходить в лоток", "не дає себе гладити", "сірий"],
            },
        )
        print('Success, Lama added!')
    else:
        print('Lama already exists!')

def add_cat_liza():
    result = db.cats.find_one({"name": "Liza"})
    if result is None:
        db.cats.insert_one(
            {
                "name": "Liza",
                "age": 4,
                "features": ["ходить в лоток", "дає себе гладити", "білий"],
            },
        )
        print('Success, Liza added!')
    else:
        print('Liza already exists!')

# Реалізуйте функцію для виведення всіх записів із колекції.
def fetch_all():
    result = db.cats.find({})
    if result is None:
        print('No cats found!')
        return
    print('All cats: ')
    for el in result:
        print(el)

# Реалізуйте функцію, яка дозволяє користувачеві ввести ім'я кота та виводить інформацію про цього кота.
def fetch_by_name(name: str):
    find_document = db.cats.find_one({"name": name})
    if find_document is None:
        print('Cat with name ${name} not found!')
        return
    print('Cat details: ', find_document)

# Створіть функцію, яка дозволяє користувачеві оновити вік кота за ім'ям.
def update_age_by_name(name: str, age: int):
    find_document = db.cats.find_one({"name": name})
    if find_document is None:
        print('Cat with name ${name} not found!')
        return
    db.cats.update_one({"name": name}, {"$set": {"age": age}})
    updated_document = db.cats.find_one({"name": name})
    print('Success, document updated! Updated document: ', updated_document)

# Створіть функцію, яка дозволяє додати нову характеристику до списку features кота за ім'ям.
def add_feature_by_name(name: str, feature: str):
    find_document = db.cats.find_one({"name": name})
    if find_document is None:
        print('Cat with name ${name} not found!')
        return
    db.cats.update_one({"name": name}, {"$push": {"features": feature}})
    updated_document = db.cats.find_one({"name": name})
    print('Success, document updated! Updated document: ', updated_document)

# Реалізуйте функцію для видалення запису з колекції за ім'ям тварини.
def delete_by_name(name: str):
    find_document = db.cats.find_one({"name": name})
    if find_document is None:
        print('Cat with name ${name} not found!')
        return
    db.cats.delete_many({"name": name})
    find_deleted_document = db.cats.find_one({"name": name})
    if find_deleted_document is None:
        print('Success, document deleted!')

# Реалізуйте функцію для видалення всіх записів із колекції.
def delete_all():
    db.cats.delete_many({})
    if db.cats.count_documents({}) == 0:
        print('Success, all documents deleted!')

if __name__ == '__main__':
    add_cat_barsik()
    add_cat_lama()
    add_cat_liza()
    fetch_all()
    fetch_by_name("barsik")
    update_age_by_name("barsik", 4)
    add_feature_by_name("barsik", "грається з м'ячиком")
    delete_by_name("barsik")
    delete_all()