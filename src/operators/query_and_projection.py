from pymongo.database import Database

def q1(db: Database) -> list[dict]:
    """
    Q: Get first_name, last_name and gender of all students born on 2023-01-01
    """
    res = db.students.find({
        "dob": "2023-01-01"
    }, {"_id": 0, "first_name": 1, "last_name": 1, "gender": 1})

    return list(res)

def q2(db: Database) -> list[dict]:
    """
    Q: Get first_name, last_name, dob and gender of all males born on 2023-02-07
    """
    res = db.students.find({
        "dob": "2023-02-07",
        "gender": "Male"
    }, {"_id": 0, "first_name": 1, "last_name": 1, "gender": 1, "dob": 1})

    return list(res)


def q3(db: Database) -> list[dict]:
    """
    Q: Get first_name, last_name, dob and gender of all males born on 2023-02-07 or 2023-01-01
    """
    res = db.students.find({
        "gender": "Male",
        "$or": [
            {"dob": "2023-01-01"},
            {"dob": "2023-02-07"}
        ]},
    {"_id": 0, "first_name": 1, "last_name": 1, "gender": 1, "dob": 1})

    return list(res)

def q4(db: Database) -> list[dict]:
    """
    Q: Get id, dob and grades of all males born before 2023-06-10
    """
    res = db.students.find({
        "gender": "Male",
        "dob": {"$lt": "2022-06-10"}},
        {"grades": 1, "dob": 1})

    return list(res)

def q5(db: Database) -> list[dict]:
    """
    Q: Get first_name, dob, genders and grades of all males graded in python born before 2022-08-01
    """
    res = db.students.find({
        "gender": "Male",
        "dob": {"$lt": "2022-07-01"},
        "grades.subject": "Python"},
        {"grades": 1, "dob": 1, "first_name": 1, "_id": 0, "gender": 1})

    return list(res)

def q6(db: Database) -> list[dict]:
    """
    Q: Get first_name, dob, gender and grades of all agenders and genderfluids graded in javascript or typescript
    """
    res = db.students.find({
        "gender": {"$in": ["Agender", "Genderfluid"]}, # One of these
        "$and": [{"grades.subject": "Typescript"}, {"grades.subject": "Javascript"}]}, # Graded in both of these - both exist
        {"grades": 1, "dob": 1, "first_name": 1, "_id": 0, "gender": 1})

    return list(res)

def q7(db: Database) -> list[dict]:
    """
    Q: Get first_name, dob, gender and grades of all agenders and genderfluids graded in Dart and not in Typescript
    """
    res = db.students.find({
        "gender": {"$in": ["Agender", "Genderfluid"]}, # One of these
        "$and": [{"grades.subject": "Dart"}, {"grades.subject": {"$nin": ["Typescript"]}}]},
        {"grades": 1, "dob": 1, "first_name": 1, "_id": 0, "gender": 1})

    return list(res)
    
def q8(db: Database) -> list[dict]:
    """
    Q: Get first_name, dob, gender and grades of all agenders not graded in neither python nor sql
    """
    res = db.students.find({
        "gender": "Agender",
        "grades.subject": {"$nin": ["Python", "SQL"]}},
        {"grades": 1, "dob": 1, "first_name": 1, "_id": 0, "gender": 1})

    return list(res)

def q9(db: Database) -> list[dict]:
    """
    Q: Get first_name, dob, gender and grades of all agenders with grade in java greater than 7
    """
    res = db.students.find({
        "gender": "Agender",
        "grades": {"$elemMatch": {"subject": "Java", "grade": {"$gt": 7}}}},
        {"grades": 1, "dob": 1, "first_name": 1, "_id": 0, "gender": 1})

    return list(res)

def q10(db: Database) -> list[dict]:
    """
    Q: Get first_name, dob, gender and grades of all agenders with grade in java greater than 7 and grade in c equal or greater than 9
    """
    res = db.students.find({
        "gender": "Agender",
        "grades": {"$all": [
            {"$elemMatch": {"subject": "Java", "grade": {"$gt": 7}}},
            {"$elemMatch": {"subject": "C", "grade": {"$gte": 9}}}]}},
        {"grades": 1, "dob": 1, "first_name": 1, "_id": 0, "gender": 1})

    return list(res)

def q11(db: Database) -> list[dict]:
    """
    Q: Get first_name, dob, gender and grades of all agenders graded in 7 subjects
    """
    res = db.students.find({
        "gender": {"$nin": ["Male", "Female"]},
        "grades": {"$size": 7}},
        {"grades": 1, "dob": 1, "first_name": 1, "_id": 0, "gender": 1})

    return list(res)

def q12(db: Database) -> list[dict]:
    """
    Q: Get first_name, dob, gender and grades of all agenders graded in at least 9 subjects
    """
    res = db.students.find({
        "gender": {"$nin": ["Male", "Female"]},
        "grades.8": {"$exists": True}},
        {"grades": 1, "dob": 1, "first_name": 1, "_id": 0, "gender": 1})

    return list(res)

def q13(db: Database) -> list[dict]:
    """
    Q: Get first subject of all males born on 2013-02-07 with grade greater than 8
    """
    res = db.students.find({
        "gender": "Male",
        "dob": "2023-02-07",
        "grades": {"$elemMatch": {"grade": {"$gt": 8}}}},
        {"grades.$": 1, "dob": 1, "first_name": 1, "_id": 0, "gender": 1})

    return list(res)

def q14(db: Database) -> list[dict]:
    """
    Q: Get first subject of all males born on 2023-02-07 and 2023-01-01 with grade greater than 8. Sort by name ascending
    """
    res = db.students.find({
        "gender": "Male",
        "dob": {"$in": ["2023-02-07", "2023-01-01"]},
        "grades": {"$elemMatch": {"grade": {"$gt": 8}}}},
        {"grades.$": 1, "dob": 1, "first_name": 1, "_id": 0, "gender": 1}).sort({"first_name": 1, "dob": 1})

    return list(res)

def q15(db: Database) -> list[dict]:
    """
    Q: Get first subject of all males born on 2013-02-07 with grade greater than 8
    """
    res = db.students.find({
        "gender": "Male",
        "dob": "2023-02-07",
        "grades": {"$elemMatch": {"grade": {"$gt": 8}}}},
        {"grades.$": 1, "dob": 1, "first_name": 1, "_id": 0, "gender": 1}).limit(1)
    
    return list(res)