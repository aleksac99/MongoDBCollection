# TODO: Check the validity of first 10 questions
from pymongo.database import Database

def q1(db: Database) -> list:
    """
    Q: Find number of people for each gender
    """
    res = db.students.aggregate([
        {"$group": {"_id": "$gender", "cnt": {"$sum": 1}}}
    ])
    return list(res)

def q2(db: Database) -> list:
    """
    Q: Find number of people for each gender born before 2000-01-01
    """
    res = db.students.aggregate([
        {"$match": {"dob": {"$lt": "2000-01-01"}}},
        {"$group": {"_id": "$gender", "cnt": {"$sum": 1}}}
    ])
    return list(res)

def q3(db: Database) -> list:
    """
    Q: Find dates of birth of all females born before 1995-01-01
    """
    res = db.students.aggregate([
        {"$match": {"gender": "Female", "dob": {"$lt": "1995-01-01"}}},
        {"$group": {"_id": "$dob"}}
    ])
    return list(res)

def q4(db: Database) -> list:
    """
    Q: Find number of distinct dates of birth of all females born before 2022-07-01
    """
    res = db.students.aggregate([
        {"$match": {"gender": "Female", "dob": {"$lt": "2022-07-01"}}},
        {"$group": {"_id": "$dob"}},
        {"$count": "num_dobs"}
    ])
    return list(res)

def q5(db: Database) -> list:
    """
    Q: Return grades if its size is 3
    """
    res = db.students.aggregate([
        {"$match": {"dob": "2023-02-07"}},
        {"$project": {"name": "$first_name", "dob": 1, "_id": 0, "java_grade": "$grades"}}
    ])
    return list(res)


def q6(db: Database) -> list:
    """
    Q: Return grades if its size is 3
    """
    res = db.students.aggregate([
        {"$unwind": "$grades"},
        # {"$match": {"grades.subject": "Rust", "gender": {"$nin": ["Male", "Female"]}}},
        {"$group": {"_id": "$grades.subject", "average": {"$avg": "$grades.grade"}}},
        {"$project": {"lang": "$_id", "_id": 0, "average": 1}},
        {"$sort": {"average": -1}}
        # {"$count": "num_rust_grades"}
    ])
    return list(res)

def q7(db: Database) -> list:
    """
    Q: Return grades if its size is 3
    """
    res = db.students.aggregate([
        {"$unwind": "$subjects"},
        # {"$match": {"grades.subject": "Rust", "gender": {"$nin": ["Male", "Female"]}}},
        {"$group": {"_id": "$subjects.subject", "count": {"$sum": 1}}},
        {"$project": {"lang": "$_id", "_id": 0, "count": 1}},
        {"$sort": {"count": -1}}
        # {"$count": "num_rust_grades"}
    ])
    return list(res)


# -------------------------------------------------------------------------

def q11(db: Database) -> list:
    """
    Q: Find all first year students. Sort by date of birth, return maximum 10 results
    """
    res = db.students.aggregate([
        {"$match": {"year": 1}},
        {"$sort": {"dob": 1}},
        {"$limit": 10}
    ])

    return list(res)

def q12(db: Database) -> list:
    """
    Q: Find all students born after 2002-12-01.
    """
    res = db.students.aggregate([
        {"$match": {"dob": {"$gt": "2002-12-01"}}}
    ])

    return list(res)


def q13(db: Database) -> list:
    """
    Q: For each date where more than 1 student have birthday, find name and year of those student grouped by date of birth.
    """
    res = db.students.aggregate([
        {"$group": {
            "_id": "$dob",
            "cnt": {"$sum": 1},
            "students": {"$push": {"name": {"$concat": ["$first_name", " ", "$last_name"]}, "year": "$year"}}}
        },
        {"$match": {"cnt": {"$gt": 1}}},
        {"$project": {"students": 1}}
    ])

    return list(res)


def q14(db: Database) -> list:
    """
    Q: Find average year of studies for all genders
    """
    res = db.students.aggregate([
        {"$group": {"_id": "$gender", "avg_year": {"$avg": "$year"}}},
        {"$project": {"avg_year": {"$round": ["$avg_year", 2]}}},
        {"$sort": {"avg_year": 1, "_id": -1}}
    ])

    return list(res)


def q15(db: Database) -> list:
    """
    Q: Find number of exam tries for all people born in  January or February 1999
    """
    res = db.students.aggregate([
        {"$match": {"$and": [{"dob": {"$gte": "1999-01-01"}}, {"dob": {"$lte": "1999-03-01"}}]}},
        {"$group": { "_id": None, "total_tries": {"$sum": {"$size": "$entrance_exam_results"}}}}
    ])

    return list(res)

def q16(db: Database) -> list:
    """
    Q: Find average last entrance exam result for people born between 1999-01-01 and 1999-03-01 grouped by gender.
    """
    res = db.students.aggregate([
        {"$match": {"$and": [{"dob": {"$gte": "1999-01-01"}}, {"dob": {"$lte": "1999-03-01"}}]}},
        {"$group": {"_id": "$gender", "last_exam_avg": {"$avg": {"$last": "$entrance_exam_results"}}}},
        {"$project": {"last_exam_avg": {"$round": ["$last_exam_avg", 2]}}}
    ])

    return list(res)


def q17(db: Database) -> list:
    """
    Q: Group by tags
    """
    res = db.students.aggregate([
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "cnt": {"$sum": 1}}},
        {"$match": {"cnt": {"$gt": 10}}},
        {"$sort": {"cnt": -1}}
    ])

    return list(res)


def q18(db: Database) -> list:
    """
    Q: Find average year for each tag that occurs more than 15 times.
    """
    res = db.students.aggregate([
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "avg_year": {"$avg": "$year"}, "cnt": {"$sum": 1}}},
        {"$match": {"cnt": {"$gt": 15}}},
        {"$sort": {"avg_year": 1}},
        {"$project": {"avg_year": {"$round": ["$avg_year", 2]}}}
    ])

    return list(res)

def q19(db: Database) -> list:
    """
    Q: Find email with maximum number of letters for each gender
    """
    res = db.students.aggregate([
        {"$match": {"$and": [{"dob": {"$gte": "1999-01-01"}}, {"dob": {"$lte": "1999-03-01"}}]}},
        {"$group": {"_id": "$gender", "emails": {"$push": {"email":"$email", "size": {"$strLenCP": "$email"}}}, "max_lets": {"$max": {"$strLenCP": "$email"}}}},
        {"$project": {
            "max_lets": 1,
            "email": {"$first": {"$filter": {"input": "$emails", "as": "tmp", "cond": {"$eq": ["$$tmp.size", "$max_lets"]}}}}}},
        {"$project": {"email": "$email.email", "size": "$max_lets"}}
    ])

    return list(res)