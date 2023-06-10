from pymongo.database import Database

def q1(db: Database) -> list:
    """
    Q: Find student count for each gender.
    """
    res = db.students.aggregate([
        {"$group": {"_id": "$gender", "cnt": {"$sum": 1}}}
    ])
    return list(res)

def q2(db: Database) -> list:
    """
    Q: Find student count for each gender of all students born before 2000-01-01.
    """
    res = db.students.aggregate([
        {"$match": {"dob": {"$lt": "2000-01-01"}}},
        {"$group": {"_id": "$gender", "cnt": {"$sum": 1}}}
    ])
    return list(res)

def q3(db: Database) -> list:
    """
    Q: Find dates of birth of all females born before 1995-01-01.
    """
    res = db.students.aggregate([
        {"$match": {"gender": "Female", "dob": {"$lt": "1995-01-01"}}},
        {"$group": {"_id": "$dob"}}
    ])
    return list(res)

def q4(db: Database) -> list:
    """
    Q: Find number of distinct dates of birth of all females born before 2022-07-01.
    """
    res = db.students.aggregate([
        {"$match": {"gender": "Female", "dob": {"$lt": "2022-07-01"}}},
        {"$group": {"_id": "$dob"}},
        {"$count": "num_dobs"}
    ])
    return list(res)

def q5(db: Database) -> list:
    """
    Q: For each female born in 1995 graded in Roman History, return their Roman History grade. 
    """
    res = db.students.aggregate([
        {"$match": {"gender": "Female", "dob": {"$gte": "1995-01-01"}, "dob": {"$lte": "1995-12-31"}}},
        {"$unwind": "$subjects"},
        {"$match": {"subjects.subject": "Roman History"}},
        {"$project": {"name": {"$concat": ["$first_name", " ", "$last_name"]}, "dob": 1, "gender": 1, "_id": 0, "history_grade": "$subjects.grade"}}
    ])
    return list(res)

def q6(db: Database) -> list:
    """
    Q: Find all first year students. Sort by date of birth, return maximum 10 results
    """
    res = db.students.aggregate([
        {"$match": {"year": 1}},
        {"$sort": {"dob": 1}},
        {"$limit": 10}
    ])

    return list(res)

def q7(db: Database) -> list:
    """
    Q: Find all students born after 2002-12-01.
    """
    res = db.students.aggregate([
        {"$match": {"dob": {"$gt": "2002-12-01"}}}
    ])

    return list(res)


def q8(db: Database) -> list:
    """
    Q: Get names and year of each student grouped by their date of birth, for all dates with two or more students born on that day.
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


def q9(db: Database) -> list:
    """
    Q: Find average year of studies for all genders. Round average year to two decimals.
    """
    res = db.students.aggregate([
        {"$group": {"_id": "$gender", "avg_year": {"$avg": "$year"}}},
        {"$project": {"avg_year": {"$round": ["$avg_year", 2]}}},
        {"$sort": {"avg_year": 1, "_id": -1}}
    ])

    return list(res)


def q10(db: Database) -> list:
    """
    Q: Find number of entrance exam tries for all people born in January or February 1999.
    """
    res = db.students.aggregate([
        {"$match": {"$and": [{"dob": {"$gte": "1999-01-01"}}, {"dob": {"$lte": "1999-03-01"}}]}},
        {"$group": { "_id": None, "total_tries": {"$sum": {"$size": "$entrance_exam_results"}}}}
    ])

    return list(res)

def q11(db: Database) -> list:
    """
    Q: Find average last entrance exam result for people born between 1999-01-01 and 1999-03-01 grouped by gender.
    """
    res = db.students.aggregate([
        {"$match": {"$and": [{"dob": {"$gte": "1999-01-01"}}, {"dob": {"$lte": "1999-03-01"}}]}},
        {"$group": {"_id": "$gender", "last_exam_avg": {"$avg": {"$last": "$entrance_exam_results"}}}},
        {"$project": {"last_exam_avg": {"$round": ["$last_exam_avg", 2]}}}
    ])

    return list(res)


def q12(db: Database) -> list:
    """
    Q: Group students by tags.
    """
    res = db.students.aggregate([
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "cnt": {"$sum": 1}}},
        {"$match": {"cnt": {"$gt": 10}}},
        {"$sort": {"cnt": -1}}
    ])

    return list(res)


def q13(db: Database) -> list:
    """
    Q: Find average year for each tag that occurs more than 15 times. Sort by average year ascending.
    """
    res = db.students.aggregate([
        {"$unwind": "$tags"},
        {"$group": {"_id": "$tags", "avg_year": {"$avg": "$year"}, "cnt": {"$sum": 1}}},
        {"$match": {"cnt": {"$gt": 15}}},
        {"$sort": {"avg_year": 1}},
        {"$project": {"avg_year": {"$round": ["$avg_year", 2]}}}
    ])

    return list(res)

def q14(db: Database) -> list:
    """
    Q: Find email with maximum number of letters for each gender among students born between 1999-01-01 and 1999-03-01.
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


def q15(db: Database) -> list:
    """
    Q: Find average grade for each person born from 1999-01-01 and 1999-03-01.
    """
    res = db.students.aggregate([
        {"$match": {"$and": [{"dob": {"$gte": "1999-01-01"}}, {"dob": {"$lte": "1999-03-01"}}]}},
        {"$unwind": "$subjects"},
        {"$group": {"_id": "$_id", "avg_grade": {"$avg": "$subjects.grade"},
                    "name": {"$first": {"$concat": ["$first_name", " ", "$last_name"]}}}},
        {"$project": {"_id": 0, "avg_grade": {"$round": ["$avg_grade", 2]}, "name": 1}}
    ])

    return list(res)


def q16(db: Database) -> list:
    """
    Q: Find a person with the highest grade average with at least 7 grades.
    """
    res = db.students.aggregate([
        {"$match": {"subjects.6": {"$exists": True}}},
        {"$unwind": "$subjects"},
        {"$group": {"_id": {"$concat": ["$first_name", " ", "$last_name"]}, "avg_grade": {"$avg": "$subjects.grade"}}},
        {"$sort": {"avg_grade": -1}},
        {"$limit": 1}
    ])

    return list(res)

def q17(db: Database) -> list:
    """
    Q: Find a person with max and min average from the list of persons with more than 7 grades.
    """
    res = db.students.aggregate([
        {"$match": {"subjects.7": {"$exists": True}}},
        {"$unwind": "$subjects"},
        {"$group": {"_id": {"$concat": ["$first_name", " ", "$last_name"]}, "avg_grade": {"$avg": "$subjects.grade"}}},
        {"$sort": {"avg_grade": -1}},
        {"$group": {"_id": None, "max_name": {"$first": "$_id"}, "max_grade": {"$first": "$avg_grade"},
                                 "min_name": {"$last": "$_id"}, "min_grade": {"$last": "$avg_grade"}}},
        {"$project": {"_id": 0, "max_person": {"max_name": "$max_name", "max_grade": "$max_grade"},
                                "min_person": {"min_name": "$min_name", "min_grade": "$min_grade"}}}
    ])

    return list(res)