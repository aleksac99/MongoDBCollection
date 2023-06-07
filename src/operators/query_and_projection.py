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


def q2(db: Database) -> list[dict]:
    """
    Q: Get first_name, last_name, dob and gender of all males born on 2023-02-07
    """