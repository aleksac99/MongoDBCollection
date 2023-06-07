import pprint
from pymongo.database import Database

def query_student_by_value(db: Database, field: str, value: int | str, many: bool=False) -> None:

    if many:
        res = db.students.find({field: value})
        for r in res:
            pprint.pprint(r)
    else:
        res = db.students.find_one({field: value})
        pprint.pprint(res)

def query_students_by_operator(db: Database, field: str, operator: str, value: str, many: bool=False) -> None:

    if many:
        res = db.students.find({field: {
            operator: value
        }})

        for r in res:
            pprint.pprint(r)

    else:
        res = db.students.find_one({field: {
            operator: value
        }})
        pprint.pprint(res)


def query_students_by_logic_operator(db: Database, logic: str, fields: list, values: list, many: bool=False) -> None:
    
    if many:
        res = db.students.find(
            {logic: [{f:v} for f, v in zip(fields, values)]})

        for r in res:
            pprint.pprint(r)

    else:
        res = db.students.find_one(
            {logic: [{f:v} for f, v in zip(fields, values)]})

        pprint.pprint(res)