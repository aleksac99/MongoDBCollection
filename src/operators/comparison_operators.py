from utils import get_db
import pprint

def query_student(operator, value):

    db = get_db()
    res = db.students.find_one({"first_name": "Mike"})
    pprint.pprint(res)

if __name__=="__main__":
    query_student(1,1)