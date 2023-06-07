import sys
sys.path.append('..')

from utils import get_db
import pprint

def query_student_by_value(field: str, value: int | str) -> None:

    db = get_db()
    res = db.students.find_one({field: value})
    pprint.pprint(res)