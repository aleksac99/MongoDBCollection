import pprint
from utils import get_db


from operators.query_and_projection import *

if __name__=="__main__":

    db = get_db()

    # pprint.pprint(q1(db))
    pprint.pprint(q2(db))