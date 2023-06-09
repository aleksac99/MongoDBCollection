import pprint
from utils import get_db

# from operators.query_and_projection import *
from operators.aggregation import *

if __name__=="__main__":

    db = get_db()

    pprint.pprint(q7(db))