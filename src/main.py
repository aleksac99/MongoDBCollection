from utils import get_db

from operators.comparison_operators import *

if __name__=="__main__":

    db = get_db()

    # query_student_by_value(db, "first_name", "Booth") # Returns 1
    # print("---")
    # query_student_by_value(db, "first_name", "Booth", True) # Returns Cursor with 1 value
    # print("---")
    # query_student_by_value(db, "first_name", "Alex") # Returns None
    # print("---")
    # query_student_by_value(db, "dob", "2023-01-01", True) # Returns Cursor with 2 values

    # -----------------------------

    # query_student_by_value(db, "grades.subject", "Dart") # Returns 1
    # query_student_by_value(db, "grades.subject", "Dart", True) # Returns multiple

    # -----------------------

    # query_students_by_operator(db, "dob", "$lt", "2022-06-15")
    query_students_by_operator(db, "dob", "$in", ["2023-01-01", "2023-02-07"], True)

    # -----------------------
    # query_students_by_logic_operator(
    #     db, "$and", ["dob", "gender"], ["2023-01-01", "Male"], True) # 2 Born on given date, 1 is Male

    # query_students_by_logic_operator(
    #     db, "$or", ["dob", "dob"], ["2023-01-01", "2023-02-07"], True)