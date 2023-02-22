from db import DBManager


if __name__ == '__main__':
    db = DBManager()
    data= db.fetch_data('2022-02-22')
