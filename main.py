import sqlalchemy
import json
from sqlalchemy.orm import sessionmaker

from models import create_tables, Publisher, Book, Stock, Sale, Shop

DSN = 'postgresql://postgres:Roma2003@localhost:5432/ORM'
engine = sqlalchemy.create_engine(DSN)

create_tables(engine)

Session = sessionmaker(bind=engine)
session = Session()

with open('tests_data.json', 'r') as fd:
    data = json.load(fd)

for record in data:
    model = {
        'publisher': Publisher,
        'shop': Shop,
        'book': Book,
        'stock': Stock,
        'sale': Sale
    }[record.get('model')]
    session.add(model(id=record.get('pk'), **record.get('fields')))
session.commit()


def find_publisher():
    publisher = input("""как вы хотите искать автора?
    name - по имени
    id - по идентификатору""")
    if publisher == 'name':
        a = input('Введите имя автора')
        subq = session.query(Publisher).filter(Publisher.name == a).subquery()
        subq2 = session.query(Book).join(subq, Book.id_publisher == subq.c.id).subquery()
        subq3 = session.query(Stock).join(subq2, Stock.id_book == subq2.c.id).subquery()
        for c in session.query(Shop).join(subq3, Shop.id == subq3.c.id_shop).all():
            print(c)

    elif publisher == 'id':
        b = input('Введите id автора')
        subq = session.query(Publisher).filter(Publisher.id == b).subquery()
        subq2 = session.query(Book).join(subq, Book.id_publisher == subq.c.id).subquery()
        subq3 = session.query(Stock).join(subq2, Stock.id_book == subq2.c.id).subquery()
        for c in session.query(Shop).join(subq3, Shop.id == subq3.c.id_shop).all():
            print(c)


find_publisher()

session.close()
