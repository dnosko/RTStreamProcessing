from sqlalchemy import String, DateTime, Boolean, ForeignKey, Table, Column, Integer, MetaData

metadata = MetaData()
users = Table('users', metadata,
              Column('id', Integer, primary_key=True),
              Column('device', Integer)
              )

polygons_category = Table('category_polygons', metadata,
                          Column('id', Integer, primary_key=True),
                          Column('name', String))

polygons = Table('polygons', metadata,
                 Column('id', Integer, primary_key=True),
                 Column('creation', DateTime),
                 Column('valid', Boolean),
                 Column('category', Integer, ForeignKey('polygons_category.id')),
                 Column('fence', String))
