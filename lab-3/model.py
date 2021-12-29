import typing

import sqlalchemy
from sqlalchemy import Column, Integer, String, create_engine, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker


Base = declarative_base()
engine = create_engine('postgresql+psycopg2://postgres:20071944__@localhost:5432/music_test')


class Artist(Base):
    __tablename__ = 'artist'

    artist_id = Column(Integer, nullable=False, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    album = relationship("Album", cascade="all, delete")

    def __init__(self, artist_id: int, name: str):
        self.artist_id = artist_id
        self.name = name

    def __repr__(self):
        return "{:>10}{:>35}".format(self.artist_id, self.name)


class Genre(Base):
    __tablename__ = 'genre'

    genre_id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)

    track = relationship("Track", cascade="all, delete")

    def __init__(self, genre_id: int, name: str):
        self.genre_id = genre_id
        self.name = name

    def __repr__(self):
        return "{:>10}{:>35}".format(self.genre_id, self.name)


class Album(Base):
    __tablename__ = 'album'

    album_id = Column(Integer, primary_key=True)
    title = Column(String, unique=True, nullable=False)
    tracks_number = Column(Integer, nullable=False)

    artist_id = Column(Integer, ForeignKey("artist.artist_id"))

    track = relationship('Track', cascade="all, delete")

    def __init__(self, album_id: int, title: str, tracks_number: int, artist_fk: int):
        self.album_id = album_id
        self.title = title
        self.tracks_number = tracks_number
        self.artist_id = artist_fk

    def __repr__(self):
        return "{:>10}{:>35}".format(self.album_id, self.tracks_number)


class Track(Base):
    __tablename__ = "track"

    track_id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    len = Column(String, nullable=False)
    year = Column(Integer, nullable=False)
    number_within_album = Column(Integer, nullable=False)

    genre_id = Column(Integer, ForeignKey("genre.genre_id"))
    album_id = Column(Integer, ForeignKey("album.album_id"))

    def __init__(self, track_id: int, title: str, length: str, year: int,
                 num_in_album: int, genre_id: int, album_fk: int):
        self.track_id = track_id
        self.title = title
        self.len = length
        self.year = year
        self.number_within_album = num_in_album
        self.genre_id = genre_id
        self.album_id = album_fk

    def __repr__(self):
        return "{:>10}{:>35}{:>35}{:>10}{:>10}{:>10}{:>10}"\
            .format(self.track_id, self.title, self.len, self.year,
                    self.number_within_album,
                    self.genre_id, self.album_fk)


def get_rows(table_name: str) -> typing.Set | None:
    Session = sessionmaker(bind=engine)
    session = Session()
    records = None

    if "artist" == table_name:
        records = session.query(Artist.artist_id, Artist.name).all()
    elif "album" == table_name:
        records = session.query(Album.album_id, Album.title,
                                Album.tracks_number, Album.artist_id).all()
    elif "track" == table_name:
        records = session.query(Track.track_id, Track.title,
                                Track.len, Track.year,
                                Track.number_within_album,
                                Track.album_id, Track.genre_id).all()
    elif "genre" == table_name:
        records = session.query(Genre.genre_id, Genre.name).all()

    return records


def get_table_names() -> typing.List:
    return sqlalchemy.inspect(engine).get_table_names(schema="public")


def get_table_columns(table_name: str) -> typing.List:
    inspector = sqlalchemy.inspect(engine)
    return [col_descriptor["name"]
            for col_descriptor in inspector.get_columns(table_name, schema="public")]


def get_table_attr_types(table_name: str) -> typing.List:
    inspector = sqlalchemy.inspect(engine)
    return [(col_descriptor["name"], col_descriptor["type"].python_type)
            for col_descriptor in inspector.get_columns(table_name, schema="public")]


def insert(table_num: int, params: typing.Dict) -> int:
    Session = sessionmaker(bind=engine)
    session = Session()
    id = None

    if table_num == 0:
        id, name = tuple(params.values())
        s = Artist(artist_id=id, name=name)
        session.add(s)

    elif table_num == 1:
        id, title, tracks_number, artist_fk = tuple(params.values())
        s = Album(album_id=id, title=title,
                  tracks_number=tracks_number, artist_fk=artist_fk)
        session.add(s)

    elif table_num == 2:
        id, title, length, year, al_num, genre_id, album_id = tuple(params.values())
        s = Track(track_id=id, title=title, length=length, year=year, num_in_album=al_num,
                  genre_id=genre_id, album_fk=album_id)
        session.add(s)

    elif table_num == 3:
        id, name = tuple(params.values())
        s = Genre(genre_id=id, name=name)
        session.add(s)

    session.commit()
    return id


def delete(table_num: int, params: typing.Dict) -> typing.List:
    Session = sessionmaker(bind=engine)
    session = Session()

    deletion_logs = []
    id = list(params.values())[0]

    if table_num == 0:
        records = session.query(Artist).get(id)
        if records is not None:
            records = session.query(Album).filter(Album.artist_id == id).all()

            if records is not None:
                for record in records:
                    album_id = record.album_id
                    record = session.query(Track).filter(Track.album_id == album_id).all()
                    if record is not None:
                        delete = session.query(Track).filter(Track.album_id == album_id)

                        for i in delete:
                            session.delete(i)
                        deletion_logs.append(["track", album_id, "album_id", "deleted"])

                delete = session.query(Album).filter(Album.artist_id == id)

                for i in delete:
                    session.delete(i)
                deletion_logs.append(["album", id, "artist_id", "deleted"])

            session.delete(session.query(Artist).
                           filter(Artist.artist_id == id).one())

            deletion_logs.append(["artist", id, "artist_id", "deleted"])
        else:
            deletion_logs.append(["artist", id, "artist_id", "not found"])

    elif table_num == 1:
        records = session.query(Album).get(id)
        if records is not None:
            records = session.query(Track).filter(Track.album_id == id).all()
            if records is not None:
                delete = session.query(Track).filter(Track.album_id == id)
                for i in delete:
                    session.delete(i)
                deletion_logs.append(["track", id, "album_id", "deleted"])

            session.delete(session.query(Album).filter(Album.album_id == id).one())
            deletion_logs.append(["album", id, "album_id", "deleted"])
        else:
            deletion_logs.append(["artist", id, "artist_id", "not found"])

    elif table_num == 2:
        records = session.query(Track).get(id)

        if records is not None:
            session.delete(session.query(Track).filter(Track.track_id == id).one())

            deletion_logs.append(["track", id, "track_id", "deleted"])
        else:
            deletion_logs.append(["track", id, "track_id", "not found"])

    elif table_num == 3:
        records = session.query(Genre).get(id)
        if records is not None:
            records = session.query(Track).filter(Track.genre_id == id).all()
            if records is not None:
                delete = session.query(Track).filter(Track.genre_id == id)
                for i in delete:
                    session.delete(i)
                    deletion_logs.append(["track", id, "genre_id", "deleted"])

            session.delete(session.query(Genre).filter(Genre.genre_id == id).one())
            deletion_logs.append(["genre", id, "genre_id", "deleted"])
        else:
            deletion_logs.append(["genre", id, "genre_id", "not found"])
    else:
        "Input correct number"

    session.commit()
    return deletion_logs


def update(table_num: int, params: typing.Tuple[str, ...]) -> typing.List:
    Session = sessionmaker(bind=engine)
    session = Session()

    updating_logs = []

    if table_num == 0:
        id, name = params
        artist = session.query(Album).get(id)
        if artist is not None:
            artist.name = name if name is not None else artist.name
            session.add(artist)
            updating_logs.append(["artist", id, "updated"])
        else:
            updating_logs.append(["artist", id, "not found"])

    elif table_num == 1:
        id, title, artist_id = params
        album = session.query(Album).get(id)
        if album is not None:
            album.title = title if title is not None else album.title
            album.album_id = artist_id if artist_id is not None else album.artist_id
            session.add(album)
            updating_logs.append(["album", id, "updated"])
        else:
            updating_logs.append(["album", id, "not found"])

    elif table_num == 2:
        id, title, len, year, number_within_album, album_id, genre_id = params
        track = session.query(Track).get(params[0])
        if track is not None:
            track.title = title if title is not None else track.title
            track.len = len if len is not None else track.len
            track.year = year if year is not None else track.year
            track.number_within_album = number_within_album \
                if number_within_album else track.number_within_album
            track.album_id = album_id if album_id is not None else track.album_id
            track.genre_id = genre_id if genre_id is not None else track.genre_id

            session.add(track)
            updating_logs.append(["track", id, "updated"])
        else:
            updating_logs.append(["track", id, "not found"])

    elif table_num == 3:
        id, name = params
        genre = session.query(Genre).get(id)
        if genre is not None:
            genre.name = name if name is not None else genre.name
            session.add(genre)
            updating_logs.append(["dgenre", id, "updated"])
        else:
            updating_logs.append(["gtenre", id, "not found"])

    session.commit()
    return updating_logs
