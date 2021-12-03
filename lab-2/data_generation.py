from model import ModelPostgreSQL
import xml.etree.ElementTree as ET
from typing_types import *
from typing import Dict
from collections import defaultdict
from numpy import array
import ex_hadler
from time import time


class Analyzer:
    COMMON_TYPES = array([
        ["character varying", str],
        ["varchar", str],
        ["char", str],
        ["character", str],
        ["text", str],
        ["smallint", int],
        ["integer", int],
        ["bigint", int],
        ["smallserial", int],
        ["serial", int],
        ["bigserial", int]
    ])

    CUSTOM_TYPES = array([
        ["content length", str]
    ])

    def __init__(self, model: ModelPostgreSQL, is_empty: bool = False):
        self.__model = model
        self.__type_analysis = self.__analyze_data_types() if not is_empty else None

    @property
    def model(self):
        return self.__model

    @property
    def type_analysis(self):
        return self.__type_analysis

    def __analyze_data_types(self) -> Dict[str, Dict[str, Tuple[str, bool]]]:
        with self.model.conn.cursor() as cursor:
            cursor: psql_cur
            tables = self.model.tables
            parsed = defaultdict(dict)

            for table in tables:
                cursor.execute(f"""SELECT column_name, data_type FROM information_schema.columns
                                   WHERE table_name = '{table}'""")
                for attribute, data_type in cursor.fetchall():
                    parsed[table][attribute] = (data_type, data_type in self.COMMON_TYPES[:, 0])

            return dict(parsed)


class SQLGenerator(Analyzer):
    def __init__(self, model: ModelPostgreSQL, xml_source: str, is_empty: bool = False):
        super().__init__(model, is_empty)
        self.doc_path = xml_source
        self.text_request = f"SELECT chr(trunc(65 + random()*25)::int) ||" \
                            f"chr(trunc(65 + random()*25)::int) || chr(trunc(65 + random()*25)::int) || " \
                            f"chr(trunc(65 + random()*25)::int) || chr(trunc(65 + random()*25)::int) ||" \
                            f"chr(trunc(65 + random()*25)::int) || chr(trunc(65 + random()*25)::int) " \
                            f"from generate_series(1, 1)"

    def create_tables(self, conn: psql_conn):
        def __find_field(song, wanted_field):
            found = False

            for tag in song:
                if not found:
                    # Looking for the wanted field
                    if tag.tag == "key" and tag.text == wanted_field:
                        found = True
                else:
                    # After founding it, we return the content of the following
                    # tag (the one with its value)
                    return tag.text

            return False

        try:
            with conn.cursor() as cursor:
                try:
                    cursor.execute('''
                        DROP TABLE IF EXISTS artist CASCADE;
                        DROP TABLE IF EXISTS genre CASCADE;
                        DROP TABLE IF EXISTS album CASCADE;
                        DROP TABLE IF EXISTS track CASCADE;

                        CREATE TABLE artist (
                            id  SERIAL NOT NULL PRIMARY KEY UNIQUE,
                            name CHARACTER VARYING(255) NOT NULL UNIQUE
                        );
                        CREATE TABLE genre (
                            id  SERIAL NOT NULL PRIMARY KEY UNIQUE,
                            name CHARACTER VARYING(255) UNIQUE NOT NULL
                        );
                        CREATE TABLE album (
                            id  SERIAL NOT NULL PRIMARY KEY UNIQUE,
                            title CHARACTER VARYING(255) UNIQUE NOT NULL,
                            tracks_number SMALLINT NOT NULL,
                            artist_id  BIGINT NOT NULL,
                            CONSTRAINT FK_artist_id
                            FOREIGN KEY (artist_id) REFERENCES artist (id) 
                            ON UPDATE CASCADE 
                            ON DELETE CASCADE

                        );
                        CREATE TABLE track (
                            id  SERIAL NOT NULL PRIMARY KEY UNIQUE,
                            title CHARACTER VARYING(255) NOT NULL,
                            len CHARACTER VARYING(255) NOT NULL,
                            year SMALLINT NOT NULL,
                            number_within_album SMALLINT NOT NULL,
                            album_id  BIGINT NOT NULL,
                            genre_id  BIGINT NOT NULL,

                            CONSTRAINT FK_album_id
                                FOREIGN KEY (album_id) 
                                    REFERENCES album (id) 
                                        ON UPDATE CASCADE 
                                        ON DELETE CASCADE,
                            CONSTRAINT FK_genre_id
                                FOREIGN KEY (genre_id) REFERENCES genre (id) 
                                    ON UPDATE CASCADE 
                                    ON DELETE CASCADE
                        );
                        ''')
                    conn.commit()

                except Exception as _ex:
                    raise ex_hadler.Error(_ex)

            data_source = open(self.doc_path)
            data = data_source.read()
            xml_data = ET.fromstring(data)

            # Obtaining every tag with track data
            tracks_data = xml_data.findall("dict/dict/dict")

            genres = set()
            artists = set()
            albums = set()
            tracks = set()

            # Getting the values of the fields we'll insert
            for track in tracks_data:
                title = __find_field(track, "Name")
                artist = __find_field(track, "Artist")
                genre = __find_field(track, "Genre")
                album = __find_field(track, "Album")
                year = int(__find_field(track, "Year"))
                track_num = int(__find_field(track, "Track Number"))
                album_len = int(__find_field(track, "Track Count"))

                tracks.add(title)
                artists.add(artist)
                albums.add(album)
                genres.add(genre)

                with conn.cursor() as cursor:

                    # Artist
                    if artist:  # If it's a filled string, != False
                        # If the value hasn't been introduced yet and exists, we'll insert it
                        artist_statement = """INSERT INTO artist(name) SELECT %s WHERE NOT EXISTS
                            (SELECT * FROM Artist WHERE name = %s)"""
                        params = (artist, artist)  # Params needed for completing the statement
                        cursor.execute(artist_statement, params)

                    # Genre
                    if genre:  # If it's a filled string, != False
                        # If the value hasn't been introduced yet and exists, we'll insert it
                        genre_statement = """INSERT INTO Genre(name) SELECT %s WHERE NOT EXISTS
                            (SELECT * FROM Genre WHERE name = %s)"""
                        params = (genre, genre)
                        cursor.execute(genre_statement, params)

                    # Album
                    if album:  # If it's a filled string, != False
                        # First of all, we'll get the artist id
                        artistID_statement = "SELECT id from Artist WHERE name = %s"
                        cursor.execute(artistID_statement, (artist,))
                        # .fetchone() returns a one-element tuple, and we want its content
                        artist_id = cursor.fetchone()[0]

                        # Now we're going to insert the data
                        album_statement = """INSERT INTO Album(title, artist_id, tracks_number)
                            SELECT %s, %s, %s WHERE NOT EXISTS (SELECT * FROM Album WHERE title = %s)"""
                        params = (album, artist_id, album_len, album)
                        cursor.execute(album_statement, params)

                    # Track
                    if title:  # If it's a filled string, != False
                        # Obtaining genre_id
                        genreID_statement = "SELECT id from Genre WHERE name = %s"
                        cursor.execute(genreID_statement, (genre,))
                        try:
                            genre_id = cursor.fetchone()[0]
                        except TypeError:
                            genre_id = 0
                        # Obtaining album_id
                        albumID_statement = "SELECT id from Album WHERE title = %s"
                        cursor.execute(albumID_statement, (album,))
                        try:
                            album_id = cursor.fetchone()[0]
                        except TypeError:
                            album_id = 0

                        # Inserting data
                        track_statement = """INSERT INTO Track(title, album_id, genre_id, len, year, number_within_album) 
                        SELECT %s, %s, %s, 
                        (select '00:' 
                            || (chr(trunc(48+random()*2)::int)::text)  || (chr(trunc(48+random()*10)::int)::text) || ':' 
                            || (chr(trunc(48+random()*6)::int)::text)  || (chr(trunc(48+random()*10)::int)::text) 
                            from generate_series(1, 1)), %s, %s WHERE NOT EXISTS (SELECT * FROM Track WHERE title = %s)"""
                        params = (title, album_id, genre_id, year, track_num, title)
                        cursor.execute(track_statement, params)

            conn.commit()

        except Exception as _ex:
            raise ex_hadler.Error(_ex)

    def get_if_unique(self, table: str, attr: str) -> bool:
        with self.model.conn.cursor() as cursor:
            sql_get_all = f"SELECT {attr} FROM {table};"
            cursor.execute(sql_get_all)
            elements = tuple(element[0] for element in cursor.fetchall())
            cursor.execute(self.text_request)
            value = cursor.fetchall()[0][0]
            if value in elements:
                return False
            return value

    def generate_one_unique_attr_table(self, table: str, rows_to_generate: int):
        try:
            n_gen_rows = 0
            with self.model.conn.cursor() as cursor:
                while n_gen_rows != rows_to_generate:
                    value = self.get_if_unique(table, "name")
                    if value:
                        sql_insert = f"INSERT INTO {table} (name) VALUES (\'{value}\')"
                        cursor.execute(sql_insert)
                        n_gen_rows += 1
                self.model.conn.commit()

        except Exception as _ex:
            raise ex_hadler.Error(_ex)

    def generate_table(self, table: str, rows_to_generate: int) -> Tuple[str, float]:
        table_analysis = self.type_analysis[table]
        if False in [is_valid for data_type, is_valid in table_analysis.values()]:
            raise TypeError("The required type is not supported for generation.")
        try:
            with self.model.conn.cursor() as cursor:
                # cursor.execute(f"""SELECT con.*
                #                    FROM pg_catalog.pg_constraint con
                #                         INNER JOIN pg_catalog.pg_class rel
                #                                    ON rel.oid = con.conrelid
                #                         INNER JOIN pg_catalog.pg_namespace nsp
                #                                    ON nsp.oid = connamespace
                #                    WHERE nsp.nspname = 'public'
                #                          AND rel.relname = '{table}';""")

                # columns = tuple(table_analysis.keys())
                # constraints = tuple((row[1], row[3]) for row in cursor.fetchall())
                # types = (arg_type for arg_type, is_supported in list(table_analysis.values()))
                n_gen_rows = 0
                start = time()
                if "genre" == table:
                    self.generate_one_unique_attr_table("genre", rows_to_generate)
                elif "artist" == table:
                    self.generate_one_unique_attr_table("artist", rows_to_generate)
                elif "album" == table:
                    while n_gen_rows != rows_to_generate:

                        value = self.get_if_unique(table, "title")
                        if not value:
                            continue
                        sql_insert = f"INSERT INTO album (title, tracks_number, artist_id) VALUES (" \
                                     f"({self.text_request}), " \
                                     f"(SELECT trunc(random()*(50-1)+1)::int FROM generate_series(1, 1))," \
                                     f"(SELECT id FROM artist ORDER BY RANDOM() LIMIT 1))"
                        cursor.execute(sql_insert)
                        n_gen_rows += 1
                elif "track" == table:
                    while n_gen_rows != rows_to_generate:
                        sql_album_id_tracks_num = "SELECT id, tracks_number FROM album ORDER BY RANDOM() LIMIT 1"
                        cursor.execute(sql_album_id_tracks_num)
                        album_id, album_tracks = cursor.fetchall()[0]
                        sql_insert = f"INSERT INTO track (title, len, year, number_within_album, album_id, genre_id) " \
                                     f"VALUES (" \
                                     f"({self.text_request}), " \
                                     f"(SELECT chr(trunc(48+random()*3)::int) || chr(trunc(48+random()*4)::int) || ':' " \
                                     f"|| chr(trunc(48+random()*6)::int)  || chr(trunc(48+random()*10)::int) || ':' " \
                                     f"|| chr(trunc(48+random()*6)::int)  || chr(trunc(48+random()*10)::int) " \
                                     f"FROM generate_series(1, 1)), " \
                                     f"(SELECT trunc(random()*(2021-1940+1)+1940)::int FROM generate_series(1, 1))," \
                                     f"(SELECT trunc(floor(random()*({album_tracks}-1+1)+1))::int " \
                                     f"FROM generate_series(1, 1))," \
                                     f"{album_id}," \
                                     f"(SELECT id FROM genre ORDER BY random() LIMIT 1));"
                        cursor.execute(sql_insert)
                        n_gen_rows += 1
                result = time() - start
                self.model.conn.commit()
                return f"INSERT 0 {rows_to_generate}", result
        except Exception as _ex:
            raise ex_hadler.Error(_ex)
