# standard modules import
# for explicit type instantiation from it's string names
import builtins
import dataclasses as dc
# for JSON encoding and decoding
import json
from typing import List, Dict, Union


# Custom data structures
@dc.dataclass
class FilmRecord:
    # class structure for FILM record
    # {"title":"After Dark in Central Park","year":1900,"cast":[],"genres":[]}
    title: str
    year: int
    cast: List
    genres: List

    def to_json(self):
        return json.dumps(self, indent=4, sort_keys=True)

    @staticmethod
    def safe_parse(json_primitive: Dict, field_name: str, type_name: str):
        if json_primitive.get(field_name) != None:
            return json_primitive[field_name]
        else:
            # Will work only in Python 3.__
            the_type_needed = getattr(builtins, type_name)
            return the_type_needed()  # returns the default value for the type

    @staticmethod
    def serialize(item: "FilmRecord"):
        res = ""
        for k, v in item.__dict__.items():
            res += "|" + k + " - " + str(v)
        return bytes("<" + res + "|>", "utf-8")

    @staticmethod
    def deserialize(value: Union[str, bytes]):
        if (type(value) == bytes):
            value = value.decode("uts-8")

        value = value[2:-2]
        tokens = value.split("|")
        data_obj = FilmRecord(None, None, None, None)
        for token in tokens:
            data_fields = token.split(" - ")
            if data_fields[0] in {"cast", "genres"}:
                data = data_fields[1][1:-1].split(",")

                if data[0] == "" and len(data) == 1:
                    del (data[0])

                data_fields[1] = data

            setattr(data_obj, data_fields[0], data_fields[1])
        return data_obj

    @classmethod
    def decode(cls, json_obj: object):

        # check the object is typed and
        return cls(
            FilmRecord.safe_parse(json_obj, "title", "str"),
            FilmRecord.safe_parse(json_obj, "year", "int"),
            FilmRecord.safe_parse(json_obj, "cast", "list"),
            FilmRecord.safe_parse(json_obj, "genres", "list")
        )
