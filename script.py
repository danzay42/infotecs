import datetime
import pytz
from fastapi import FastAPI
import uvicorn


class MemDataBase:
    hashed_db: dict
    hashed_db_names: dict
    fields = ("geonameid", "name", "asciiname", "alternatenames",
              "latitude", "longitude", "feature_class", "feature code", "country code", "cc2",
              "admin1 code", "admin2 code", "admin3 code", "admin4 code", "population",
              "elevation", "dem", "timezone", "modification date")

    def __init__(self, file: str) -> None:
        self.hashed_db = self.init_db(file)
        self.hashed_db_names = self.init_hased_names(self.hashed_db)
        # print(f"{len(self.hashed_db)=}, {len(self.hashed_db_names)=}")

    def init_db(self, file: str):
        db = {}
        for line in open(file).readlines():
            geo_item = dict(zip(self.fields, line.strip().split('\t')))
            if geo_item["feature_class"] == "P":  # filter city, town, villages, etc...
                db[int(geo_item["geonameid"])] = geo_item
        return db
    
    def init_hased_names(self, db: dict):
        hashed_names = {}
        for geo_item in sorted(db.values(), key=lambda gi: int(gi["population"])):
            for name in geo_item["alternatenames"].split(','):
                hashed_names.update({
                    name: [geo_item] + hashed_names.get(name, [])
                    })
        return hashed_names

    def get_by_id(self, id: int):
        return self.hashed_db.get(id)

    def get_list(self, skip: int, limit: int):
        return list(self.hashed_db.values())[skip:skip+limit]

    def get_by_name(self, name: str):
        return self.hashed_db_names.get(name, [None])[0]

    def get_name_help(self, name_part: str, limit: int):
        return [
            name 
            for name in self.hashed_db_names.keys()
            if name.startswith(name_part)
        ][:limit]
    

def timezone_diff(tz1, tz2):
    tz1 = pytz.timezone(tz1)
    tz2 = pytz.timezone(tz2)
    dt = datetime.datetime.now()
    
    delta_time = tz1.utcoffset(dt) - tz2.utcoffset(dt)
    delta_minutes = delta_time.total_seconds() / 60

    delta_str = '+' if delta_minutes >= 0 else '-'
    delta_str += f"{int(abs(delta_minutes / 60)):02}:"
    delta_str += f"{int(abs(delta_minutes % 60)):02}"

    return delta_minutes, delta_str


app = FastAPI(title="GeoNames API")
mem_db: MemDataBase

@app.on_event("startup")
def db_up():
    global mem_db
    mem_db = MemDataBase("RU.txt")

@app.get('/info')
async def info(id: int):
    return mem_db.get_by_id(id)

@app.get('/')
async def pagination(page: int = 0, limit: int = 10):
    return mem_db.get_list(page*limit, limit)

@app.get('/diff')
async def diff(name_1: str, name_2: str):
    gi_1 = mem_db.get_by_name(name_1)
    gi_2 = mem_db.get_by_name(name_2)
    if gi_1 is None or gi_2 is None:
        return None
    time_delta, time_delta_str = timezone_diff(gi_1["timezone"], gi_2["timezone"])
    return {
        "north": name_1 if float(gi_1["latitude"]) >= float(gi_2["latitude"]) else name_2,
        "is_same_time": time_delta == 0,
        "timezone_diff": time_delta_str,
        name_1: gi_1,
        name_2: gi_2,
    }

@app.get('/help')
async def help(name_part: str, limit: int = 10):
    return mem_db.get_name_help(name_part, limit)


if __name__ == "__main__":
    uvicorn.run("script:app", port=8000, host="127.0.0.1", reload=True)
