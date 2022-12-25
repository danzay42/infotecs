import datetime
import dataclasses
import pytz
from fastapi import FastAPI, HTTPException, status
import uvicorn


@dataclasses.dataclass
class GeoInfo:
    geonameid: int
    name: str
    asciiname: str
    alternatenames: str
    latitude: float
    longitude: float
    feature_class: str
    feature_code: str
    country_code: str
    cc2: str
    admin1_code: str
    admin2_code: str
    admin3_code: str
    admin4_code: str
    population: int
    elevation: str
    dem: str
    timezone: str
    modification_date: datetime.date


@dataclasses.dataclass
class GeoInfoCompare:
        north: str
        is_same_time: bool
        timezone_diff: str
        name_1: GeoInfo
        name_2: GeoInfo


class MemDataBase:
    hashed_db: dict[int, GeoInfo]
    hashed_db_names: dict[str, list[GeoInfo]]

    def __init__(self, file: str) -> None:
        self.hashed_db = self.init_db(file)
        self.hashed_db_names = self.init_hased_names(self.hashed_db)

    @staticmethod
    def init_db(file: str) -> dict[int, GeoInfo]:
        db = {}
        for line in open(file).readlines():
            geo_item = GeoInfo(*line.strip().split('\t'))
            if geo_item.feature_class == "P":  # filter city, town, villages, etc...
                db[int(geo_item.geonameid)] = geo_item
        return db
    
    @staticmethod
    def init_hased_names(db: dict[int, GeoInfo]) -> dict[str, list[GeoInfo]]:
        hashed_names = {}
        for geo_item in sorted(db.values(), key=lambda gi: int(gi.population)):
            for name in geo_item.alternatenames.split(','):
                hashed_names.update({
                    name: [geo_item] + hashed_names.get(name, [])
                    })
        return hashed_names

    def get_by_id(self, id: int) -> GeoInfo | None:
        return self.hashed_db.get(id)

    def get_list(self, skip: int, limit: int) -> list[GeoInfo]:
        return list(self.hashed_db.values())[skip:skip+limit]

    def get_by_name(self, name: str) -> list[GeoInfo] | None: 
        return self.hashed_db_names.get(name)

    def get_name_help(self, name_part: str, limit: int) -> list[str]:
        possible_names = [
            name
            for name in self.hashed_db_names.keys()
            if name.startswith(name_part)
        ][:limit] 
        return possible_names


class Service:

    @staticmethod
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

    @staticmethod
    def get_diff_info(name_1: str, name_2: str, db: MemDataBase) -> GeoInfoCompare | None:
        gi_1, gi_2 = db.get_by_name(name_1), db.get_by_name(name_2)
        if gi_1 is None or gi_2 is None:
            return None
        gi_1, gi_2 = gi_1[0], gi_2[0]

        time_delta, time_delta_str = Service.timezone_diff(gi_1.timezone, gi_2.timezone)
        north = name_1 if gi_1.latitude >= gi_2.latitude else name_2
        is_same_timezone = time_delta == 0

        return GeoInfoCompare(north=north, is_same_time=is_same_timezone, timezone_diff=time_delta_str, name_1=gi_1, name_2=gi_2)


app = FastAPI(title="GeoNames API")
mem_db: MemDataBase


@app.on_event("startup")
def db_up():
    global mem_db
    mem_db = MemDataBase("RU.txt")

@app.get('/info', response_model=GeoInfo)
async def info(id: int):
    if id < 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="id must be >= 0")
    res = mem_db.get_by_id(id)
    if res is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="id not found")
    return res 

@app.get('/', response_model=list[GeoInfo])
async def pagination(page: int = 0, limit: int = 10):
    if page < 0 or 0 > limit > 1000:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="page must be >= 0 and 0 < limit <= 1000")
    return mem_db.get_list(page*limit, limit)

@app.get('/diff', response_model=GeoInfoCompare)
async def diff(name_1: str, name_2: str):
    res = Service.get_diff_info(name_1, name_2, mem_db)
    if res is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="one of names not found")
    return res 

@app.get('/help', response_model=list[str])
async def help(name_part: str, limit: int = 10) -> list[str]:
    if name_part == '' or 0 > limit > 1000:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="name must contain symbols and 0 < limit <= 1000")
    return mem_db.get_name_help(name_part, limit)


if __name__ == "__main__":
    uvicorn.run("script:app", port=8000, host="127.0.0.1", reload=True)
