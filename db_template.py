def db_template(id: str, status: str, symbol_type: str, type: str, date: str, info: str, lon: float, lat: float, city: str, district: str, street: str, cnt_fire_dep: int):
    return {"id":id,
            "status":status,
            "symbol_type":symbol_type,
            "type":type,
            "date":date,
            "info":info,
            "lon":lon,
            "lat":lat,
            "city":city,
            "district":district,
            "street":street,
            "cnt_fire_dep": cnt_fire_dep
    }
