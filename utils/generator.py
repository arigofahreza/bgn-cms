from models import User, ReportUser


def filter_query_location(query,
                          kd_propinsi: str | None = None,
                          kd_kabupaten: str | None = None,
                          kd_kecamatan: str | None = None,
                          kd_kelurahan: str | None = None):
    if kd_propinsi and not kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        query = (query
        .join(User, User.phone == ReportUser.created_by_phone)
        .filter(
            User.location_id.like(f'{kd_propinsi}%')
        ))
        return query
    elif kd_propinsi and kd_kabupaten and not kd_kecamatan and not kd_kelurahan:
        query = (query
        .join(User, User.phone == ReportUser.created_by_phone)
        .filter(
            User.location_id.like(f'{kd_propinsi}.{kd_kabupaten}%')
        ))
        return query
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and not kd_kelurahan:
        query = (query
        .join(User, User.phone == ReportUser.created_by_phone)
        .filter(
            User.location_id.like(f'{kd_propinsi}.{kd_kabupaten}.{kd_kecamatan}%')
        ))
        return query
    elif kd_propinsi and kd_kabupaten and kd_kecamatan and kd_kelurahan:
        query = (query
        .join(User, User.phone == ReportUser.created_by_phone)
        .filter(
            User.location_id == f'{kd_propinsi}.{kd_kabupaten}.{kd_kecamatan}.{kd_kelurahan}'
        ))
        return query
    else:
        return query

