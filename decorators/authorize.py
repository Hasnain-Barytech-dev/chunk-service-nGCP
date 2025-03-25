import requests
from functools import wraps
import jwt
from flask import request, abort
from flask import current_app
from api.chunk import models

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if "Authorization" in request.headers:
            token = request.headers["Authorization"].split(" ")[1]
        if not token:
            return {
                "message": "Authentication Token is missing!",
                "data": None,
                "error": "Unauthorized"
            }, 401
        try:
            jwt_data=jwt.decode(token, current_app.config["SECRET_KEY"], algorithms=["HS256"])

            permissions = get_company_user_permissions(request.headers.get('X-Tenant-ID'), jwt_data['user'].get('uuid'))
            permissions_list = permissions.get('permissions') or []
            fileuploadedfromchat = request.headers.get('fileuploadedfromchat', 'false').lower() == 'true'
            if not fileuploadedfromchat:
                if 'CREATE_RESOURCES' not in permissions_list:
                    return {
                        "message": "UNAUTHORIZED",
                        "data": None,
                        "error_code": "UNAUTHORIZED",
                        "error": "UNAUTHORIZED"
                    }, 400
            if permissions is None:
                return {
                    "message": "Something went wrong",
                    "data": None,
                    "error": str(e)
                }, 500
            else:
                jwt_data['company_user'] = permissions

        except Exception as e:
            return {
                "message": "Something went wrong",
                "data": None,
                "error": str(e)
            }, 500

        return f(jwt_data, *args, **kwargs)

    return decorated

def get_company_user_permissions(company_id, user_id):
    try:
        url = f"{current_app.config['DJANGO_BASE_URL']}/api/v2/company/get_company_user_permissions/?company_id={company_id}&user_id={user_id}"
        res = requests.get(url)
        res = res.json()
        return res
    except Exception as ex:
        return None