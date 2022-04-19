import singer


def format_datetime(dt):
    if dt is not None:
        return singer.utils.strftime(dt)
    return None


def get_permission_details(permission):
    return {
        'capabilities': permission.capabilities,
        'grantee_id': permission.grantee.id,
        'grantee_tag_name': permission.grantee.tag_name
    }


def get_user_details(user):
    return {
        'id': user.id,
        'auth_setting': user.auth_setting,
        'email': user.email,
        'name': user.name,
        'full_name': user.fullname,
        'role': user.site_role,
    }
