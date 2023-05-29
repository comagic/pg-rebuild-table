acl_map = {
    'a': 'insert',
    'r': 'select',
    'w': 'update',
    'd': 'delete',
    'D': 'truncate',
    'x': 'references',
    't': 'trigger',
    'X': 'execute',
    'U': 'usage',
    'C': 'create',
    'T': 'temp',
    'c': 'connect'
}
acl_order = 'rawdDxtXUCTc'
grant_all_pattern = {
    'column': 'arwx',
    'database': 'CTc',
    'fdw': 'U',
    'foreign server': 'U',
    'function': 'X',
    'procedure': 'X',
    'language': 'U',
    'largeobject': 'rw',
    'namespace': 'UC',
    'table': 'arwdDxt',
    'sequence': 'Urw',
    'schema': 'UC',
    'tablespace': 'C',
    'type': 'U',
    'domain': 'U',
}
function_public_acl = ('=X/postgres', '=X/gpadmin')

grant_all_pattern_with_grant_option = {
    k: '*'.join(v) + '*'
    for k, v in grant_all_pattern.items()
}


def resolve_perm(obj_type, perm):
    gr_opt = ''
    if grant_all_pattern[obj_type] == perm:
        perm = 'all'
    elif grant_all_pattern_with_grant_option[obj_type] == perm:
        perm = 'all'
        gr_opt = ' with grant option'
    else:
        perm = ', '.join(
            acl_map[c]
            for c in sorted(perm, key=lambda x: acl_order.find(x))
        )
    return perm, gr_opt


def acl_to_grants(acl, obj_type, obj_name, subobj_name=''):
    if not acl:
        return ''

    res = []
    if obj_type in ['function', 'procedure']:
        for fpa in function_public_acl:
            if fpa in acl:
                acl.remove(fpa)
                break
        else:
            res.append(
                'revoke all on %(obj_type)s %(obj_name)s from public;'
                % locals()
            )
    for a in sorted('public' + i if i.startswith('=') else i for i in acl):
        role, perm = a.split('/')[0].split('=')  # format: role=perm/grantor
        if role in ['postgres', 'gpadmin']:
            continue

        if subobj_name:  # column
            subobj_name = '(%s) ' % subobj_name

        perm, gr_opt = resolve_perm(obj_type, perm)
        if obj_type == 'column':
            obj_type = 'table'
        res.append(
            'grant %(perm)s %(subobj_name)son %(obj_type)s '
            '%(obj_name)s to %(role)s;'
            % locals())
    return '\n'.join(res)
