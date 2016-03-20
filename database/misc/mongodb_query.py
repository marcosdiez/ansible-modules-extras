#!/usr/bin/python

# (c) 2016, Marcos Diez <marcos AT unitron.com.br>
# https://github.com/marcosdiez/
# This file is part of Ansible
#
# Ansible is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Ansible is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Ansible.  If not, see <http://www.gnu.org/licenses/>.


DOCUMENTATION = '''
---
module: mongodb_query
short_description: Makes a query to the MongoDB database and returns the result.
description:

    - Makes a query to the MongoDB database and returns the result.
version_added: "2.2"
options:
    connection_string:
        description:
            - "The connection_string authenticate with."
            - "mongodb://localhost/"
            - "mongodb://server1,server2/"
            - "mongodb://login:password@server1:port1,server2:port2/table_with_credentials"
            - "More info here: https://docs.mongodb.org/manual/reference/connection-string/"
        required: false
        default: mongodb://localhost
    database:
        description:
            - "The database which the query will be made"
        required: true
    collection:
        description:
            - "The database which the query will be made"
        required: true
    query:
        description:
            - "The query that will be made. More info here: https://docs.mongodb.org/getting-started/python/query/"
        required: false
        default: {}
    projection:
        description:
            - "A filter to receive only some of the keys of the document. More info here: https://docs.mongodb.org/getting-started/python/query/"
        required: false
        default: null
    limit:
        description:
            - "Maximum number of documents that will be returned"
        required: false
        default: 0
    skip:
        description:
            - "Number of documents that will not be returned"
        required: false
        default: 0
    sort:
        description:
            - "a list of lists containing the document keys that you want to sort."
            - "Examples"
            - '[ [ "name", "ASCENDING" ] ]'
            - '[ [ "age", "DESCENDING" ], [ "name", "ASCENDING" ] ]'
        required: false
        default: null
notes:
    - Requires the pymongo Python package on the remote host, version 2.4.2+.
      This can be installed using pip or the OS package manager.
      http://api.mongodb.org/python/current/installation.html
    - More info on the syntax of the queires is available below.
      https://docs.mongodb.org/getting-started/python/query/
    - The return value is a json containing at least a key called 'result', which contains a list of results

requirements: [ "pymongo" ]
author: "Marcos Diez (@marcosdiez)"
'''


EXAMPLES = '''
# Get all info from the 'startup_log', which is in the 'local' database
- mongodb_query: db='local' collection="startup_log"

# Get all the documents from 'startup_log' where "hostname"="batman"
- mongodb_query:
    db: 'local'
    collection: "startup_log"
    query: { "hostname": "batman" }

# Connects to the replicaset bellow, and query the db 'accounts' and the collection 'software' where the os=linux.
# returns only the key `name`, skipping the first `4` elements, showing `3` results, sorting by name ASCENDING and
# by version descending
- mongodb_query:
    connection_string: "mongodb://the_login:the_password@server1.domain.com:27017,server2.domain.com:37017/admin"
    db: 'accounts'
    collection: "software"
    query: { "os" : "linux" }
    projection: { "name": True , '_id': False}
    limit: 3
    skip: 4
    sort: [ [ "name", "ASCENDING" ], [ "version",  "DESCENDING" ] ]

'''


import sys


try:
    from bson import json_util
    from pymongo  import ASCENDING, DESCENDING
    from pymongo.errors import ConnectionFailure
    from pymongo.errors import OperationFailure
    from pymongo import version as PyMongoVersion
    from pymongo import MongoClient
except ImportError:
    try:  # for older PyMongo 2.2
        from pymongo import Connection as MongoClient
    except ImportError:
        pymongo_found = False
    else:
        pymongo_found = True
else:
    pymongo_found = True


# =========================================
# MongoDB module specific support methods.
#


def _fix_sort_parameter(sort_parameter, module):
    if sort_parameter is None:
        return sort_parameter
    for item in sort_parameter:
        original_sort_order = item[1]
        sort_order = original_sort_order.upper()

        if sort_order == "ASCENDING":
            item[1] = ASCENDING
        elif sort_order == "DESCENDING":
            item[1] = DESCENDING
        else:
            module.fail_json(msg='Invalid Sort parameter [{}]. It must be either ASCENDING or DESCENDING'.format(original_sort_order))

    return sort_parameter

# =========================================
# Module execution.
#

def main():
    module = AnsibleModule(
        argument_spec = dict(
            connection_string=dict(default="mongodb://localhost/",  no_log=True),
            database=dict(required=True, aliases=['db']),
            collection=dict(required=True),
            query=dict(required=False, type='dict', default={}),
            projection=dict(required=False, type='dict', default=None),
            skip=dict(required=False, type='int', default="0"),
            limit=dict(required=False, type='int', default="0"),
            sort=dict(required=False, type='list', default=None)
        ),
        supports_check_mode = True
    )

    if not pymongo_found:
        module.fail_json(msg='the python pymongo module is required')

    connection_string = module.params['connection_string']
    database = module.params['database']
    collection =  module.params['collection']
    query = module.params['query']
    projection = module.params['projection']
    skip = module.params['skip']
    limit = module.params['limit']
    sort = _fix_sort_parameter(module.params['sort'], module)

    try:
        client = MongoClient(connection_string)
        result = client[database][collection].find(query,
                                                   projection=projection,
                                                   skip=skip,
                                                   limit=limit,
                                                   sort=sort)
        result_json = { "changed": False, "result": result }
        print(json_util.dumps(result_json))
        sys.exit(0)

    except ConnectionFailure, e:
        module.fail_json(msg='unable to connect to database: %s' % str(e))


# import module snippets
from ansible.module_utils.basic import *
main()
