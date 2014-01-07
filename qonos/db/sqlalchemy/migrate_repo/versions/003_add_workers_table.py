# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright 2013 Rackspace
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from sqlalchemy.schema import Column
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table

from qonos.db.sqlalchemy.migrate_repo.schema import create_tables
from qonos.db.sqlalchemy.migrate_repo.schema import DateTime
from qonos.db.sqlalchemy.migrate_repo.schema import drop_tables
from qonos.db.sqlalchemy.migrate_repo.schema import Integer
from qonos.db.sqlalchemy.migrate_repo.schema import String


def define_workers_table(meta):
    workers = Table('workers',
                    meta,
                    Column('id', String(36), primary_key=True, nullable=False),
                    Column('host', String(255), nullable=False),
                    Column('process_id', Integer(), nullable=True),
                    Column('created_at', DateTime(), nullable=False),
                    Column('updated_at', DateTime(), nullable=False),
                    mysql_engine='InnoDB',
                    mysql_charset='utf8',
                    extend_existing=True)
    return workers


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine
    tables = [define_workers_table(meta)]
    create_tables(tables)


def downgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine
    tables = [define_workers_table(meta)]
    drop_tables(tables)
