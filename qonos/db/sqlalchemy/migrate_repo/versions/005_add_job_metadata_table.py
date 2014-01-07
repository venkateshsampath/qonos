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
from sqlalchemy.schema import ForeignKey
from sqlalchemy.schema import MetaData
from sqlalchemy.schema import Table
from sqlalchemy.schema import UniqueConstraint

from qonos.db.sqlalchemy.migrate_repo.schema import create_tables
from qonos.db.sqlalchemy.migrate_repo.schema import DateTime
from qonos.db.sqlalchemy.migrate_repo.schema import drop_tables
from qonos.db.sqlalchemy.migrate_repo.schema import from_migration_import
from qonos.db.sqlalchemy.migrate_repo.schema import String
from qonos.db.sqlalchemy.migrate_repo.schema import Text


def define_job_metadata_table(meta):
    (define_jobs_table,) = from_migration_import(
        '004_add_jobs_table',
        ['define_jobs_table'])

    jobs = define_jobs_table(meta)

    # NOTE(dperaza) DB2: specify the UniqueConstraint option when creating
    # the table will cause an index being created to specify the index
    # name and skip the step of creating another index with the same columns.
    # The index name is needed so it can be dropped and re-created later on.

    constr_kwargs = {}
    if meta.bind.name == 'ibm_db_sa':
        constr_kwargs['name'] = 'ix_job_metadata_job_id_key'

    job_metadata = Table('job_metadata',
                         meta,
                         Column('id',
                                String(36),
                                primary_key=True,
                                nullable=False),
                         Column('job_id',
                                String(36),
                                ForeignKey(jobs.c.id),
                                nullable=False),
                         Column('key', String(255), nullable=False),
                         Column('value', Text(), nullable=False),
                         Column('created_at', DateTime(), nullable=False),
                         Column('updated_at', DateTime(), nullable=False),
                         UniqueConstraint('job_id', 'key',
                                          **constr_kwargs),
                         mysql_engine='InnoDB',
                         mysql_charset='utf8',
                         extend_existing=True)

    return job_metadata


def upgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine
    tables = [define_job_metadata_table(meta)]
    create_tables(tables)


def downgrade(migrate_engine):
    meta = MetaData()
    meta.bind = migrate_engine
    tables = [define_job_metadata_table(meta)]
    drop_tables(tables)
