from chronos.openstack.common import version as common_version

NEXT_VERSION = '1.1'
version_info = common_version.VersionInfo('chronos',
                                          pre_version=NEXT_VERSION)
