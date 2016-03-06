#!/usr/bin/env python
# encoding: utf-8

import re
import subprocess
import string

__version__ = '0.1'
__author__ = 'Guan Ji Chen @ vaneCloud.com'


VERBOSE_LOGGING = True
CONFIGS = []
COLLECTD_GPFS = {}
MMFSPATH = '/usr/lpp/mmfs/bin'


class GPFS(object):

    """Docstring for GPFS. """

    def __init__(self):
        """TODO: to be defined. """
        self.cl = ''
        self.node = []
        self.disk = []
        self.filesystem = []

    def _get_gpfs_cluster_status(self):
        """
        :returns: {cl.cluster.status: 0|1}

        """
        fd=self._run(cmd='/usr/lpp/mmfs/bin/mmfsadm dump tscomm')
        fdd=self._get_childs_data(fd)
        fdo=[]
        if fdd:
            cluster_status=1
        else:
            cluster_status=0
        fd=self._run(cmd='/usr/lpp/mmfs/bin/mmlscluster')
        fdd=self._get_childs_data(fd)
        for line in fdd.splitlines():
            searchObj = re.search(r'GPFS\ cluster\ name:(.*)$', line)
            if searchObj:
                cluster_name = searchObj.groups()[0].lstrip(' ').split('.')[0]
        fdo.append({'cl.' + cluster_name + '.status': cluster_status})
        return fdo

    def _get_gpfs_node_status(self):
        """
        :returns: [
                    {no.node1.status: 0|1},
                    {no.node2.status: 0|1},
                    {no.node3.status: 0|1}
                    ]
        """
        fdo=[]
        fd=self._run(cmd='/usr/lpp/mmfs/bin/mmgetstate -aLY')
        fdd=self._get_childs_data(fd)
        if fdd:
            for line in fdd.splitlines()[1:]:
                """
                0:  mmgetstate
                1:  not used
                2:  HEADER
                3:  version
                4:  reserved
                5:  reserved
                6:  nodeName
                7:  nodeNumber
                8:  state
                9:  quorum
                10: nodesUp
                11: totalNodes
                12: remarks
                13: cnfsState
                14: NULL
                """
                node_name=line.split(':')[6]
                # Explain node state
                if line.split(':')[8] == 'down':
                    node_status=0
                elif line.split(':')[8] == 'active':
                    node_status=1
                elif line.split(':')[8] == 'arbitraiting':
                    node_status=2
                else:
                    node_status=3
                fdo.append({'no.' + node_name + '.status': node_status})
        else:
            fdo.append(0)
        return fdo

    def _get_gpfs_disk_status(self):
        """TODO
        :returns: [
                    {di.disk1.status: 0|1|2},
                    {di.disk1.status: 0|1|2},
                    {di.disk1.status: 0|1|2}
                    ]

        """
        """
        GET Filesystem FIRST, THEN GET DISK STATUS
        """
        fdo=[]
        fd=self._run(cmd='/usr/lpp/mmfs/bin/mmlsfs all -Y')
        fdd=self._get_childs_data(fd)
        fs=[]
        if fdd:
            for line in fdd.splitlines()[1:]:
                """
                0: mmlsfs
                1: not used
                2: HEADER
                3: version
                4: reserved
                5: reserved
                6: deviceName
                7: filedName
                8: data
                9: remarks
                10: not used
                """
                # Explain filesystem state
                if line.split(':')[7] == 'inodeSize':
                    fs.append(line.split(':')[6])
        else:
            print "Get disk information failed"
        for fsi in fs:
            fd=self._run(cmd='/usr/lpp/mmfs/bin/mmlsdisk ' + fsi + ' -Y')
            fdd=self._get_childs_data(fd)
            disk_status=0
            for line in fdd.splitlines()[1:]:
                """
                0: mmlsdisk
                1: not used
                2: HEADER
                3: version
                4: reserved
                5: reserved
                6: nsdName
                7: driverType
                8: sectorSize
                9: failureGroup
                10: metadata
                11: data
                12: status
                13: availability
                14: diskID
                15: storagePool
                16: remarks
                17: numQuorumDisks
                18: readQuorumValue
                19: writeQuorumValue
                20: diskSizeKB
                21: diskUID
                22: not used
                """
                disk_name=line.split(':')[6]
                # Explain disk state
                if line.split(':')[12]== 'down':
                    disk_status=0
                elif line.split(':')[12]== 'ready':
                    disk_status=1
                elif line.split(':')[12]== 'suspended':
                    disk_status=2
                else:
                    disk_status=3
            fdo.append({'di.' + disk_name + '.status': disk_status})
        return fdo

    def _get_gpfs_filesystem_status(self):
        """TODO
        :returns: [
                    {fi.filesystem1.status: 0|1},
                    {fi.filesystem2.status: 0|1},
                    {fi.filesystem3.status: 0|1}
        ]

        """
        fdo=[]
        fd=self._run(cmd='/usr/lpp/mmfs/bin/mmlsfs all -Y')
        fdd=self._get_childs_data(fd)
        fs=[]
        if fdd:
            for line in fdd.splitlines()[1:]:
                """
                0: mmlsfs
                1: not used
                2: HEADER
                3: version
                4: reserved
                5: reserved
                6: deviceName
                7: filedName
                8: data
                9: remarks
                10: not used
                """
                # Explain filesystem state
                if line.split(':')[7] == 'inodeSize':
                    fs.append(line.split(':')[6])
        else:
            print "Get Filesystem information failed"
        for fsd in fs:
            fd=self._run(cmd='/usr/lpp/mmfs/bin/mmlsmount ' + fsd + ' -Y')
            fdd=self._get_childs_data(fd)
            for line in fdd.splitlines()[1:]:
                """
                0: mmlsmount
                1: not used
                2: HEADER
                3: version
                4: reserved
                5: reserved
                6: localDevName
                7: readDevName
                8: owningCluster
                9: totalNodes
                10: nodeIP
                11: nodeName
                12: clusterName
                13: mountMode
                """
                if line.split(':')[9] == '0':
                    filesystem_status = 0
                else:
                    filesystem_status = 1
            fdo.append(({'fs.' + fsd + '.status': filesystem_status}))
        return fdo

    def _get_filesystems(self):
        """ TODO: merge get filesystem names functions from other functions"""
        pass

    def _get_gpfs_filesystem_usage(self):
        """TODO
        :returns: [
                    {fi.filesystem1.usage: 0-100},
                    {fi.filesystem2.usage: 0-100},
                    {fi.filesystem3.usage: 0-100}
                    ]

        """
        fdo=[]
        fd=self._run(cmd='/usr/lpp/mmfs/bin/mmlsfs all -Y')
        fdd=self._get_childs_data(fd)
        fs=[]
        if fdd:
            for line in fdd.splitlines()[1:]:
                """
                0: mmlsfs
                1: not used
                2: HEADER
                3: version
                4: reserved
                5: reserved
                6: deviceName
                7: filedName
                8: data
                9: remarks
                10: not used
                """
                # Explain filesystem state
                if line.split(':')[7] == 'inodeSize':
                    fs.append(line.split(':')[6])
        else:
            print "Get Filesystem information failed"
        for fsd in fs:
            fd=self._run(cmd='/usr/lpp/mmfs/bin/mmdf ' + fsd + ' -Y')
            fdd=self._get_childs_data(fd)
            for line in fdd.splitlines()[4:]:
                """
                0: mmdf
                1: fsTotal
                2: HEADER
                3: version
                4: reserved
                5: reserved
                6: fsSize
                7: freeBlocks
                8: freeBlocksPct
                9: freeFragments
                10: freeFragementsPct
                11: not used
                """
                if line.split(':')[1] == 'fsTotal':
                    filesystem_usage= 100 - string.atoi(line.split(':')[8])
            fdo.append(({'fs.' + fsd + '.usage': filesystem_usage}))
        return fdo

    def dump_ds(self):
        """dump gpfs data
        :returns: TODO

        """
        # gcs gpfs_cluster_status
        gpfs_cluster_status = self._get_gpfs_cluster_status()
        print "GPFS Cluster Status is %s" % gpfs_cluster_status
        # gns gpfs_node_status
        gpfs_node_status = self._get_gpfs_node_status()
        print "GPFS Node Status is %s" % gpfs_node_status
        # gpfs_disk_status
        gpfs_disk_status = self._get_gpfs_disk_status()
        print "GPFS Disk Status is %s" % gpfs_disk_status
        # gpfs_filesystem_status
        gpfs_filesystem_status = self._get_gpfs_filesystem_status()
        print "GPFS Filesystem Status is %s" % gpfs_filesystem_status
        # gpfs_filesystem_usage
        gpfs_filesystem_usage = self._get_gpfs_filesystem_usage()
        print "GPFS Filesystem Usage is %s" % gpfs_filesystem_usage


    def _get_childs_data(self, child):
        (stdout, stderr) = child.communicate()
        exp = child.poll()

        if exp:
            return 0
        else:
            return stdout


    def _run(self, cmd):
        """TODO: Docstring for _run.
        :returns: TODO

        """
        gpfs_cluster_status = self._call_cmd(cmd)
        return gpfs_cluster_status

    def _call_cmd(self, cmd):
        """
        :returns: TODO

        """
        #  collectd.info('%s', cmd)
        #  print ('%s' % cmd)
        return subprocess.Popen(cmd, bufsize=1, shell=True, stdout=subprocess.PIPE)


def fetch_info(conf):
    """TODO: get gpfs data

    :conf: TODO
    :returns: TODO

    """
    pass


def read_callback():
    for conf in CONFIGS:
        get_metrics(conf)


def configure_callback(conf):
    """Configure collectd callback
    :returns: TODO

    """
    host = ''
    port = ''
    auth = ''
    instance = ''

    for node in conf.children:
        key = node.key.lower()
        val = node.values[0]
        log_verbose('Analyzing config %s key (value: %s)' % (key, val))
        searchObj = re.search(r'redis_(.*)$', key, re.M | re.I)

        if key == 'host':
            host = val
        elif key == 'port':
            port = int(val)
        elif key == 'auth':
            auth = val
        elif key == 'verbose':
            global VERBOSE_LOGGING
            VERBOSE_LOGGING = bool(node.values[0]) or VERBOSE_LOGGING
        elif key == 'instance':
            instance = val
        elif searchObj:
            log_verbose('Matching expression found: key: %s - value: %s' % (searchObj.group(1), val))
            global REDIS_INFO
            COLLECTD_GPFS[searchObj.group(1)] = val
        else:
            collectd.warning('collectd_gpfs plugin: Unknown config key: %s.' % key)
            continue

    log_verbose('Configured with host=%s, port=%s, instance name=%s, using_auth=%s' % (host, port, instance, auth != None))

    CONFIGS.append({'host': host, 'port': port, 'auth': auth, 'instance': instance})


def get_metrics(conf):
    """TODO: Docstring for get_metrics.
    :returns: TODO

    """
    info = fetch_info(conf)

    if not info:
        collectd.error('gpfs plugin: GPFS ERROR!')
        return
    plugin_instance = conf['instance']
    if plugin_instance is None:
        plugin_instance = '{host}:{port}'.format(host=conf['host'], port=conf['port'])
    # TODO: dispatch gpfs values
    for key, val in COLLECTD_GPFS.iteritems:
        log_verbose('key: %s, val: %s', (key, val))
        # TODO: if else loop to dispatch values


def log_verbose(msg):
    if not VERBOSE_LOGGING:
        return
    collectd.info('gpfs plugin [verbose]: %s' % msg)


if __name__ == '__main__':
    import sys

    gpfs = GPFS()
    ds = gpfs.dump_ds()

    sys.exit(0)
else:
    import collectd

    # Register callbacks
    collectd.register_config(configure_callback)
    collectd.register_read(read_callback)
