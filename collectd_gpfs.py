#!/usr/bin/env python
# encoding: utf-8

import re
import subprocess
import string

__version__ = '0.1'
__author__ = 'Guan Ji Chen @ vaneCloud.com'


global VERBOSE_LOGGING
VERBOSE_LOGGING = True
CONFIGS = []
COLLECTD_GPFS = {}
MMFSPATH = '/usr/lpp/mmfs/bin'


class GPFS(object):

    """Docstring for GPFS. """

    def __init__(self):
        """TODO: to be defined. """
        self.metrics = {}
        self.colleced = False

    def _get_gpfs_cluster_status(self):
        """
        :returns: {cl.cluster.status: 0|1}

        """
        fd=self._run(cmd='/usr/lpp/mmfs/bin/mmfsadm dump tscomm')
        fdd=self._get_childs_data(fd)
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
        self.metrics['cl.' + cluster_name + '.status'] = cluster_status
        return

    def _get_gpfs_node_status(self):
        """
        :returns: [
                    {no.node1.status: 0|1},
                    {no.node2.status: 0|1},
                    {no.node3.status: 0|1}
                    ]
        """
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
                self.metrics['no.' + node_name + '.status'] = node_status
        else:
            pass
        return

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
            if self.colleced:
                collectd.info("et disk information failed")
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
            self.metrics['di.' + disk_name + '.status'] = disk_status
        return

    def _get_gpfs_filesystem_status(self):
        """TODO
        :returns: [
                    {fi.filesystem1.status: 0|1},
                    {fi.filesystem2.status: 0|1},
                    {fi.filesystem3.status: 0|1}
        ]

        """
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
            if self.colleced:
                collectd.info("et disk information failed")
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
            self.metrics['fs.' + fsd + '.status'] = filesystem_status
        return

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
            if self.colleced:
                collectd.info("et disk information failed")
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
            self.metrics['fs.' + fsd + '.usage'] = filesystem_usage
        return

    def dump_ds(self):
        """dump gpfs data
        :returns: TODO

        """
        self._get_gpfs_cluster_status()
        self._get_gpfs_node_status()
        self._get_gpfs_disk_status()
        self._get_gpfs_filesystem_status()
        self._get_gpfs_filesystem_usage()
        if not self.colleced:
            print "data logged from console"
        else:
            self.log_verbose('data logged from plugin')
        return

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

    def read_callback(self):
        self.log_verbose('Read callback called')
        self.dump_ds()
        for key in self.metrics:
            metric = collectd.Values()
            #  metric.interval = 30
            metric.type = 'gauge'
            metric.type_instance = key
            metric.values = [self.metrics[key]]
            #  self.log_verbose('Sending value: %s - %s' % metric.type_instance, self.metrics[key])
            metric.dispatch()

    def log_verbose(self, msg):
        if not VERBOSE_LOGGING:
            return
        collectd.info('gpfs plugin [verbose]: %s' % msg)

    #  def _dispatch_value(self, plugin_instance, val_type, type_instance, value):
            #  """
            #  Dispatch a value to collectd
            #  """
            #  self.log_verbose(
                #  'Sending value: %s.%s=%s' % (
                    #  plugin_instance,
                    #  '-'.join([val_type, type_instance]),
                    #  value))

            #  val = collectd.Values()
            #  val.plugin = self.plugin_name
            #  val.plugin_instance = plugin_instance
            #  val.type = val_type
            #  if len(type_instance):
                #  val.type_instance = type_instance
            #  val.values = [value, ]
            #  val.dispatch()

    def configure_callback(self, conf):
        for node in conf.children:
            val = str(node.values[0])
            if node.key == 'Verbose':
                if val in ['True', 'true']:
                    VERBOSE_LOGGING = True
                else:
                    VERBOSE_LOGGING = False

def fetch_info(conf):
    """TODO: get gpfs data

    :conf: TODO
    :returns: TODO

    """
    pass





def get_metrics(conf):
    """TODO: Docstring for get_metrics.
    :returns: TODO

    """
    pass


if __name__ == '__main__':
    import sys

    gpfs = GPFS()
    gpfs.dump_ds()
    print gpfs.metrics

    sys.exit(0)
else:
    import collectd
    # Register callbacks
    gpfs = GPFS()
    collectd.register_config(gpfs.configure_callback)
    collectd.register_read(gpfs.read_callback)
