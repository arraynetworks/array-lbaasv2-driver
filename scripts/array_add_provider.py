#!/usr/bin/python
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
#
# Refer to the README and COPYING files for full details of the license
#
import os
import optparse
import sys
import logging
import traceback
import subprocess
import shutil
import time

from oslo_config.cfg import ConfigParser

LOG_FILE = "/var/log/array_add_provider.log"
NEUTRON_LBAASCONFPATH = '/etc/neutron/neutron_lbaas.conf'
NEUTRON_LBAASCONF_BAK_PATH =\
    NEUTRON_LBAASCONFPATH + str(time.time()) + '_bak'

ARRAYMODULETEMP= '''\
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


from array_lbaasv2_driver_{0}.v2.driver_v2 import ArrayLBaaSV2Driver

class {0}(ArrayLBaaSV2Driver):
    """Plugin Driver for xx environment."""

    def __init__(self, plugin):
        super({0}, self).__init__(plugin, self.__class__.__name__)

'''

def init_log(log_file):
    """
        This function init the logging module.
    """
    FORMAT = '%(asctime)-15s %(message)s'
    try:
        #logging.basicConfig(format=FORMAT, filename=log_file, level=logging.DEBUG)
        logging.basicConfig(format=FORMAT, filename="/dev/stdout", level=logging.DEBUG)
    except:
        logging.basicConfig(format=FORMAT, filename="/dev/stdout", filemode='w+', level=logging.DEBUG)
    logging.getLogger('arraymultiprovider')


def log_exec(argv):
    """
        This function executes a given shell command while logging it.
    """
    out = None
    err = None
    rc = None
    try:
        logging.debug(argv)
        p = subprocess.Popen(argv , stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #p = subprocess.Popen(argv)
        out, err = p.communicate()
        rc = p.returncode
        logging.debug(out)
        logging.debug(err)
    except:
        logging.error(traceback.format_exc())
    return (out, err, rc)


class Usage(Exception):
    def __init__(self, msg = None, no_error = False):
        Exception.__init__(self, msg, no_error)


class ArrayMultiProvider(object):
    """
    It will build all the rpms which are modified by us, and
    then build update pack package
    """
    def __init__(self, environment):
        self.envir = environment
        self.new_provider_path = "/usr/lib/python2.7/site-packages/array_lbaasv2_driver_%s/" % self.envir

    def _check_env(self):
        if os.path.exists(self.new_provider_path):
            return False
        return True

    def _generate_provider(self):
        generateprovidercmd = ['/usr/bin/generate_provider.sh', self.envir]
        out, err, rc = log_exec(generateprovidercmd)
        if rc != 0:
            logging.debug("Failed to generate the provider directory")
            return False
        return True

    def _generate_driver(self):
        modname="array_" + self.envir
        modfilename = modname + ".py"
        driver_path = self.new_provider_path + "v2/" + modfilename
        with open(driver_path, "w") as mf:
            mf.write(ARRAYMODULETEMP.format(self.envir))
        return True

    def _write_conf_file(self):
        # backup the neutorn_lbaas.conf
        try:
            os.remove(NEUTRON_LBAASCONF_BAK_PATH)
        except OSError as exc:
            if not exc.args[1] == 'No such file or directory':
                raise
        logging.debug('NEUTRON_LBAASCONFPATH: {0}'.format(NEUTRON_LBAASCONFPATH))
        logging.debug(
            'NEUTRON_LBAASCONF_BAK_PATH: {0}'.format(NEUTRON_LBAASCONF_BAK_PATH))
        shutil.copy(NEUTRON_LBAASCONFPATH, NEUTRON_LBAASCONF_BAK_PATH)

        # append the new provider into neutorn_lbaas.conf
        confline = "LOADBALANCERV2:" + self.envir +\
                   ":array_lbaasv2_driver_" + self.envir + ".v2.array_"+\
                   self.envir + "." + self.envir
        conf = ConfigParser(NEUTRON_LBAASCONFPATH, {})
        conf.parse()
        conf.sections['service_providers']['service_provider']\
            .append(confline)

        with open(NEUTRON_LBAASCONFPATH, 'w') as cfh:
            for section, options in conf.sections.items():
                cfh.write('['+section+']\n')
                for opt, values in options.items():
                    for value in values:
                        cfh.write(" ".join([opt, "=", value]) + '\n')

    def _delete_provider(self):
        # backup the neutorn_lbaas.conf
        try:
            os.remove(NEUTRON_LBAASCONF_BAK_PATH)
        except OSError as exc:
            if not exc.args[1] == 'No such file or directory':
                raise
        logging.debug('NEUTRON_LBAASCONFPATH: {0}'.format(NEUTRON_LBAASCONFPATH))
        logging.debug(
            'NEUTRON_LBAASCONF_BAK_PATH: {0}'.format(NEUTRON_LBAASCONF_BAK_PATH))
        shutil.copy(NEUTRON_LBAASCONFPATH, NEUTRON_LBAASCONF_BAK_PATH)

        # append the new provider into neutorn_lbaas.conf
        confline = "service_provider = LOADBALANCERV2:" + self.envir +\
                   ":array_lbaasv2_driver_" + self.envir + ".v2.array_"+\
                   self.envir + "." + self.envir
        os.system('sed -i "/%s/d" %s' % (confline, NEUTRON_LBAASCONFPATH))
        try:
            shutil.rmtree(self.new_provider_path)
        except OSError as exc:
            if not exc.args[1] == 'No such file or directory':
                raise
        return True

    def _update_provider(self):

        try:
            shutil.rmtree(self.new_provider_path)
        except OSError as exc:
            if not exc.args[1] == 'No such file or directory':
                raise
        logging.debug("Generate new provider module for update...")
        if not self._generate_provider():
            logging.debug("Failed to generate new provider module...")
            return False

        logging.debug("Generate new provider driver for update...")
        if not self._generate_driver():
            logging.debug("Generate new provider driver...")
            return False
        return True

    def delete(self):
        logging.debug("check the environment for delete...")
        if self._check_env():
            logging.debug("The enviroment has not existed")
            return False
        logging.debug("delete the environment...")
        if not self._delete_provider():
            logging.debug("failed to delete %s from neutorn_lbaas.conf", self.envir)
            return False
        return True

    def update(self):
        logging.debug("check the environment for update...")
        if self._check_env():
            logging.debug("The enviroment has not existed")
            return False
        logging.debug("update the environment...")
        if not self._update_provider():
            logging.debug("failed to update for %s", self.envir)
            return False
        return True

    def add(self):
        logging.debug("Check the environment...")
        if not self._check_env():
            logging.debug("The environment has existed.")
            return False

        logging.debug("Generate new provider module...")
        if not self._generate_provider():
            logging.debug("Failed to generate new provider module...")
            return False

        logging.debug("Generate new provider driver...")
        if not self._generate_driver():
            logging.debug("Generate new provider driver...")
            return False

        logging.debug("Write the neutorn_lbaas.conf file")
        if not self._write_conf_file():
            logging.debug("Write the neutorn_lbaas.conf file")
            return False
        return True

def parse_options():
    parser = optparse.OptionParser();

    parser.add_option("-o", "--operation", dest="opt",
        help="Operation: add, delete, update", default='add')
    parser.add_option("-e", "--environment", dest="envir",
        help="Environment parameter")

    (options, args) = parser.parse_args()

    return options

def main():
    init_log(LOG_FILE)
    rc = None
    rtn = None
    try:
        options = parse_options()
    except Usage, (msg, no_error):
        if no_error:
            out = sys.stdout
            ret = 0
        else:
            out = sys.stderr
            ret = 2
        if msg:
            print >> out, msg
        return ret
    logging.info('options : %s', options)

    if not options.envir:
        print("Please input the environment!!!")
        logging.error("Please input the environment!!!")
        return 0
    rc = ArrayMultiProvider(options.envir)
    if not options.opt or options.opt == 'add':
        rtn =  rc.add()
    elif options.opt == 'delete':
        rtn = rc.delete()
    elif options.opt == 'update':
        rtn = rc.update()

    if rtn:
        return 0
    else:
        return 1

if __name__ == '__main__':
    sys.exit(main())
