import argparse
import json
import logging
import subprocess


logger = logging.getLogger(__name__)


_DESC = 'Ensure standby_replay assignments to ranks matches a specification'


def get_fs_map():
    # check if we can get a ceph fs dump
    fs_dump = subprocess.run(['ceph', 'fs' 'dump', '--format', 'json'],
                             capture_output=True)
    if fs_dump.returncode != 0:
        logger.error(f'ERROR "ceph fs dump" returned rc: {fs_dump.returncode}')
        logger.error(f'ERROR {fs_dump.stderr}')
        exit(1)

    return json.loads(fs_dump.stdout)


def get_fs(name, fsmap):
    for fs in fsmap['filesystems']:
        if fs['name'] == name:
            logger.info(f'Found requested file system {name}')
            return fs

    logger.error(f'ERROR could not find file system with name {name}')
    exit(1)


def get_current_standby_assignment(fs):
    return {mds['name']: mds['rank'] for _gid, mds in fs['info'].items() if
            mds['state'] == 'up:standby_replay'}


def get_cold_standby(fs):
    return fs['standbys'][0]['name']


def check(current, wanted, cold, last=[]):
    if current == wanted:
        return last
    restart_rank = ''
    if cold in wanted:
        # current cold is wanted as replay
        wanted_rank = wanted[cold]
        restart_rank = current[wanted_rank]
    else:
        # standby_replays are assigned ti the wrong ranks
        for mds, rank in wanted.items():
            if current[rank] == mds:
                # this one if where we want it
                logger.info(f'mds {mds} is assigned to the wanted rank {rank}')
                continue
            if mds in last and mds == last[0]:
                logger.info(f'mds {mds} was just restarted')

            logger.info(f'chose mds {mds} for restart')
            restart_rank = mds
            break

    # actually restart

    # restart mds
    return last.append(restart_rank)


class MDSStandbyFor(object):

    fs = ''
    args = argparse.Namespace()

    def __init__(self):
        parser = argparse.ArgumentParser(description=_DESC)

        parser.add_argument(
            'fs',
            help='file system name')
        parser.add_argument(
            'standby_assignment',
            type=json.loads,
            help='A simple json dictionary with <mds_name>: <rank> pairs')

        self.args = parser.parse_args()

        self.main()

    def main(self):
        last = []
        for _ in range(10):
            # stop after 10 attempts
            fsmap = get_fs_map()

            if not fsmap['standbys']:
                logger.error('ERROR no unassigned standbys found in fsmap')
                exit(1)

            if len(fsmap['standbys']) != 1:
                logger.error('ERROR no can only work with 1 unassigned standby, got {len(fsmap["standbys"])}')
                exit(1)

            fs = get_fs(self.args.fs, fsmap)

            current = get_current_standby_assignment(fs)
            cold = get_cold_standby(fs)
            new_last = check(current, self.args.standby_assignments, cold)
            if last == new_last:
                logger.info('done')
                exit(0)
            else:
                last = new_last
        exit(1)


if __name__ == "__main__":
    MDSStandbyFor()
